# 算子 Pipeline 性能优化设计

> 目标：消除 1M 数据点基准测试中发现的 3 个性能劣化，使 Execute 路径全面达到 Iterator 路径性能水平。

## 背景

1M 数据点基准测试 (`tests/e2e/query_op_benchmark/main.go`) 发现 Execute 路径在以下场景劣化：

| 场景 | Iterator 路径 | Execute 路径 | 劣化 |
|------|-------------|-------------|------|
| Sort+Limit Top-100 | N/A | 1.79s / 958MB Sys | 全量排序 |
| Filter cpu>50 | 623ms | 692ms | +11% |
| GroupBy+Agg host | 641ms | 823ms | +28% |

## 约束

- 不修改 `Operator` 接口（Open/Next/Close）签名
- 不修改 proto 定义（`types/mts.pb.go`）
- 不新增文件，所有改动在 `operator.go` 和 `executor.go`
- 不改变对外查询语义（相同输入 → 相同输出）

---

## 优化 1：Sort+Limit 堆选 Top-K

### 根因

`SortOperator.Open()` 无条件收集全量 N 行并 `sort.Slice` 全排序。下游 `LimitOperator` 仅需 K 行，但 Sort 毫不知情。1M 行 × ~800B/PointRow ≈ 800MB 内存分配。

### 设计

**方案：LimitHint 下推**

`BuildPipeline` 在构建 Limit 算子时向前查找最近的 Sort 算子（穿透 Project），将 `offset+limit` 作为 Hint 注入 `SortOperator.limitHint`。

SortOperator 新增字段：
```go
type SortOperator struct {
    // ... 现有字段 ...
    limitHint int64  // >0 时启用堆选 Top-K，0 保持全排序
}
```

Open 时分支：
- `limitHint > 0`：调用 `selectTopK(limitHint)`，用 `container/heap` 维护 top-K
- `limitHint == 0`：保持现有全排序逻辑

`selectTopK()` 对每条上游数据：
- 堆未满（< K）：直接入堆
- 堆满：与堆顶比较，若优于堆顶则替换
- DESC 排序用 min-heap（堆顶是 top-K 中最小元素），ASC 排序用 max-heap

时间复杂度：O(N log K)，空间 O(K)。K=100 时约 80KB（100 × 800B）。

### 边界处理

- offset > 0：堆大小 = offset + limit，Next() 时跳过前 offset 个
- 无 downstream Limit：limitHint = 0，保持全排序
- Sort→Project→Limit：BuildPipeline 穿透 Project 传递 Hint

---

## 优化 2：Filter 单次扫描 + FieldIndex

### 根因

`FilterOperator.matchRow()` 每个 condition 独立线性扫描 `row.Fields`。3 个 WHERE 条件 = 3 次 Fields 遍历。手动过滤直接 `f.Value.GetFloatValue() > 50` 一次调用。

### 设计

**方案：FieldIndex 预计算 + 单次扫描求值**

Open 时从上游取一行样本，为每个 condition 的 field 名预计算 `row.Fields` 中的索引位置。

```go
type filterCondition struct {
    // ... 现有字段 ...
    fieldIndex int  // Fields 索引，-1=回退线性扫描
    tagPresent bool  // tag 是否存在
}
```

matchRow 逻辑：
1. 所有 `fieldIndex >= 0` 的 condition 直接 O(1) 索引取值 → 比较
2. 第一次遇到 `fieldIndex == -1` 的 condition 时，做一次 Fields 扫描取出所有剩余条件的值
3. 比较失败的 condition 立即返回 false（短路求值）

### 边界处理

- 空条件列表：Filter 变为透传（`f.skip = true`），Next 直接返回上游数据
- field 不存在：fieldIndex 保持 -1，matchRow 中按 NE=true / 其他=false 处理
- 不同 SID 可能有不同 Fields 顺序：fieldIndex 失效时自动回退线性扫描

---

## 优化 3：GroupAggregate 三连优化

### 根因

三个微劣化叠加：
1. 每个 agg 函数独立扫描 Fields 找字段值（O(functions × fields)）
2. groupKey 用 `tag + "\x00" + tag` 每行分配字符串
3. `sort.Strings(groupKeys)` 无条件执行，即使只有 3 个分组

### 设计

**3a. 字段位置预计算**

Open 时取一行样本，为每个 aggFunc 建立 `fieldName → fieldIndex` 映射。Next 循环中直接索引取值：

```go
type aggFuncSpec struct {
    function    string
    field       string
    windowNanos int64
    fieldIndex  int  // 新增：Fields 中的索引（-1=未找到）
}
```

**3b. groupKey 零分配**

- 单 tag：直接 `row.Tags[groupByTags[0]]` 做 key
- 多 tag：预计算所需容量，用 `strings.Builder` 加 `Grow()` 预分配

**3c. 跳过分组键排序**

`groupKeys` 改为在第一次遇到新分组时直接 append 到切片（插入顺序），移除 `sort.Strings(groupKeys)` 调用。理由：

- SQL 语义中 GROUP BY 不保证输出顺序（有序需求由 ORDER BY 覆盖）
- 插入顺序在同一查询内稳定，测试结果可复现
- 跨查询顺序可能不同（Go map 迭代随机化），但语义正确

已有 Sort 算子在 pipeline 下游时，会按用户指定的字段排序，无需 groupKey 排序。

---

## 验证策略

### 单元测试

- **SortOperator**：limitHint=0（全排序）/ limitHint=K（堆选）/ limitHint+offset / 单行 / 空数据
- **FilterOperator**：fieldIndex 命中 / fieldIndex 回退 / 混合 / 空条件透传
- **GroupAggregateOperator**：fieldIndex 命中 / 单 tag key / 多 tag key / 无排序模式
- 所有现有单测保持通过（行为不变）

### 性能验证

- 运行 `tests/e2e/query_op_benchmark` 对比优化前后 TPS/延迟/内存
- 目标：
  - Sort+Limit Top-100：从 1.79s 降到 < 50ms
  - Filter：劣化从 +11% 降到 ±5% 以内
  - GroupBy+Agg：劣化从 +28% 降到 ±5% 以内

### E2E 回归

- `tests/e2e/query_builder_test` 16 个场景全部通过
- `tests/e2e/query_1m` 等现有性能测试无退化

---

## 风险与缓解

| 风险 | 缓解 |
|------|------|
| fieldIndex 跨行失效（不同 SID Fields 顺序不同）| fieldIndex=-1 自动回退线性扫描 |
| heap 选出的 top-K 在 offset>0 时语义错误 | 堆大小 = offset+limit，Next 跳过前 offset |
| ASC/DESC 堆类型选反 | 单测显式验证"Top-3 ASC = [5,10,15]" |
| 跳过 groupKeys 排序导致结果不稳定 | 无 Sort 算子时不排序（确定性非必需），有 Sort 算子时排序 |
