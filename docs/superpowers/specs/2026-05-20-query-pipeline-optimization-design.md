# 查询算子 Pipeline 性能优化设计文档

> **状态**: 已确认
> **日期**: 2026-05-20
> **关联**: `docs/superpowers/specs/2026-05-20-query-load-optimization-design.md`（前置优化）

## 目标

消除 Execute (QueryPlan) 路径相对于 Iterator 路径的性能差距，在 Filter 和 GroupBy 场景追平甚至反超。

## 基准数据

1M 数据点（3 host × 3 region，字段 cpu/mem/counter），compaction 后 2 个 SSTable：

| 场景 | Iterator (旧) | Execute (新) | 差距 |
|------|--------------|-------------|------|
| Scan+Project (100 行) | 6.10ms | 3.45ms | **1.77x ✅** |
| Filter cpu>50 (590K 行) | 477ms | 563ms | **0.85x ❌** |
| GroupBy+Agg (3 组) | 495ms | 868ms | **0.57x ❌** |
| Sort+Limit Top-100 | — | 672ms | 新路径独有 |

## 根因分析

### FilterOperator（-18%, +86ms）

每行额外开销链：

```
RowIterator.Next() → FilterOperator.Next() → ScanOperator.Next() → Iterator.Next()
```

- 2 次 interface 分发 + 2 次 nil 检查 + 1 次 `context.Background()` 分配/行
- `matchField()` 对 row.Fields 做线性扫描（O(n) per field per row）
- 1M 行 × 86ns 额外开销 ≈ 86ms

### GroupAggregateOperator（-75%, +373ms）

`loadAndAggregate()` 三阶段全量物化：

1. **全量加载**: `map[string][]*types.PointRow` — 1M 行指针全部物化
2. **冗余排序**: `aggregateGroup()` 对每组 `sort.Slice` — 数据已按时间有序
3. **重复扫描**: 每个聚合函数调用 `getFieldFloat()` 线性扫描 Fields
4. **字符串分配**: `groupKey()` 每行 `strings.Join` 分配新 string

对比 Iterator 手动版本：单次流式扫描，读→更新累加器→丢弃，从不物化。

### Sort+Limit Top-K（672ms）

- 堆选 Top-K 已生效 (O(N log K))
- 但 Sort 在 Project **之前**执行，排序完整行而非裁剪后行
- `compareFieldValue` 无快速路径，每次比较都走完整 type switch

### 内存分配

- GroupBy 全量物化是主因：`map[string][]*types.PointRow` + groupKey 字符串分配
- GC 后 AllocDelta=0 是因为回收及时，但峰值分配巨大

---

## 设计方案

### §1 GroupBy 流式聚合（P0）

**思路**: 将 `loadAndAggregate` 从"全量加载→分组→排序→聚合"改为"流式读取→实时更新累加器"。

**新增 `aggAccumulator` 接口**:

```go
type aggAccumulator interface {
    update(row *types.PointRow)
    result(field string) *types.FieldEntry
}
```

**累加器实现**:

| 聚合函数 | 状态 | update 操作 |
|---------|------|------------|
| avg | sum, count | sum += v; count++ |
| max | max | if v > max { max = v } |
| min | min | if v < min { min = v } |
| sum | sum | sum += v |
| count | count | count++ |
| first | firstRow | 仅首次设置 |
| last | lastRow | 每次覆盖 |
| diff | firstVal, lastVal | 记录首尾 |
| rate/irate/derivative | firstVal, lastVal, tsFirst, tsLast | 记录首尾 + 时间 |

**流程**:

```
Open():
  1. upstream.Open()
  2. 建立流水线: accumulators map[string][]aggAccumulator
  3. 逐行流式读取:
     for row := upstream.Next() {
         key := groupKey(row)
         accs := loadOrInit(key)
         每个 acc.update(row)
     }
  4. 产出结果行 → 按 key 排序（仅分组数级别排序）
```

**效果**:
- 内存: O(N) → O(G × A)，G=分组数，A=聚合函数数
- 消除组内排序: 3 × 333K 的 sort.Slice → 0
- 单次扫描: 1M 行 × 1 次遍历（vs 原来的 1 次加载 + N 次聚合函数遍历）

### §2 Filter+Scan 算子融合（P1）

**思路**: 新建 `FilteredScanOperator`，将字段过滤条件直接传入 Iterator 层，消除中间 interface 分发。

**FilteredScanOperator**:

```go
type FilteredScanOperator struct {
    iter       *Iterator
    conditions []*types.FilterCondition
    fieldIndex map[string]int  // field名→Fields切片索引
}

func (f *FilteredScanOperator) Next() (*types.PointRow, error) {
    for f.iter.Next(context.Background()) {
        row := f.iter.Points()
        if f.matchAll(row) {
            return row, nil
        }
    }
    return nil, nil
}
```

**matchField O(1) 优化**: 构造时预计算 `field→index` 映射，替代每行的线性 Fields 扫描。

**BuildPipeline 变更**: 当 Pipeline 以 `Scan → Filter` 开头时，直接用 `FilteredScanOperator` 替代两层包装。后续算子不变。

**效果**:
- 消除 2 次 interface 分发
- `matchField` 从 O(n) → O(1)
- 纯 Scan 路径不受影响

### §3 Sort 比较开销优化（P2）

#### 3A: Project 前提重排序

当 Sort 的排序字段 ⊆ Project 字段时，将 Project 移到 Sort 之前：

```
原:  Scan → Sort → Project → Limit
新:  Scan → Project → Sort → Limit   (条件: SortFields ⊆ ProjectFields)
```

Sort 处理裁剪后的轻量行，减少每次比较的 Fields 遍历开销。

#### 3B: float64 快速比较路径

`compareFieldValue` 中最常见（>90%）的比较是 float64 vs float64，增加快速路径跳过完整 type switch：

```go
func compareFieldValue(a, b *types.FieldValue) int {
    af, aIsFloat := a.Value.(*types.FieldValue_FloatValue)
    bf, bIsFloat := b.Value.(*types.FieldValue_FloatValue)
    if aIsFloat && bIsFloat {
        if af.FloatValue < bf.FloatValue { return -1 }
        if af.FloatValue > bf.FloatValue { return 1 }
        return 0
    }
    // 回退完整 type switch
    ...
}
```

### §4 groupKey 分配消除（P3）

**思路**: `sync.Map` 缓存 key → string，避免每行 `strings.Join` 分配。

```go
var groupKeyCache sync.Map

func groupKey(row *types.PointRow, tags []string) string {
    var buf strings.Builder
    for i, tag := range tags {
        if i > 0 { buf.WriteByte(0) }
        buf.WriteString(row.Tags[tag])
    }
    raw := buf.String()
    if cached, ok := groupKeyCache.Load(raw); ok {
        return cached.(string)
    }
    groupKeyCache.Store(raw, raw)
    return raw
}
```

配合 §1 流式聚合后，仅首次遇到的分组 key 会分配，后续全部命中缓存。

---

## 改动范围

| 文件 | 改动内容 | 预估行数 |
|------|---------|---------|
| `internal/query/operator.go` | 流式聚合累加器 + FilteredScanOperator + fieldIndex + float64 快速路径 + groupKey 缓存 | +250 / -120 |
| `internal/query/executor.go` | BuildPipeline 融合 Scan+Filter + Project 前提重排 | +30 |
| `internal/query/operator_test.go` | 累加器单元测试 + FilteredScanOperator 测试 | +150 |
| `internal/query/executor_test.go` | Pipeline 排列测试 | +50 |

**总计**: 4 个文件，~510 行新增/~120 行删除。不改动 Iterator/SSTable/Shard/Engine 层。

## 预期效果

| 场景 | 优化前 | 优化后（预估） | vs Iterator |
|------|--------|-------------|-------------|
| Filter cpu>50 (590K) | 563ms | ~450ms | **追平反超** (Iterator 477ms) |
| GroupBy+Agg (3 组) | 868ms | ~400ms | **反超** (Iterator 495ms) |
| Sort+Limit Top-100 | 672ms | ~580ms | 新路径独有 |
| Scan+Project (100) | 3.45ms | 不变 | 1.77x ✅ |

## 实施顺序

1. **§4 groupKey 缓存** — 独立、无依赖，先做
2. **§1 流式聚合** — 核心变更，依赖 §4 的缓存
3. **§2 Filter+Scan 融合** — 与 §1 并行无依赖
4. **§3 Sort 优化** — 依赖 §1/§2 稳定后做

## 测试策略

- 累加器单元测试：每个聚合函数的 update/result 正确性
- FilteredScanOperator 测试：单条件、多条件 AND、无匹配
- Pipeline 排列测试：Project 前提重排序正确性验证
- 回归：`operator_test.go` 和 `executor_test.go` 全部现有测试
- E2E：`query_builder_test` 16 个测试用例 + `query_op_benchmark` 对比
