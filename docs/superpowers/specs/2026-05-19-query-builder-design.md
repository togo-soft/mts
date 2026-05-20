# 查询 Builder 算子 Pipeline 设计文档

- 文档版本：v1.0
- 更新日期：2026-05-19
- 状态：设计完成，待实现
- 方案：裁剪版 Plan B（QueryPlan + Operator DAG）

---

## 1. 目标

在现有 `query.Iterator`（min-heap 归并）基础上，新增一套基于算子 Pipeline 的查询执行路径。用户通过 Builder 模式构建 `QueryPlan`，Engine 解析后组装算子链并执行。

**硬约束**：现有 `Engine.Iterator()` 保持不变，新路径完全独立。

---

## 2. 架构概览

```
QueryPlan (proto)  →  Engine.Execute(ctx, plan)  →  Operator Pipeline

QueryRangeRequest  →  Engine.Iterator(ctx, req)   →  现有 min-heap Iterator（不变）
```

两条路径独立，互不干扰。

### 2.1 算子 Pipeline 数据流

```
Scan → Filter → [GroupBy → Aggregate] → Sort → Project → Limit
```

每个算子实现统一接口，链式调用：

```go
type Operator interface {
    Open(ctx context.Context) error
    Next() (*types.PointRow, error)
    Close() error
}
```

下游的 `Next()` 调用上游的 `Next()`，逐行拉取（Pull 模型）。

---

## 3. 算子定义（7 种）

| 算子 | 职责 | 关键字段 |
|------|------|---------|
| **Scan** | 读取数据源（Shard/MemTable/unordered/downsample），内部复用 `query.Iterator` 的 min-heap 归并 | database, measurement, startTime, endTime, downsampleWindowNanos |
| **Filter** | 行级过滤：`Tags["host"] == "web-01" AND Fields["cpu"] > 80`。多个条件 AND 叠加，不支持 OR/NOT/嵌套 | filters []FilterExpr |
| **GroupBy** | 按 tag 值分组（`GROUP BY host`），输出各分组的 bucket key | groupByTags []string |
| **Aggregate** | 窗口内聚合：`avg(cpu)`, `sum(mem)`。复用 downsample 的 accumulator 实现 | aggregations []AggExpr |
| **Sort** | 单字段/时间戳排序（`ORDER BY timestamp DESC`）。全量排序，非流式 | sortBy []SortExpr |
| **Project** | 字段投影（`SELECT cpu, mem`），选择输出字段 | fields []string |
| **Limit** | Offset + Limit 截断，跳过前 N 行后限制输出行数 | offset, limit int64 |

### 3.1 FilterExpr（裁剪版）

```go
type FilterExpr struct {
    Tag   string           // tag 名，空表示字段过滤
    Field string           // 字段名，空表示 tag 过滤
    Op    FilterOp         // EQ, NE, GT, GTE, LT, LTE
    Value FieldValue       // 比较值
}
```

### 3.2 AggExpr

```go
type AggExpr struct {
    Function    string  // avg, max, min, sum, count, first, last, diff, rate, irate, derivative
    Field       string  // 目标字段名
    WindowNanos int64   // 可选窗口大小，0 表示使用 GroupBy 的 key
}
```

### 3.3 SortExpr

```go
type SortExpr struct {
    Field     string        // 字段名，"timestamp" 表示按时间排序
    Direction SortDirection // ASC 或 DESC
}
```

---

## 4. Builder API

```go
plan := query.NewBuilder().
    Select("avg(cpu)", "max(cpu)", "host").
    From("monitoring", "server_metrics").
    Where("host", query.EQ, "web-01").
    Where("region", query.EQ, "us-east").
    GroupBy("host").
    OrderBy("avg_cpu", query.DESC).
    Offset(0).
    Limit(100).
    Build()
```

### 4.1 Builder 规则

- `Select()` 中的聚合函数通过前缀识别：`avg(cpu)` → AggExpr{Function: "avg", Field: "cpu"}
- `Select()` 中的普通字段：出现在 GroupBy 中则用作分组 key，否则仅投影
- `From(db, meas)` 设置 database + measurement，必填
- `Where()` 多次调用自动 AND 叠加
- `SortBy()` / `GroupBy()` 支持多值
- `Build()` 序列化为 proto `QueryPlan`

### 4.2 Engine 执行入口

```go
func (e *Engine) Execute(ctx context.Context, plan *types.QueryPlan) (*query.RowIterator, error)
```

---

## 5. Proto Schema（新增，不修改现有 message）

```protobuf
message QueryPlan {
  string database = 1;
  string measurement = 2;
  int64 start_time = 3;
  int64 end_time = 4;
  repeated OperatorSpec ops = 5;
}

message OperatorSpec {
  oneof op {
    ScanSpec scan = 1;
    FilterSpec filter = 2;
    GroupBySpec group_by = 3;
    AggregateSpec aggregate = 4;
    SortSpec sort = 5;
    ProjectSpec project = 6;
    LimitSpec limit = 7;
  }
}

message ScanSpec           { int64 downsample_window_nanos = 1; }
message FilterSpec         { repeated FilterCondition conditions = 1; }
message FilterCondition    { string tag = 1; string field = 2; FilterOp op = 3; FieldValue value = 4; }
message GroupBySpec        { repeated string tags = 1; }
message AggregateSpec      { repeated AggFunction functions = 1; }
message AggFunction        { string function = 1; string field = 2; int64 window_nanos = 3; }
message SortSpec           { repeated SortField fields = 1; }
message SortField          { string field = 1; SortDirection direction = 2; }
message ProjectSpec        { repeated string fields = 1; }
message LimitSpec          { int64 offset = 1; int64 limit = 2; }

enum FilterOp     { EQ = 0; NE = 1; GT = 2; GTE = 3; LT = 4; LTE = 5; }
enum SortDirection { ASC = 0; DESC = 1; }
```

---

## 6. 文件结构

```
新增:
  internal/query/operator.go      # Operator 接口 + 7 种算子实现
  internal/query/builder.go       # Builder API + Build() 序列化

修改:
  internal/query/executor.go      # 重写: 解析 QueryPlan → 组装 Pipeline → 执行
  internal/engine/engine_query.go # 新增 Execute(ctx, plan) 方法
  proto/mts.proto                 # 新增 QueryPlan 及相关 message
  types/mts.pb.go                 # protoc 重新生成

不变:
  internal/query/iterator.go      # 简单查询路径，原封不动
```

---

## 7. 实现约束

- Scan 算子内部初始化 `query.Iterator`（min-heap），复用现有 ShardIterator 归并逻辑
- Aggregate 算子复用 `downsample.accumulator`，不重复实现聚合函数
- Sort 算子：全量收集上游数据到内存，排序后逐行输出。大数据量场景在下个阶段优化（spill-to-disk）
- GroupBy：按 tag 值分组，输出 bucket key，每个 bucket 由下游 Aggregate 消费
- 代码硬性上限：函数 ≤ 50 行、文件 ≤ 300 行、嵌套 ≤ 3、位置参数 ≤ 3、圈复杂度 ≤ 10

---

## 8. 测试策略

- 每个算子独立单元测试，覆盖正常路径和边界条件
- Builder 序列化/反序列化往返测试
- Executor 集成测试：端到端执行 QueryPlan
- E2E 测试：`tests/e2e/` 下新增 `query_builder_test/`，覆盖 GroupBy + Aggregate + Sort 组合场景

---

## 9. 遗留任务（完整版缺失部分）

| 类别 | 缺失项 | 说明 |
|------|--------|------|
| **表达式系统** | 嵌套 WHERE（OR/NOT/括号） | 当前仅支持 AND 平铺，完整版需表达式树 + 短路求值 |
| **表达式系统** | 字段间比较（`WHERE cpu > mem`） | 当前仅支持 field/op/literal |
| **表达式系统** | 算术表达式（`SELECT cpu*100`） | SELECT 中的计算列 |
| **优化器** | 算子重排序 | 如 Filter 下推到 Scan 之前 |
| **优化器** | 索引选择 | 利用 tag 索引加速 Filter |
| **优化器** | 裁剪（Pruning） | 根据时间范围跳过无关 Shard |
| **执行模型** | 向量化执行 | Chunk 批量处理替代逐行 Next() |
| **执行模型** | 并行执行 | 多 Shard 并行 Scan + Merge |
| **执行模型** | Spill-to-disk | 大数据 Sort/GroupBy 溢写磁盘 |
| **序列化** | `HAVING` 子句 | 聚合后过滤 |
| **序列化** | 子查询/CTE | 嵌套 QueryPlan |
| **序列化** | ORDER BY 多字段 | 当前仅支持单字段排序 |
| **调试** | `EXPLAIN` | 查询计划可视化 |

---

## 10. 风险与权衡

| 风险 | 缓解措施 |
|------|---------|
| Sort 全量加载内存 | 裁剪版接受此限制，遗留任务标记 spill-to-disk |
| GroupBy 内存占用 | 分组数通常有限（tag 基数可控） |
| 算子链调用开销 | Go 接口调用开销极小，实际瓶颈在 IO |
| Proto oneof 扩展性 | oneof 支持向后兼容，新增算子加 case 即可 |
