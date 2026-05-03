# micro-ts 流式查询与分页实现设计

## 1. 概述

本文档描述 micro-ts 流式查询（Streaming Query）和分页查询的实现设计，参考 InfluxDB 迭代器模式和 VictoriaMetrics 时间区间查询优化。

**目标**：
- 实现内存可控的流式查询
- 支持分页查询（Offset/Limit）
- 利用块级时间索引优化 SSTable 读取
- 支持多 Shard 并发归并排序

---

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                         Query 执行流程                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  第一层：Shard 时间边界过滤（ShardManager.GetShards）             │
│       │                                                         │
│       ├── 根据 startTime/endTime 返回相交的 Shards               │
│       └── Shard 自带 startTime/endTime 边界                     │
│                           │                                     │
│                           ▼                                     │
│  第二层：QueryIterator（流式迭代）                               │
│       │                                                         │
│       ├── ShardIterator（归并 MemTable + SSTable）              │
│       │       - 按 timestamp 有序输出                             │
│       │                                                         │
│       └── tag filter / field projection                         │
│                           │                                     │
│                           ▼                                     │
│                    Limit / Offset                               │
│                           │                                     │
│                           ▼                                     │
│                    返回给 caller                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 核心设计原则

| 层级 | 过滤方式 | 说明 |
|------|----------|------|
| 第一层 | Shard 时间边界 | GetShards 返回与查询范围相交的 Shards |
| 第二层 | ShardIterator 归并 | MemTable + SSTable 按 timestamp 有序归并 |
| 第三层 | tag/field filter | 应用标签过滤和字段投影 |
| 第四层 | Limit/Offset | 控制返回数量 |

### 2.2 核心组件

#### QueryIterator

```go
// QueryIterator 流式查询迭代器
type QueryIterator struct {
    ctx      context.Context
    req      *types.QueryRangeRequest
    shards   []*Shard
    curShard int

    // 当前 shard 的迭代器
    memIter  *MemTableIterator
    sstIter  *SSTableIterator

    // 合并后的 peek
    peekRow  *types.PointRow
    consumed int64 // 已返回的行数
    skipped  int64 // 已跳过的行数（用于 offset）
}

// Next 移动到下一个点
func (q *QueryIterator) Next() bool {
    for {
        // 1. 如果有 peek，先返回 peek
        if q.peekRow != nil {
            // 应用 offset
            if q.skipped < q.req.Offset {
                q.skipped++
                continue
            }
            // 应用 limit
            if q.req.Limit > 0 && q.consumed >= q.req.Limit {
                return false
            }
            q.consumed++
            return true
        }

        // 2. 从当前 shard 迭代器获取下一个点
        row := q.nextFromCurrentShard()
        if row == nil {
            // 3. 当前 shard 耗尽，切换到下一个 shard
            if !q.switchToNextShard() {
                return false
            }
            continue
        }

        // 4. 应用 tag filter
        if !q.matchTags(row) {
            continue
        }

        q.peekRow = row
    }
}

// Points 返回当前点
func (q *QueryIterator) Points() *types.PointRow {
    return q.peekRow
}
```

#### ShardIterator

```go
// ShardIterator 单个 Shard 的迭代器
type ShardIterator struct {
    shard    *Shard
    memIter  *MemTableIterator  // MemTable 迭代器
    sstIter  *SSTableIterator  // SSTable 迭代器

    // 当前 peek
    memRow  *types.PointRow
    sstRow  *types.PointRow
}

// Next 返回下一个有序点（按 timestamp 升序）
func (si *ShardIterator) Next() *types.PointRow {
    for {
        // 如果 MemTable 和 SSTable 都有数据，取 timestamp 较小的
        if si.memRow != nil && si.sstRow != nil {
            if si.memRow.Timestamp < si.sstRow.Timestamp {
                row := si.memRow
                si.memRow = si.memIter.Next()
                return row
            } else {
                row := si.sstRow
                si.sstRow = si.sstIter.Next()
                return row
            }
        }

        // 只剩 MemTable
        if si.memRow != nil {
            row := si.memRow
            si.memRow = si.memIter.Next()
            return row
        }

        // 只剩 SSTable
        if si.sstRow != nil {
            row := si.sstRow
            si.sstRow = si.sstIter.Next()
            return row
        }

        // 都耗尽了
        return nil
    }
}
```

#### SSTable 块级时间索引

**文件结构**：
```
┌─────────────────────────────────────────┐
│  Header                                 │
│  - magic: 0x5453455250454746           │
│  - version: 1                          │
│  - min_timestamp, max_timestamp        │
├─────────────────────────────────────────┤
│  Block 0                               │
│  - timestamp delta encoding            │
│  - sid data...                         │
│  - field data...                       │
├─────────────────────────────────────────┤
│  Block 1                               │
│  ...                                   │
├─────────────────────────────────────────┤
│  Index Block                           │
│  - block_count: N                      │
│  - index[0]: min_ts, max_ts, offset   │
│  - index[1]: min_ts, max_ts, offset   │
│  ...                                   │
├─────────────────────────────────────────┤
│  Footer                                │
│  - index_offset                        │
└─────────────────────────────────────────┘
```

**Index Entry (20 bytes)**：
```
[8 bytes: min_timestamp]
[8 bytes: max_timestamp]
[4 bytes: block_offset]
```

**seekBlock 二分查找**：
```go
func (r *SSTableReader) seekBlock(targetTime int64) (blockIndex int, err error) {
    // 1. 加载 index block（在文件尾部 footer 指向）
    // 2. 二分查找：targetTime >= index[i].min_ts && targetTime < index[i].max_ts
    // 3. 返回匹配的 block 偏移量
}
```

---

## 3. API 设计

### 3.1 新增 QueryIterator 方法

```go
// QueryIterator 创建流式查询迭代器
func (db *DB) QueryIterator(ctx context.Context, req *types.QueryRangeRequest) (*QueryIterator, error)

// Next 移动到下一个点，返回 false 表示结束
func (it *QueryIterator) Next() bool

// Points 返回当前点
func (it *QueryIterator) Points() *types.PointRow

// Close 关闭迭代器
func (it *QueryIterator) Close() error
```

### 3.2 重新实现 QueryRange

```go
// QueryRange 内部使用 Iterator 实现
func (db *DB) QueryRange(ctx context.Context, req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
    it, err := db.QueryIterator(ctx, req)
    if err != nil {
        return nil, err
    }
    defer it.Close()

    var rows []types.PointRow
    for it.Next() {
        row := it.Points()
        rows = append(rows, *row)
        // 注意：不再检查 limit，因为 iterator 已经处理
    }

    return &types.QueryRangeResponse{
        Database:    req.Database,
        Measurement: req.Measurement,
        StartTime:   req.StartTime,
        EndTime:     req.EndTime,
        TotalCount:  int64(len(rows)), // 流式无法预知总数，暂返回实际数量
        Rows:        rows,
    }, nil
}
```

---

## 4. 分页实现

### 4.1 Iterator 内部 offset 处理

```go
func (q *QueryIterator) Next() bool {
    for {
        if q.peekRow != nil {
            // 应用 offset：跳过前 N 条
            if q.skipped < q.req.Offset {
                q.skipped++
                q.peekRow = nil
                continue
            }

            // 应用 limit
            if q.req.Limit > 0 && q.consumed >= q.req.Limit {
                return false
            }

            q.consumed++
            return true
        }
        // ...
    }
}
```

### 4.2 分页查询流程

```
查询第 N 页（每页 100 条）：
  offset = (page-1) * limit
  limit = limit

Iterator 内部：
  - 跳过前 offset 条（不占用内存返回给 caller）
  - 返回最多 limit 条
```

---

## 5. 多 Shard 归并排序

### 5.1 Min-Heap 归并

```go
type mergeIterator struct {
    shards []*ShardIterator
    heap   []*ShardIterator
}

// 维护 min-heap，按 timestamp 排序
func (m *mergeIterator) Next() *types.PointRow {
    if len(m.heap) == 0 {
        return nil
    }

    // 弹出最小元素
    si := heap.Pop(m.heap).(*ShardIterator)
    row := si.Current()

    // 补充该 shard 的下一个元素
    if next := si.Next(); next != nil {
        heap.Push(m.heap, si)
    }

    return row
}
```

### 5.2 并发读取

```go
// 启动多个 goroutine 读取不同 shard
rowsCh := make(chan []types.PointRow, len(shards))
var wg sync.WaitGroup

for _, s := range shards {
    wg.Add(1)
    go func(s *Shard) {
        defer wg.Done()
        iter := NewShardIterator(s, req.StartTime, req.EndTime)
        var batch []types.PointRow
        for row := iter.Next(); row != nil; row = iter.Next() {
            batch = append(batch, *row)
            if len(batch) >= batchSize {
                rowsCh <- batch
                batch = batch[:0]
            }
        }
        if len(batch) > 0 {
            rowsCh <- batch
        }
    }(s)
}
```

---

## 6. 内存优化

### 6.1 关键优化点

| 优化点 | 实现方式 |
|--------|----------|
| 不一次性加载所有数据 | 使用 Iterator 逐条读取 |
| SSTable 块级读取 | 只读取命中的 block，不扫描全表 |
| 分批归并 | batchSize 控制内存使用 |
| offset 跳过优化 | 跳过时不构建完整 row |

### 6.2 内存使用估算

| 数据规模 | 内存占用 |
|----------|----------|
| 查询 100 条 | ~10KB |
| 查询 1M 条（分批） | batchSize × 单条大小 |
| SSTable block | ~64KB per block |

---

## 7. 实现计划

### Phase 1: ShardIterator
- [ ] 实现 ShardIterator，按 timestamp 归并 MemTable + SSTable
- [ ] 实现 ShardIterator 的 Next/Points 方法

### Phase 2: QueryIterator
- [ ] 实现 QueryIterator，内部管理多 Shard 迭代
- [ ] 实现 tag filter 和 field projection
- [ ] 实现 offset/limit 逻辑

### Phase 3: API 集成
- [ ] DB.QueryIterator 方法
- [ ] 重构 DB.QueryRange 使用 Iterator
- [ ] E2E 测试验证

---

## 8. 验收标准

- [ ] 查询 1M 数据，内存占用不随数据量增长（受 batchSize 限制）
- [ ] 分页查询正确返回指定 offset/limit
- [ ] 多 Shard 查询结果按时间有序归并
- [ ] E2E 完整性测试通过