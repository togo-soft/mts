# Streaming Query 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现内存可控的流式查询和分页查询，参考 InfluxDB 迭代器模式和 VictoriaMetrics 时间区间查询能力

**Architecture:** 采用 4 层过滤架构：(1) Shard 时间边界过滤 → (2) ShardIterator 归并 MemTable+SSTable → (3) tag/field filter → (4) Limit/Offset。QueryIterator 以流式方式逐条返回数据，不一次性加载所有数据到内存。

**Tech Stack:** Go 原生实现，无外部依赖

---

## 文件结构

| 文件 | 职责 |
|------|------|
| `internal/storage/shard/sstable/iterator.go` | SSTable 迭代器，按 timestamp 有序遍历 |
| `internal/storage/shard/iterator.go` | ShardIterator，归并 MemTable + SSTable |
| `internal/query/iterator.go` | QueryIterator，顶级迭代器，支持多 Shard、tag/field 过滤、offset/limit |
| `internal/engine/engine.go` | 添加 QueryIterator 方法 |
| `microts.go` | 暴露 DB.QueryIterator API |

---

## Task 1: SSTableIterator 实现

SSTable 迭代器，按 timestamp 有序遍历 SSTable 数据。

**Files:**
- Create: `internal/storage/shard/sstable/iterator.go`
- Test: `internal/storage/shard/sstable/iterator_test.go`

- [ ] **Step 1: 创建 SSTableIterator 结构**

```go
// internal/storage/shard/sstable/iterator.go
package sstable

// Iterator SSTable 迭代器
type Iterator struct {
    reader     *Reader
    timestamps []int64
    pos        int
    fields     []string
    fieldData  map[string][]byte
}
```

- [ ] **Step 2: 实现 NewIterator 方法**

```go
// NewIterator 创建迭代器
func (r *Reader) NewIterator() (*Iterator, error) {
    // 读取 timestamps 和 fields 数据
    // 不在这里读取所有数据，按需加载
}
```

- [ ] **Step 3: 实现 Next 方法**

```go
// Next 移动到下一个点
func (it *Iterator) Next() bool {
    it.pos++
    return it.pos < len(it.timestamps)
}
```

- [ ] **Step 4: 实现 Point 方法**

```go
// Point 返回当前点的数据
func (it *Iterator) Point() *types.PointRow {
    // 根据 pos 从 fieldData 构建 PointRow
}
```

- [ ] **Step 5: 编写单元测试**

测试场景：
- 空 SSTable
- 单条数据
- 多条数据按时间顺序
- Next 超出范围

---

## Task 2: ShardIterator 实现

ShardIterator 归并 MemTable 和 SSTable 的数据，按 timestamp 有序输出。

**Files:**
- Create: `internal/storage/shard/iterator.go`
- Modify: `internal/storage/shard/memtable.go:80-103` (已有 Iterator，扩展支持)
- Test: `internal/storage/shard/iterator_test.go`

- [ ] **Step 1: 创建 ShardIterator 结构**

```go
// internal/storage/shard/iterator.go
package shard

// ShardIterator 单个 Shard 的迭代器
type ShardIterator struct {
    shard    *Shard
    memIter  *MemTableIterator  // MemTable 迭代器
    sstIter  *sstable.Iterator  // SSTable 迭代器

    // 当前 peek
    memRow  *types.PointRow
    sstRow  *types.PointRow
}
```

- [ ] **Step 2: 实现 NewShardIterator 工厂方法**

```go
// NewShardIterator 创建 Shard 迭代器
func NewShardIterator(shard *Shard, startTime, endTime int64) *ShardIterator {
    // 创建 MemTable 迭代器
    // 创建 SSTable 迭代器
    // 初始化 peek
}
```

- [ ] **Step 3: 实现 Next 方法（核心归并逻辑）**

```go
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

- [ ] **Step 4: 编写单元测试**

测试场景：
- 只有 MemTable 数据
- 只有 SSTable 数据
- MemTable 和 SSTable 都有数据（验证归并顺序）
- 时间戳相等的边界情况

---

## Task 3: QueryIterator 实现

QueryIterator 是顶级迭代器，管理多 Shard 迭代，支持 tag/field 过滤和 offset/limit。

**Files:**
- Create: `internal/query/iterator.go`
- Test: `internal/query/iterator_test.go`

- [ ] **Step 1: 创建 QueryIterator 结构**

```go
// internal/query/iterator.go
package query

import (
    "context"
    "micro-ts/internal/storage/shard"
    "micro-ts/internal/types"
)

// QueryIterator 流式查询迭代器
type QueryIterator struct {
    ctx      context.Context
    req      *types.QueryRangeRequest

    // Shard 迭代器们
    shards       []*shard.Shard
    shardIters   []*shard.ShardIterator
    curShardIdx  int

    // Min-heap 用于多 Shard 归并排序
    heap []*shard.ShardIterator

    // 合并后的 peek
    peekRow  *types.PointRow
    consumed int64 // 已返回的行数
    skipped  int64 // 已跳过的行数（用于 offset）
}
```

- [ ] **Step 2: 实现 NewQueryIterator 工厂方法**

```go
// NewQueryIterator 创建流式查询迭代器
func NewQueryIterator(ctx context.Context, shards []*shard.Shard, req *types.QueryRangeRequest) *QueryIterator {
    // 为每个 Shard 创建 ShardIterator
    // 构建初始 heap
    // peek 第一个元素
}
```

- [ ] **Step 3: 实现 tag 过滤逻辑**

```go
// matchTags 检查 row 是否匹配 tag 过滤条件
func (q *QueryIterator) matchTags(row *types.PointRow) bool {
    for k, v := range q.req.Tags {
        if row.Tags[k] != v {
            return false
        }
    }
    return true
}
```

- [ ] **Step 4: 实现 field 投影逻辑**

```go
// projectFields 对 row 进行字段投影
func (q *QueryIterator) projectFields(row *types.PointRow) *types.PointRow {
    if len(q.req.Fields) == 0 {
        return row
    }
    // 只保留请求的字段
}
```

- [ ] **Step 5: 实现 Next 方法**

```go
// Next 移动到下一个点
func (q *QueryIterator) Next() bool {
    for {
        // 1. 如果有 peek，先处理
        if q.peekRow != nil {
            // 应用 offset
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

        // 2. 从 heap 取出最小元素
        if len(q.heap) == 0 {
            return false
        }

        // 3. 弹出最小 timestamp 的 ShardIterator
        si := heap.Pop(q.heap).(*shard.ShardIterator)
        row := si.Current()

        // 4. 补充该 shard 的下一个元素
        if next := si.Next(); next != nil {
            heap.Push(q.heap, si)
        }

        // 5. 应用 tag filter
        if !q.matchTags(row) {
            continue
        }

        // 6. 应用 field projection
        q.peekRow = q.projectFields(row)
    }
}
```

- [ ] **Step 6: 实现 Points 和 Close 方法**

```go
// Points 返回当前点
func (q *QueryIterator) Points() *types.PointRow {
    return q.peekRow
}

// Close 关闭迭代器
func (q *QueryIterator) Close() error {
    // 关闭所有 ShardIterator
}
```

- [ ] **Step 7: 编写单元测试**

测试场景：
- 空 Shard 列表
- 单 Shard 基本查询
- 多 Shard 归并排序验证
- tag 过滤
- field 投影
- offset 跳过
- limit 限制
- offset + limit 组合

---

## Task 4: Engine.QueryIterator 方法

在 Engine 中添加 QueryIterator 方法。

**Files:**
- Modify: `internal/engine/engine.go`

- [ ] **Step 1: 添加 QueryIterator 方法**

```go
// QueryIterator 创建流式查询迭代器
func (e *Engine) QueryIterator(ctx context.Context, req *types.QueryRangeRequest) (*query.QueryIterator, error) {
    // 获取相交的 Shards
    shards := e.shardManager.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)
    if len(shards) == 0 {
        return nil, nil
    }

    return query.NewQueryIterator(ctx, shards, req), nil
}
```

- [ ] **Step 2: 验证编译通过**

Run: `go build ./...`

---

## Task 5: DB.QueryIterator API

在 DB 层暴露 QueryIterator 方法。

**Files:**
- Modify: `microts.go`

- [ ] **Step 1: 添加 QueryIterator 方法**

```go
// QueryIterator 创建流式查询迭代器
func (db *DB) QueryIterator(ctx context.Context, req *types.QueryRangeRequest) (*query.QueryIterator, error) {
    return db.engine.QueryIterator(ctx, req)
}
```

- [ ] **Step 2: 验证编译通过**

Run: `go build ./...`

---

## Task 6: 重构 DB.QueryRange 使用 Iterator

将 DB.QueryRange 重构为内部使用 QueryIterator 实现。

**Files:**
- Modify: `microts.go`

- [ ] **Step 1: 重构 QueryRange 使用 Iterator**

```go
// QueryRange 范围查询（内部使用 Iterator 实现）
func (db *DB) QueryRange(ctx context.Context, req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
    it, err := db.QueryIterator(ctx, req)
    if err != nil {
        return nil, err
    }
    if it == nil {
        return &types.QueryRangeResponse{
            Database:    req.Database,
            Measurement: req.Measurement,
            StartTime:   req.StartTime,
            EndTime:     req.EndTime,
            TotalCount:  0,
            Rows:        []types.PointRow{},
        }, nil
    }
    defer it.Close()

    var rows []types.PointRow
    for it.Next() {
        row := it.Points()
        rows = append(rows, *row)
    }

    return &types.QueryRangeResponse{
        Database:    req.Database,
        Measurement: req.Measurement,
        StartTime:   req.StartTime,
        EndTime:     req.EndTime,
        TotalCount:  int64(len(rows)),
        Rows:        rows,
    }, nil
}
```

- [ ] **Step 2: 验证编译通过**

Run: `go build ./...`

---

## Task 7: E2E 集成测试

使用现有的 e2e 测试验证流式查询功能。

**Files:**
- Modify: `tests/e2e/integrity/main.go` (如需要)

- [ ] **Step 1: 运行 integrity 测试**

Run: `go run tests/e2e/integrity/main.go`
Expected: PASS - 数据完整性验证通过

- [ ] **Step 2: 运行 query_1m 测试**

Run: `go run tests/e2e/query_1m/main.go`
Expected: 内存占用不随数据量增长（应明显低于 1GB）

- [ ] **Step 3: 验证分页功能**

创建临时测试脚本验证 offset/limit 正确性

---

## 验收标准

- [ ] SSTableIterator 单元测试通过
- [ ] ShardIterator 单元测试通过
- [ ] QueryIterator 单元测试通过
- [ ] `go build ./...` 编译通过
- [ ] E2E integrity 测试通过
- [ ] 查询 1M 数据时内存占用稳定（受 batchSize 控制）

---

## 已知约束

1. 当前 SSTable 读取会一次性加载所有 timestamps 和 fields 数据到内存
2. 后续可优化为块级读取（Block-level read），但不是当前阶段目标
3. ShardIterator 使用简单归并，未使用 min-heap（因为 shard 数量通常较少）
