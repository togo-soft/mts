# 移除内部管线中的 Point 结构体 — 设计

## 1. 当前架构

```
                    ┌──── Point (protobuf) ────┐
                    │ Database, Measurement,    │
                    │ Tags, Timestamp, Fields,  │
                    │ protobuf overhead         │
                    └───────────────────────────┘
                              │
     ┌────────────────────────┼──────────────────────────┐
     ▼                        ▼                          ▼
  Shard.Write           MemTable.Write             SSTable.WritePoints
  (唯一需要 Tags 的        (存储完整 Point)           (只用 Timestamp+Fields)
   内部点: AllocateSID)
                              │
     ┌────────────────────────┼──────────────────────────┐
     ▼                        ▼                          ▼
  MemTable.Flush        MemTableIterator          Compaction.Merge
  (→ SSTable)            .Point()                  PointRow → Point → WritePoints
                         → pointToRow()            (低效转换)
```

## 2. 目标架构

```
  外部边界:  *types.Point (仅 gRPC API)
         │
         ▼
  Shard.Write ─── 转换边界 ───► InternalPoint{Timestamp, Fields[], Sid}
         │                              │
         ▼                              ├──► MemTable (存储/迭代)
  WAL serialize (Point)                 ├──► SSTable.WritePoints
  WAL deserialize → InternalPoint       ├──► Compaction
                                        └──► ShardIterator → PointRow
```

**核心原则：** `Shard.Write()` 是唯一的 Point → InternalPoint 转换点。之后所有模块只处理 `InternalPoint`。

## 3. 类型定义

### 3.1 InternalPoint（新增于 types 包）

```go
// types/internal.go

// InternalField 紧凑字段条目。
type InternalField struct {
    Key   string
    Value *FieldValue
}

// InternalPoint 内部管线中的数据点，不含外部协议开销。
type InternalPoint struct {
    Timestamp int64
    Fields    []InternalField
    Sid       uint64
}

// PointToInternal 将外部 Point 转换为 InternalPoint。
func PointToInternal(p *Point, sid uint64) InternalPoint {
    fields := make([]InternalField, 0, len(p.Fields))
    for k, v := range p.Fields {
        fields = append(fields, InternalField{Key: k, Value: v})
    }
    return InternalPoint{
        Timestamp: p.Timestamp,
        Fields:    fields,
        Sid:       sid,
    }
}

// InternalFieldsToMap 将 []InternalField 还原为 map[string]*FieldValue。
func InternalFieldsToMap(fields []InternalField) map[string]*FieldValue {
    if len(fields) == 0 {
        return nil
    }
    m := make(map[string]*FieldValue, len(fields))
    for _, f := range fields {
        m[f.Key] = f.Value
    }
    return m
}
```

## 4. 接口变更

### 4.1 MemTable

| 方法 | 当前 | 新 |
|------|------|-----|
| `Write` | `(p *types.Point, sid uint64) error` | `(ip types.InternalPoint) error` |
| `Flush` | `() ([]*types.Point, []uint64)` | `() []types.InternalPoint` |
| `Iterator.Point` | `() *types.Point` | `() types.InternalPoint` |
| `Iterator.Sid` | `() uint64` | **删除**（InternalPoint 已包含 Sid） |

### 4.2 SSTable Writer

| 方法 | 当前 | 新 |
|------|------|-----|
| `WritePoints` | `(points []*types.Point, sids []uint64) error` | `(points []types.InternalPoint) error` |
| `writePointWithSid` | `(p *types.Point, sid uint64) error` | `(ip types.InternalPoint) error` |

### 4.3 Shard

| 方法 | 变更 |
|------|------|
| `Write` | Point → InternalPoint 转换后传给 MemTable |
| `flushLocked` | 直接传递 `[]InternalPoint` 给 SSTable |
| `calcPointTimeRange` | 接受 `[]types.InternalPoint` |
| `ReplayWAL` | deserializePoint → AllocateSID → PointToInternal → MemTable.Write |
| `Read` | iter.Point() 返回 InternalPoint（已含 Sid） |

### 4.4 ShardIterator

| 方法 | 变更 |
|------|------|
| `pointToRow` | `(p *types.Point, sid uint64)` → `(ip types.InternalPoint)` |

### 4.5 Compaction

| 文件 | 变更 |
|------|------|
| `merge.go` | `PointRow → InternalPoint` 直接构造 |
| `level.go` | `PointRow → InternalPoint` 直接构造 |

## 5. 数据流详解

### 5.1 Write 路径

```
gRPC → *types.Point
  → Engine.Write(point)
    → Shard.Write(point)
      1. serializePoint(point) → WAL (不变)
      2. sid = seriesStore.AllocateSID(point.Tags)
      3. ValidateFieldTypes(point)
      4. ip = types.PointToInternal(point, sid)   ← 转换边界
      5. memTable.Write(ip)
```

### 5.2 Flush 路径

```
memTable.Flush() → []InternalPoint
  → calcPointTimeRange(internalPoints)
  → sstable.WritePoints(internalPoints)
    → 内部: 按 Timestamp + Fields + Sid 写入列式存储
```

### 5.3 Read 路径

```
ShardIterator:
  memRow ← memIter.Point()  → InternalPoint
  sstRow ← sstable.ReadRange() → PointRow (已有 Tags)

  pointToRow(ip InternalPoint) → PointRow:
    tags, _ = shard.seriesStore.GetTagsBySID(ip.Sid)
    return PointRow{
        Sid:       ip.Sid,
        Timestamp: ip.Timestamp,
        Tags:      tags,
        Fields:    types.InternalFieldsToMap(ip.Fields),
    }
```

### 5.4 Compaction 路径

```
MergeIterator → PointRow (row)
  → InternalPoint{
        Timestamp: row.Timestamp,
        Fields:    mapToInternalFields(row.Fields),
        Sid:       row.Sid,
    }
  → sstable.WritePoints(internalPoints)
```

## 6. 影响范围

```
types/internal.go              ← 新增 InternalPoint, InternalField, 转换函数
memtable/memtable.go           ← Write/Flush/Iterator 签名变更
shard/shard_io.go              ← Write/Read 适配
shard/shard_flush.go           ← flushLocked 适配
shard/shard_lifecycle.go       ← Close 中 flush 适配
shard/iterator.go              ← pointToRow 适配
shard/shard.go                 ← ReplayWAL 适配
sstable/writer_field.go        ← WritePoints 签名变更
sstable/writer.go              ← import 变更
compaction/merge.go            ← 消除 PointRow → Point 转换
compaction/level.go            ← 消除 PointRow → Point 转换
```

## 7. 不涉及

- WAL 序列化/反序列化（保持 `*types.Point`）
- gRPC API 层（保持 `*types.Point`）
- SSTable Reader（已返回 `PointRow`，不涉及 Point）
- SeriesStore / SchemaStore
- 所有测试文件中构造 `*types.Point` 的代码（测试可继续使用 Point 构造测试数据，但传给 WritePoints 时需适配）
