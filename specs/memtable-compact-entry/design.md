# MemTable 紧凑条目优化 — 设计

## 1. 当前架构分析

### 1.1 数据结构

```
┌─ entry ─────────────────────────────────┐
│  Point (types.Point)                     │
│    Database    string          ~20B      │
│    Measurement string          ~20B      │
│    Tags        map[string]str  ~93B      │ ← 同 SID 重复存储
│    Timestamp   int64             8B      │
│    Fields      map[str]*FVal   ~95B      │
│  Sid          uint64             8B      │
│──────────────────────────────────────────│
│  ≈291B / entry                           │
└──────────────────────────────────────────┘
```

### 1.2 数据流分析

```
Write Path:
  Point → serializePoint() → WAL (完整 Point)
  Point → AllocateSID(Tags) → sid
  Point, sid → MemTable.Write() → entry{Point, sid}

Flush Path:
  MemTable.Flush() → []*Point, []uint64
  SSTable.WritePoints(points, sids)
    └── 仅使用 p.Timestamp, p.Fields, sid  ← Tags/Database/Measurement 未使用!

Read Path (MemTable):
  MemTableIterator.Point() → *Point (含 Tags)
  pointToRow() → PointRow{Timestamp, Tags, Fields}

Read Path (SSTable):
  Reader.ReadRange() → []*PointRow{Sid, Timestamp, Fields}
  seriesStore.GetTagsBySID(Sid) → Tags    ← 通过 Sid 恢复 Tags
```

### 1.3 关键发现

1. **SSTableWriter 不使用 Tags/Database/Measurement**（`writer_field.go:68-96`），Flush 时构造的 Point 无需携带 Tags
2. **SSTable 读取路径已通过 seriesStore 恢复 Tags**（`shard_io.go:198-205`），MemTable 路径可统一此方式
3. **WAL 序列化独立于 MemTable entry 结构**，优化不影响 WAL

## 2. 目标架构

### 2.1 新 entry 结构

```go
type entry struct {
    Timestamp int64
    Fields    map[string]*types.FieldValue
    Sid       uint64
}
```

移除：`Database`, `Measurement`, `Tags`（及 `Point` 包装的 protobuf 开销）

预期大小：`8 + 95 + 8 = ≈111B`，降幅 **62%**。

### 2.2 Interface 变更

**MemTable：**

| 方法 | 当前签名 | 新签名 | 说明 |
|------|---------|--------|------|
| `Write` | `(p *types.Point, sid uint64) error` | 不变 | 内部仅提取 Timestamp + Fields + Sid |
| `Flush` | `() ([]*types.Point, []uint64)` | `() ([]*types.Point, []uint64)` | 返回的 Point 中 Tags/Database/Measurement 为零值 |
| `Iterator().Point()` | `() *types.Point` | `() (*types.Point, uint64)` | 新增返回 Sid，Tags 由调用方通过 seriesStore 恢复 |
| `Iterator().Sid()` | — | `() uint64` | 新增方法 |

**ShardIterator / Shard.Read()：**

调用 `memIter.Point()` 后，通过 `shard.seriesStore.GetTagsBySID(sid)` 恢复 Tags 填充到 PointRow。

### 2.3 读路径 Tags 恢复（统一流程）

```
当前:
  MemTable 数据 → Point.Tags (直接使用)
  SSTable 数据  → seriesStore.GetTagsBySID(sid)

优化后:
  MemTable 数据 → seriesStore.GetTagsBySID(sid)  ← 统一路径
  SSTable 数据  → seriesStore.GetTagsBySID(sid)
```

## 3. 详细设计

### 3.1 `MemTable.Write` 变更

```go
func (m *MemTable) Write(p *types.Point, sid uint64) error {
    // 仅提取必要字段
    fields := make(map[string]*types.FieldValue, len(p.Fields))
    for k, v := range p.Fields {
        fields[k] = v
    }
    
    m.entries = append(m.entries, &entry{
        Timestamp: p.Timestamp,
        Fields:    fields,
        Sid:       sid,
    })
    // ... 排序逻辑不变
}
```

### 3.2 `MemTable.Flush` 变更

```go
func (m *MemTable) Flush() ([]*types.Point, []uint64) {
    // ...
    for i, e := range result {
        points[i] = &types.Point{
            Timestamp: e.Timestamp,
            Fields:    e.Fields,
            // Database, Measurement, Tags 为零值（SSTable Writer 不使用）
        }
        sids[i] = e.Sid
    }
}
```

### 3.3 `MemTableIterator` 变更

```go
// Point 返回 Point（Tags 为空，需调用方通过 Sid 恢复）
func (i *MemTableIterator) Point() *types.Point {
    e := i.entries[i.pos]
    return &types.Point{
        Timestamp: e.Timestamp,
        Fields:    e.Fields,
    }
}

// Sid 返回当前条目的 Series ID
func (i *MemTableIterator) Sid() uint64 {
    return i.entries[i.pos].Sid
}
```

### 3.4 `ShardIterator.pointToRow` 变更

```go
func (si *ShardIterator) pointToRow(p *types.Point, sid uint64) *types.PointRow {
    tags, _ := si.shard.seriesStore.GetTagsBySID(sid)
    return &types.PointRow{
        Sid:       sid,
        Timestamp: p.Timestamp,
        Tags:      tags,
        Fields:    p.Fields,
    }
}
```

### 3.5 `Shard.Read` 变更

```go
for iter.Next() {
    p := iter.Point()
    sid := iter.Sid()
    if p.Timestamp >= startTime && p.Timestamp < endTime {
        tags, _ := s.seriesStore.GetTagsBySID(sid)
        rows = append(rows, &types.PointRow{
            Timestamp: p.Timestamp,
            Tags:      tags,
            Fields:    p.Fields,
        })
    }
}
```

## 4. 内存估算

以单条时间线 `host=server1`，字段 `value=42.5`，3000 entries 为例：

| 字段 | 优化前/entry | 优化后/entry | 节省 |
|------|:---:|:---:|---:|
| protobuf overhead | ~48B | 0B | 48B |
| Database | ~20B | 0B | 20B |
| Measurement | ~20B | 0B | 20B |
| Tags map | ~93B | 0B | 93B |
| Timestamp | 8B | 8B | 0B |
| Fields map | ~95B | ~95B | 0B |
| Sid | 8B | 8B | 0B |
| **合计** | **~292B** | **~111B** | **~181B (62%)** |

3000 entries: **876KB → 333KB**，节省 **543KB**。

## 5. 风险与权衡

| 风险 | 缓解措施 |
|------|---------|
| Tags 恢复增加 seriesStore 查询开销 | seriesStore 内部已有内存缓存（MeasSeriesStore 在 boltDB 之外维护内存 map） |
| Iterator 接口变更影响调用方 | 仅 Shard 内部使用，外部无直接调用 MemTableIterator |
| Point 中 Tags 为零值被误用 | 在 Point 文档中标注 "Tags not populated from MemTable, use Sid to recover" |

## 6. 影响范围

```
internal/storage/memtable/memtable.go     ← entry struct, Write, Flush, Iterator
internal/storage/shard/iterator.go        ← pointToRow, Next, Current
internal/storage/shard/shard_io.go        ← Read
internal/storage/shard/shard_flush.go     ← flushLocked (可能不需要改)
内部测试文件                                 ← 适配新结构
```

不涉及：WAL、SSTable Writer/Reader、SeriesStore、Compaction、gRPC API。
