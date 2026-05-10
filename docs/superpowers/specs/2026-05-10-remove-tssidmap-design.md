# 移除 tsSidMap 设计文档

## 目标

消除 `Shard.tsSidMap`（`map[int64][]uint64`），将 SID 信息下沉到 MemTable entry，并将 `WritePoints` 的 SID 传递从 timestamp 查找改为并行数组索引。

## 动机

`tsSidMap` 是一个不必要的中间层。它存在的唯一原因是 `types.Point` 不携带 SID 字段，而 SSTable Writer 需要 SID。SID 在 `MemTable.Write()` 之前就已分配好，完全可以直接存入 MemTable entry，无需维护额外的映射表。

## 核心变更

### 1. MemTable（`internal/storage/memtable/memtable.go`）

- `entry` 新增 `Sid uint64` 字段
- `Write(p *types.Point, sid uint64)` 签名加 sid 参数
- `Flush() ([]*types.Point, []uint64)` 返回两个并行切片
- `Iterator` 不变——读路径 Tags 已在 Point 内，不需要 SID

### 2. SSTable Writer（`internal/storage/shard/sstable/writer_field.go`）

```
// 旧签名
func (w *Writer) WritePoints(points []*types.Point, tsSidMap map[int64][]uint64) error

// 新签名
func (w *Writer) WritePoints(points []*types.Point, sids []uint64) error
```

内部逻辑：`points[i]` 对应 `sids[i]`，不再需要 timestamp 查找和切片切除操作。

### 3. Shard（`internal/storage/shard/shard.go`）

删除 `tsSidMap map[int64][]uint64` 字段及初始化代码。

### 4. Shard 写入路径（`shard_io.go`）

```
// 旧
s.tsSidMap[point.Timestamp] = append(s.tsSidMap[point.Timestamp], sid)
s.memTable.Write(point)

// 新
s.memTable.Write(point, sid)
```

### 5. Shard 刷盘路径（`shard_flush.go`）

```
// 旧
points := s.memTable.Flush()
w.WritePoints(points, s.tsSidMap)
for _, p := range points { delete(s.tsSidMap, p.Timestamp) }

// 新
points, sids := s.memTable.Flush()
w.WritePoints(points, sids)
// 不需要清理
```

### 6. Shard 生命周期（`shard_lifecycle.go`）

- Close 中删除 `tsSidMap` 清理代码
- WAL Replay 中 `tsSidMap` 写入改为 `memTable.Write(point, sid)`

### 7. Compaction Merge（`compaction/merge.go`）

```
// 旧：构建 tsSidMap
tsSidMap[row.Timestamp] = append(tsSidMap[row.Timestamp], row.Sid)

// 新：构建并行 sids 切片
sids = append(sids, row.Sid)
```

### 8. Level Compaction（`compaction/level.go`）

与 merge.go 相同变更。

## 不变项

- SSTable 磁盘格式（`_sids.bin`、`_timestamps.bin`、`_index.bin`）
- `sidCache` 保留（读路径需要 SID→Tags 反查）
- `SeriesStore` 接口
- SSTable Reader 全部不变
- MemTable Iterator 不变

## 数据流对比

```
写入 (旧):
  AllocateSID → tsSidMap[ts].append(sid) → MemTable.Write(Point)

写入 (新):
  AllocateSID → MemTable.Write(Point, sid)

刷盘 (旧):
  Flush() → []*Point → WritePoints(points, tsSidMap) → tsSidMap 清理

刷盘 (新):
  Flush() → ([]*Point, []uint64) → WritePoints(points, sids) → 无需清理

Compaction (旧):
  PointRow(Sid) → 构建 tsSidMap → WritePoints(points, tsSidMap)

Compaction (新):
  PointRow(Sid) → 构建 []uint64 → WritePoints(points, sids)
```

## 涉及文件

| 文件 | 变更类型 |
|------|----------|
| `memtable/memtable.go` | 改 entry、Write、Flush |
| `memtable/memtable_test.go` | 适配测试 |
| `sstable/writer_field.go` | 改 WritePoints 签名与逻辑 |
| `sstable/writer_test.go` | 适配测试 |
| `shard/shard.go` | 删除 tsSidMap 字段 |
| `shard/shard_io.go` | Write 路径适配 |
| `shard/shard_flush.go` | Flush 路径适配 |
| `shard/shard_lifecycle.go` | Close + ReplayWAL 适配 |
| `compaction/merge.go` | tsSidMap → []uint64 |
| `compaction/level.go` | tsSidMap → []uint64 |
| `shard/level_compaction_e2e_test.go` | 适配测试 |
| `tests/e2e/compaction_test/main.go` | 可能适配 |

## 风险

- 影响核心写入路径和 Compaction，需全面测试
- 回退难度低，tsSidMap 可随时加回
- 收益：消除可变 map、去掉间接查找、代码更直观
