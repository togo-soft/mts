# 移除内部管线中的 Point 结构体 — 需求

## 背景

当前 `types.Point`（protobuf 生成）在整个内部管线中流转：

```
gRPC → Engine.Write → Shard.Write → MemTable.Write → MemTable.Flush
    → SSTable.WritePoints → Compaction.Merge → SSTable.WritePoints
    → MemTableIterator.Point → ShardIterator.pointToRow → Shard.Read
```

`Point` 携带完整的 `Database`、`Measurement`、`Tags` 等字段，但在 `Shard.Write()` 之后的内部管线中，这些字段不再需要：

- **Database / Measurement**：Shard 级别常量
- **Tags**：可通过 `Sid` 从 SeriesStore 恢复（已在上一次优化中实施）
- **protobuf 开销**：`MessageState`、`UnknownFields`、`SizeCache` 等字段对内部管线无用

此外，compaction 路径中还存在低效的 `PointRow → Point` 转换（仅为了传给 `WritePoints`，而 `WritePoints` 只使用 Timestamp + Fields）。

## 需求

### R1: 定义 InternalPoint 紧凑类型
SHALL 在 `types` 包中定义 `InternalPoint` 类型，仅包含内部管线所需字段：`Timestamp`、`Fields`（紧凑切片）、`Sid`。

### R2: Point → InternalPoint 转换在 Shard.Write() 边界完成
`Shard.Write()` 接收外部 `*types.Point` 后，SHALL 立即转换为 `InternalPoint`，后续所有内部操作 SHALL NOT 使用 `*types.Point`。

### R3: MemTable 仅处理 InternalPoint
`MemTable.Write()`、`MemTable.Flush()`、`MemTableIterator.Point()` SHALL 使用 `InternalPoint`。

### R4: SSTable Writer 接收 InternalPoint
`SSTable.WritePoints()` SHALL 接受 `[]InternalPoint` 替代 `[]*types.Point + []uint64`。

### R5: Compaction 路径消除 PointRow → Point 转换
Compaction 的 merge/level 模块 SHALL 直接构造 `InternalPoint` 传给 `WritePoints`，而非构造 `*types.Point`。

### R6: WAL 序列化不变
WAL 的 `serializePoint` / `deserializePoint` SHALL 保持使用 `*types.Point`（磁盘格式兼容性）。

### R7: 读路径正确性不变
`Shard.Read()` 和 `ShardIterator` SHALL 返回包含完整 Tags 的 `*types.PointRow`，与当前行为一致。

### R8: 所有测试通过
单元测试和 E2E 测试 SHALL 全部通过。

## 验收标准

- [ ] `types` 包定义 `InternalPoint` 和 `InternalField` 类型
- [ ] `types.Point` 仅出现在 API 层（gRPC）和 WAL 序列化中
- [ ] `MemTable`、`SSTable Writer`、`Compaction` 不再 import `types.Point`（或仅用于测试）
- [ ] 所有单元测试通过
- [ ] 所有 E2E 测试通过
- [ ] golangci-lint 零告警
