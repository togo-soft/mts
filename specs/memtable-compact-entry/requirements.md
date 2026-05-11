# MemTable 紧凑条目优化 — 需求

## 背景

当前 MemTable 的 `entry` 结构直接嵌入完整的 `types.Point`，每个条目独立存储 `Database`、`Measurement`、`Tags` 等数据。这些字段在以下场景中是冗余的：

- **Database / Measurement**：同一个 Shard 内所有 entry 共享，属于 Shard 级常量。
- **Tags**：同一个 SID（时间序列）下所有 entry 共享相同的 Tags，当前逐条复制存储。

随着时间线（SID）数量增长和每条时间线数据点累积，MemTable 的内存占用成为瓶颈。

## 需求

### R1: 移除 entry 中的 Database 与 Measurement
当 `MemTable` 存储 entry 时，SHALL NOT 存储 `Database` 和 `Measurement` 字段。

### R2: 移除 entry 中的 Tags
当 `MemTable` 存储 entry 时，SHALL NOT 存储 `Tags` 字段。Tags SHALL 通过 `Sid` 从 `SeriesStore` 反查获得。

### R3: 保持读写正确性
当用户调用 `Read()` 或使用 `ShardIterator` 读取数据时，SHALL 返回包含完整 Tags 的 `PointRow`，与当前行为一致。

### R4: 保持 WAL 序列化不变
WAL 的 `serializePoint` / `deserializePoint` SHALL NOT 改变，WAL 数据中仍包含完整 Point（含 Tags），以保证 WAL 独立可恢复。

### R5: 保持 Flush → SSTable 正确性
`MemTable.Flush()` 返回的数据 SHALL 使 SSTable Writer 能正确写入（SSTable Writer 不使用 Tags/Database/Measurement）。

### R6: 内存占用显著降低
优化后 MemTable 单条目内存占用 SHALL 降低 ≥50%（以 `host=server1, value=42.5` 典型场景估算）。

### R7: 行覆盖率 ≥ 90%
所有新增和修改代码 SHALL 有测试覆盖，行覆盖率 ≥ 90%。

## 验收标准

- [ ] `MemTable.entry` 不再包含 `types.Point`，仅存储 `Timestamp` + `Fields` + `Sid`
- [ ] `Shard.Read()` 和 `ShardIterator` 通过 `Sid` 从 SeriesStore 恢复 Tags
- [ ] 所有现有单元测试通过
- [ ] 所有 E2E 测试通过
- [ ] `golangci-lint` 零告警
- [ ] 内存基准测试显示单条目内存降低 ≥ 50%
