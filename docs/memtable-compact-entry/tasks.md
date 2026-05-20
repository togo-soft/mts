# MemTable 紧凑条目优化 — 任务

## 任务清单

### T1: 重构 MemTable entry 结构 ✅
- [x] 将 `entry.Point types.Point` 替换为独立的 `Timestamp int64`、`Fields map[string]*types.FieldValue`、`Sid uint64`
- [x] 修改 `MemTable.Write()` 仅存储紧凑字段（移除 Database/Measurement/Tags 复制）
- [x] 修改 `MemTable.Flush()` 从紧凑字段重构 Point（Tags/Database/Measurement 留空）
- [x] 验收：`go test ./internal/storage/memtable/...` 通过

### T2: 扩展 MemTableIterator 接口 ✅
- [x] `Point()` 返回不含 Tags 的 Point（仅 Timestamp + Fields）
- [x] 新增 `Sid() uint64` 方法
- [x] 适配 `MemTableIterator` 内部实现
- [x] 验收：memtable 包测试通过

### T3: 适配 Shard.Read() Tags 恢复 ✅
- [x] `shard_io.go:Read()` 使用 `iter.Sid()` + `seriesStore.GetTagsBySID()` 恢复 Tags
- [x] nil seriesStore 防御处理
- [x] 验收：Shard 测试通过

### T4: 适配 ShardIterator Tags 恢复 ✅
- [x] `iterator.go:pointToRow()` 接收 Sid 参数，通过 seriesStore 恢复 Tags
- [x] `NewShardIterator()`、`nextMemRowLocked()` 传递 Sid
- [x] nil seriesStore 防御处理
- [x] 验收：ShardIterator 测试通过

### T5: 适配测试文件 ✅
- [x] 所有测试使用 `SeriesStore: nil` 的 Shard 通过 nil 防御自动兼容
- [x] 内存基准测试：由 E2E 写入测试间接覆盖
- [x] 验收：`go test ./internal/storage/...` 全部通过 (11/11)

### T6: E2E 测试验证 ✅
- [x] `simple_integrity` — 100 条数据完整性 ✅
- [x] `check_fields` — 字段类型验证 ✅
- [x] `check_schema` — Schema 正确性 ✅
- [x] `grpc_write_query` — gRPC 10K 写入+查询 ✅
- [x] `integrity` — 10 万数据点完整性 ✅
- [x] `persistence_test` — MetaStore 持久化 ✅
- [x] `wal_test` — WAL 6 项测试全部通过 ✅
- [x] `retention_test` — 数据过期清理 ✅
- [x] `compaction_test` — Compaction 8 项测试全部通过 ✅
- [x] `restart_recovery` — 10 次重启 1000 条数据累积验证 ✅

### T7: Lint 与格式化 ✅
- [x] `golangci-lint run` 零告警
- [x] `goimports-reviser` 格式化完成
