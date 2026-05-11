# 移除内部管线中的 Point 结构体 — 任务

## T1: 定义 InternalPoint 类型
- [x] 在 `types/` 下新建 `internal.go`，定义 `InternalPoint`、`InternalField`
- [x] 添加 `PointToInternal()`、`InternalFieldsToMap()` 转换函数
- [x] 更新 `memtable.go` 移除 `FieldEntry`，改用 `types.InternalField`
- [x] 添加 `MapToInternalFields()` 反向转换函数（compaction 需要）

## T2: 重构 MemTable → InternalPoint
- [x] `Write(ip types.InternalPoint) error`
- [x] `Flush() []types.InternalPoint`
- [x] `Iterator.Point() types.InternalPoint`
- [x] 移除 `Iterator.Sid()`（InternalPoint 已包含）
- [x] 移除 `fieldsToMap()`（使用 `types.InternalFieldsToMap`）

## T3: 重构 SSTable Writer → InternalPoint
- [x] `WritePoints(points []types.InternalPoint) error`
- [x] `writePointWithSid` → `writeInternalPoint(ip types.InternalPoint)`
- [x] 删除对 `types.Point` 字段的依赖（`p.Fields` → `ip.Fields`）

## T4: 重构 Shard 适配
- [x] `Write()` — Point → InternalPoint 转换边界
- [x] `flushLocked()` — 传递 `[]InternalPoint`
- [x] `calcPointTimeRange()` — 接受 `[]InternalPoint`
- [x] `Close()` — 适配新 Flush 签名
- [x] `Read()` — 适配新 Iterator
- [x] `ReplayWAL()` — deserializePoint → InternalPoint

## T5: 重构 ShardIterator → InternalPoint
- [x] `pointToRow(ip types.InternalPoint)`
- [x] `NewShardIterator()` / `nextMemRowLocked()` 适配

## T6: 重构 Compaction → InternalPoint
- [x] `merge.go` — PointRow → InternalPoint 直接构造
- [x] `level.go` — PointRow → InternalPoint 直接构造

## T7: 适配测试
- [x] memtable 测试
- [x] sstable 测试（WritePoints 签名变更）
- [x] shard 测试
- [x] compaction 测试

## T8: E2E + Lint
- [x] 全部单元测试通过（13 个包）
- [x] E2E 测试通过（simple_integrity, wal_test, compaction_test, check_fields, check_schema, write_1k）
- [x] golangci-lint 零告警
- [x] goimports-reviser 格式化完成

## 依赖关系

```
T1 → T2 → T3 → T4 → T5 → T6 → T7 → T8
                  ↘ T5, T6 可并行
```
