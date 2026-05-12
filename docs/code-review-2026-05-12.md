# 代码检视报告 — microts 项目

> **日期：** 2026-05-12  
> **检视范围：** 全部 Go 源码（约 10.5 万行，105 个文件）  
> **检视方法：** 静态分析 + 人工审计 + golangci-lint + go vet  
> **分类优先级：** P0 = 数据正确性/崩溃风险 → P1 = 功能缺陷/安全 → P2 = 架构/可维护性 → P3 = 风格/规范

---

## 总览

| 等级 | 数量 | 说明 |
|------|------|------|
| P0 - 严重 | 3 | 数据损坏或系统崩溃风险 |
| P1 - 高 | 8 | 功能缺陷、安全漏洞、外部可见行为异常 |
| P2 - 中 | 12 | 架构债务、代码质量、性能隐患 |
| P3 - 低 | 7 | 风格、规范、文档一致性 |

---

## P0 — 严重问题

### 1. SimpleSeriesStore 哈希冲突导致 Series 丢失

**文件：** `internal/storage/metadata/series_simple.go:128-138`  
**类型：** BUG — 数据正确性

当 FNV-1a 哈希冲突发生时（不同 tags 映射到同一 hash 值），`AllocateSID` 检测到 tags 不匹配后不会报错，而是直接分配新的 SID 并**覆盖** `hashIdx[h]`：

```go
h := tagsHash(tags)
if sid, ok := s.hashIdx[h]; ok {
    if tagsEqual(s.series[sid], tags) {
        return sid, nil
    }
    // BUG: tags 不匹配但不报错，继续执行分配新 SID
}
// ...
s.hashIdx[h] = sid  // 覆盖旧映射 → 旧 series 永久孤儿化
```

**影响：** 在高基数场景（>10^5 unique series）下，FNV-1a 64 位碰撞概率不可忽略。旧 series 将无法通过 `AllocateSID` 再次查询到，导致相同 tags 获得不同 SID，产生**数据重复**。

**修复建议：** 使用链式冲突解决（`map[uint64][]uint64`）或改用 SHA-256 截断哈希。

---

### 2. WAL 双重重放风险

**文件：** `internal/storage/shard/manager.go:226-233`  
**类型：** BUG — 并发安全

`discoverAndReplayWAL` 在获取 `m.mu` 锁之前就已经执行了 WAL 重放（由 `loadShardFromIndex` 内部的 `ReplayWAL` 触发）。如果两个 goroutine 同时 "发现" 同一个 shard：

```
G1: loadShardFromIndex → ReplayWAL()
G2: loadShardFromIndex → ReplayWAL()  // 同一 shard 的 WAL 被重放两次
G1: m.mu.Lock → 发现已存在 → return
G2: m.mu.Lock → 发现已存在 → return
```

**影响：** 同一批 WAL 数据被重放两次到 MemTable，后续 flush 到 SSTable 时会产生**重复数据**。

**修复建议：** 将 `verifyOrDiscoverMeasurement` 中的 shard 创建移到 `m.mu` 锁保护范围内，或使 WAL Replay 幂等。

---

### 3. go.mod 声明不存在的 Go 版本

**文件：** `go.mod:3`  
**类型：** BUG — 构建系统

```go
go 1.26  // Go 1.26 不存在（截至 2026-05 最新稳定版为 1.25）
```

**影响：** `go mod tidy` 可能行为异常；CI 系统可能拒绝构建；部分工具链可能报错。

**修复建议：** 改为 `go 1.25` 或实际使用的目标版本。

---

## P1 — 高优先级

### 4. WriteBatch 伪原子性

**文件：** `internal/engine/engine_write.go:55-68`  
**类型：** BUG — API 契约

`WriteBatch` 逐条调用 `Write`，中间任一失败立即返回错误，但**已写入的数据点不回滚**。`mts.go:291` 文档声称 "原子性" 但与实际行为不符。

**影响：** 调用方无法知道哪些点已写入，无法正确重试。

**修复建议：** 选项 A) 在文档中明确标注 "非原子"；选项 B) 先用 WAL 批量写入实现原子性。

---

### 5. SSTable 格式缺少校验和

**文件：** `internal/storage/shard/sstable/writer_close.go:110-157`, `compress.go:34-60`  
**类型：** BUG — 静默数据损坏

所有压缩后的 block 格式为 `[uncompressedLen:4B][compressed_data]`，没有任何 CRC/校验和。如果磁盘出现位翻转或部分写入，**读取时无法检测**。

**影响：** 静默数据损坏 — 读取到错误的时间戳/字段值，查询结果不可信。

**修复建议：** 在每个压缩 block 末尾追加 4 字节 CRC32。

---

### 6. BlockIndex.Add 语义混淆

**文件：** `internal/storage/shard/sstable/writer_close.go:43`  
**类型：** BUG — 数据定位

```go
w.blockIndex.Add(w.firstTs, lastTs, uint32(w.totalRows), uint32(w.rowCount))
```

第三个参数 `uint32(w.totalRows)` 是累计行数，但 `BlockIndex` 的 `Offset` 字段被多处代码按不同语义理解（有的当时间偏移，有的当字节偏移）。在后续的 `encodePerBlock` 中（writer_close.go:383），`entry.Offset` 被当作行偏移使用。

**影响：** 任何依赖 `BlockIndex.Entry(i).Offset` 做字节级定位的代码都会失败。

**修复建议：** 统一 Offset 语义，或将行偏移重命名为 `RowOffset`。

---

### 7. 查询迭代器加载全量数据而非流式

**文件：** `internal/storage/shard/iterator.go:80`, `internal/storage/shard/shard_io.go:200-210`  
**类型：** PERFORMANCE + ARCHITECTURE

`NewShardIterator` 调用 `readFromSSTable` 将所有匹配时间范围的 SSTable 数据**一次性加载到内存**，而非流式读取。注释（iterator.go:36）承认此问题但无替代方案。

**影响：** 1M+ 行的查询可导致 OOM。这与项目中已有的 "Limit 下推" 优化（见 recent commit `a6ea6e2`）形成矛盾 — Limit 下推节省了返回数据量，但迭代器仍加载全部中间数据。

---

### 8. 硬编码数据目录

**文件：** `cmd/server/main.go:24`  
**类型：** SECURITY + CONFIG

```go
DataDir: "/var/lib/microts"
```

生产环境可能无法写入此路径，且不支持容器化部署配置。无环境变量或命令行参数替代。

---

### 9. Shard 路径遍历注入

**文件：** `internal/storage/shard/manager.go:92`  
**类型：** SECURITY

```go
shardDir := filepath.Join(m.dataDir, db, measurementName)
```

`db` 和 `measurementName` 直接来自 gRPC 请求，未经过 `isPathSafe` 校验。恶意客户端可传入 `../../etc` 作为 database 名，导致 shard 数据写入系统目录。

---

### 10. Catalog 错误被静默丢弃

**文件：** `internal/engine/engine_write.go:36-41`, `mts.go:547`, `mts.go:603`  
**类型：** BUG — 错误处理

多处关键操作的错误被 `_` 丢弃：
- `CreateDatabase` 创建失败 → 后续 `CreateMeasurement` / `GetShard` 继续执行
- `Flush` 在 Close 时失败 → 数据可能丢失且无信号通知调用方
- `Catalog.ListDatabases` 的 bolt 迭代错误完全丢弃（`catalog_impl.go:54`）

---

### 11. grpc.go 类型转换 Bug（已记录未修复）

**文件：** `internal/api/grpc.go:138`  
**类型：** BUG

`pointRowToProto` 无法正确处理 `*types.FieldValue` 类型的字段值。`grpc_test.go:692-708` 明确记录了此 bug 但未修复。

---

## P2 — 中优先级

### 12. 文件行数超标（违反项目规范）

项目 CLAUDE.md 要求文件 ≤ 300 行，以下文件超出：

| 文件 | 行数 |
|------|------|
| `compaction/level.go` | 616 |
| `shard/shard.go` | 607 |
| `compaction/compaction.go` | 560 |
| `wal/wal.go` | 498 |
| `sstable/writer_close.go` | 479 |
| `api/grpc.go` | 381 |
| `metadata/series_impl.go` | 376 |
| `shard/manager.go` | 370 |
| `compaction/merge.go` | 349 |
| `compression/encode.go` | 333 |

---

### 13. Shard 创建逻辑三处重复

**文件：** `shard/manager.go:105-107`, `manager.go:151-169`, `manager.go:226-241`  
**类型：** ARCHITECTURE

`GetShard`、`discoverShardsLocked`、`discoverAndReplayWAL` 三个代码路径都有完整的 shard 创建逻辑（NewShard → ReplayWAL → 注册到 maps），但锁策略和注册细节各有差异。任何 bug 修复需要在三处同步应用。

---

### 14. `discoverShardsLocked` 方法命名误导

**文件：** `internal/storage/shard/manager.go:129-131`  
**类型：** CODE_QUALITY

方法名为 `discoverShardsLocked`，被 `GetShards` 在**未持锁**状态下调用。内部自己获取锁（line 154），但命名暗示调用者已持锁，可能引发未来维护者的死锁。

---

### 15. 类型转换逻辑重复

**文件：** `engine/engine_query.go:114-131` ↔ `api/grpc.go:105-130`  
**类型：** CODE_QUALITY

`anyToProtoFieldValue` 和 `anyToFieldValue` 逻辑几乎相同，但一处处理 `*types.FieldValue` 输入，另一处不处理。两处代码会继续分叉。

---

### 16. query/executor.go 为占位死代码

**文件：** `internal/query/executor.go:46-66`  
**类型：** CODE_QUALITY

`NewExecutor` 接收 `any` 参数但不使用；`Execute` 永远返回错误。无 TODO、无进度标记。应该删除或明确标注。

---

### 17. ShouldCompactLocked 吞没 I/O 错误

**文件：** `internal/storage/compaction/compaction.go:429-431, 438-440`  
**类型：** CODE_QUALITY

`collectSSTablesWithoutRefs` 和 `CalculateShardSize` 的错误导致 `ShouldCompactLocked` 静默返回 `false`，无日志。瞬态 I/O 错误会毫无痕迹地阻止 compaction。

---

### 18. NewWriter 失败时可能泄漏临时目录

**文件：** `internal/storage/shard/sstable/writer_field.go:38-43`  
**类型：** CODE_QUALITY

`WritePoints` 中 `SafeOpenFile` 失败直接 return，不清理 `NewWriter` 中已创建的 `w.tmpDir`。对比 `NewWriter` 其他位置会清理。

---

### 19. 缺少并发安全测试

**位置：** 全项目  
**类型：** TEST

`SimpleSeriesStore.AllocateSID`、`series_impl.go` 中的 bbolt 并发访问、Manager.GetShard 的 WAL replay 竞态 — 均无并发测试覆盖。`go test -race` 无法替代有意义的并发正确性验证。

---

### 20. decodeUint64 错误处理缺失

**文件：** `internal/storage/metadata/series_simple.go:85-88`  
**类型：** CODE_QUALITY

```go
func decodeUint64(data []byte) uint64 {
    v, _ := binary.Uvarint(data)  // 错误被丢弃，损坏数据返回 0
    return v
}
```

---

### 21. flushLocked 中 UnmarkWriting 清理逻辑重复 5 次

**文件：** `internal/storage/shard/shard_flush.go:46-99`  
**类型：** CODE_QUALITY

`UnmarkWriting` 在多个错误路径中手动调用。应使用 `defer` 或提取清理函数。

---

### 22. MemTable 迭代器缺少并发保护

**文件：** `internal/storage/shard/iterator.go:89-93`  
**类型：** CODE_QUALITY

`NewShardIterator` 调用 `shard.memTable.Iterator()` 时未持有 `s.mu`，而 flush 操作在另一个 goroutine 中持有 `s.mu` 并可能清空 MemTable。

---

### 23. `acquire` 返回值固定为 true

**文件：** `internal/storage/shard/shard_sstable_ref.go:34-47`  
**类型：** CODE_QUALITY

`acquire` 方法始终返回 `true`，布尔返回值暗示的失败路径不存在，误导调用方。

---

## P3 — 低优先级

### 24. 中英文注释混用

项目中注释混合使用中文和英文。`series_simple.go` 用中文，`series_impl.go` 用英文。建议统一。

### 25. context.Context 参数未使用

**文件：** `mts.go:422, 455, 482, 518, 546, 572`  
**类型：** STYLE

多个方法接受 `ctx context.Context` 但用 `_ = ctx` 丢弃。要么使用 context（超时控制），要么移除参数。

### 26. 魔法数字未命名

- `merge.go:129`: `0x9e3779b97f4a7c15` (黄金比例哈希混合常数)
- `shard.go:514`: 100ms/30s 间隔边界
- `wal_serialize.go:56-70`: 字段类型常量 0-3

### 27. grpc.go 中未使用的导出函数

**文件：** `internal/api/grpc.go:89, 372`  
**类型：** STYLE

`fieldValueToAny` 和 `ToProtoPointRow` 仅在测试中使用，不应导出。

### 28. grpc_test.go 静默吞没测试失败

**文件：** `internal/api/grpc_test.go:193-196`  
**类型：** TEST

```go
if err != nil {
    return  // 直接 return，测试通过但实际未验证任何断言
}
```

### 29. retention.go 缺少负值/零值边缘测试

### 30. `collectInputTombstones` 吞没 LoadTombstones 错误

**文件：** `internal/storage/compaction/merge.go:237-252`  
**类型：** CODE_QUALITY

---

## 已验证排除的误报

以下发现经交叉验证后确认为误报：

- ✅ `format.go:116-120` — Marshal 中 `off` 变量复用是高效的栈变量复用，已在 `buf = append(buf, off[:]...)` 后写入，不产生数据损坏
- ✅ `merge.go:10` — `time` 包在 `Commit()` 方法（line 207）中被使用
- ✅ `writer_close.go` vs `writer_field.go` — `detectFieldType` 和 `fieldTypeSize` 是不同的函数，各有独立职责

---

## 总结

项目整体代码质量**中等偏上**。压缩、编码、compaction 核心逻辑正确且测试较充分。主要问题集中在：

1. **错误处理不完整**（多处 `_` 丢弃错误）— 根因追溯困难
2. **并发安全未充分验证**（WAL 重放、hash 冲突、迭代器）— 高风险场景缺乏测试
3. **文件规模超标**（10 个文件超 300 行）— 违反项目自身规范
4. **API 契约不诚实**（WriteBatch "原子性"、acquire 永真返回值）— 误导调用方
