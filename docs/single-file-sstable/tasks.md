# Tasks: 单文件 SSTable 格式

## T1: 定义文件格式常量和 SectionTable 类型

**目标**: 在 `sstable/` 包中新增 `format.go`，定义单文件格式的魔数、版本、header 结构和 SectionTable 类型。

**验收**:
- [ ] `format.go` 编译通过
- [ ] Header 结构体大小 ≤ 48 字节
- [ ] SectionTable 支持序列化/反序列化
- [ ] 单元测试覆盖 Header/ SectionTable 的序列化往返

**文件**: `internal/storage/shard/sstable/format.go` (新)

---

## T2: 重构 Writer → 单文件输出

**目标**: 修改 `writer.go`, `writer_field.go`, `writer_close.go`，将多文件写入改为单文件。

**验收**:
- [ ] `NewWriter` 创建 `sst_{seq}.bin.tmp` 单文件，不再创建目录
- [ ] `WritePoints` 行为不变（buffer → flushBlock 追加到单文件）
- [ ] `flushBlock` 将 timestamps/sids/fields 追加到同一文件的不同段
- [ ] `Close` 写入 section table，回填 header，rename tmp→bin
- [ ] 生成的 `.bin` 文件大小与之前多文件总大小一致
- [ ] 现有 `writer_test.go` 测试通过（路径适配后）

**文件**: `writer.go`, `writer_field.go`, `writer_close.go`

---

## T3: 重构 Reader → 从单文件读取

**目标**: 修改 `reader.go`, `reader_blocks.go`, `reader_range.go`，从单文件读取。

**验收**:
- [ ] `NewReader(filePath)` 打开单文件，解析 header 和 section table
- [ ] `readTimestampRange` 使用 `pread(file, tsOffset+blockOffset, size)` 替代 Seek+Read
- [ ] `readSidsRange` 使用 `pread(file, sidOffset+blockOffset, size)`
- [ ] `ReadAll` 从 section table 获取字段名列表（不再 readdir）
- [ ] `ReadRange` 正确按需读取字段数据
- [ ] `Close()` 关闭 fd
- [ ] `reader_test.go` 测试通过

**文件**: `reader.go`, `reader_blocks.go`, `reader_range.go`

---

## T4: 重构 Iterator → 适配单文件

**目标**: 修改 `iterator.go`, `iterator_block.go`, `iterator_next.go`，从单文件读取。

**验收**:
- [ ] `NewIterator` 使用 Reader 的单文件 fd
- [ ] `loadBlock` 使用 pread 替代逐个打开字段文件
- [ ] `loadAllData` (fallback) 使用 pread
- [ ] `iterator_test.go`, `iterator_extra_test.go` 全部通过

**文件**: `iterator.go`, `iterator_block.go`, `iterator_next.go`

---

## T5: 适配 Shard 层

**目标**: 修改 `shard/` 包中引用 SSTable 路径的代码。

**验收**:
- [ ] `shard_flush.go`: sstPath 从 `data/sst_N/` 改为 `data/sst_N.bin`
- [ ] `shard_io.go`: `readSSTableDir` → `readSSTableFile`, 路径匹配 `.bin` 后缀
- [ ] `shard_lifecycle.go`: Close 时 flush 路径适配
- [ ] `shard.go`: `recoverSSTSeq` 识别 `sst_N.bin` 文件
- [ ] `shard_test.go`, `shard_extra_test.go` 全部通过

**文件**: `shard_flush.go`, `shard_io.go`, `shard_lifecycle.go`, `shard.go`

---

## T6: 适配 Compaction 层

**目标**: 修改 `compaction/` 包中读写 SSTable 的路径。

**验收**:
- [ ] `merge.go`: NewReader/NewWriter path 适配
- [ ] `level.go`: NewReader/NewWriter path 适配, os.Rename(src.bin, dst.bin)
- [ ] `compaction.go`: `CollectSSTables` 识别 `.bin` 文件
- [ ] `.writing` 标记改为 `sst_N.bin.writing`
- [ ] 所有 compaction 测试通过

**文件**: `compaction/merge.go`, `compaction/level.go`, `compaction/compaction.go`

---

## T7: 适配所有测试

**目标**: 更新所有手写 SSTable 文件路径的测试代码。

**验收**:
- [ ] `sstable/writer_test.go`: 从 `sst_0/_timestamps.bin` 改为验证 `sst_0.bin`
- [ ] `sstable/reader_test.go`: NewReader 参数改为文件路径
- [ ] `sstable/sstable_test.go`: 同上
- [ ] `sstable/iterator_test.go`: 同上
- [ ] `sstable/iterator_extra_test.go`: 同上
- [ ] `shard/iterator_test.go`: sst 目录路径改为文件路径
- [ ] `shard/compaction_test.go`: 同上
- [ ] `shard/level_compaction_e2e_test.go`: 同上
- [ ] `shard/level_compaction_test.go`: 同上

---

## T8: 清理与验证

**目标**: 清理遗留代码，运行完整验证。

**验收**:
- [ ] 移除 Writer 中不再需要的 `fields map[string]*os.File`、`timestamp *os.File`、`sids *os.File`
- [ ] 移除 Writer 中 `initFieldFiles()` 方法
- [ ] 移除 `fields/` 子目录创建逻辑
- [ ] `go build ./...` 全量编译通过
- [ ] `go test ./internal/storage/...` 全部通过
- [ ] `golangci-lint run ./internal/...` 0 issues
- [ ] `goimports-reviser` 格式化
- [ ] E2E 测试全部通过：
  - `compaction_test` (8/8)
  - `simple_integrity` (OK)
  - `wal_test` (6/6)
  - `write_1k`, `write_10k` (TPS 无明显退化)
