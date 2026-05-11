# Tasks: 数据类型压缩编码

## 依赖关系

```
Task 1 (BitWriter/BitReader)
  ├── Task 2 (ZigZag)
  ├── Task 3 (XOR Float)
  ├── Task 5 (Bitmap)
  └── Task 4 (Dict String)
        └── Task 6 (EncodingType + 编码入口)
              └── Task 7 (Writer 集成)
                    └── Task 8 (Reader 集成)
                          └── Task 9 (E2E 验证 + 清理)
```

---

## Task 1: 实现 BitWriter / BitReader

**文件**: `internal/storage/shard/compression/bit_io.go`

**内容**:
- `BitWriter` 结构体，支持逐 bit 写入（WriteBit / WriteBits）
- `BitReader` 结构体，支持逐 bit 读取（ReadBit / ReadBits）
- Flush 方法补齐最后一个字节

**验收**:
- 单元测试覆盖所有导出方法，行覆盖率 >= 90%
- 测试边界：写入 0 位、写入 64 位、bit 对齐与不对齐、flush 补齐

---

## Task 2: 实现 ZigZag 编码

**文件**: `internal/storage/shard/compression/zigzag.go`

**内容**:
- `ZigZagEncode(values []int64) []uint64` — 有符号 → 无符号
- `ZigZagDecode(encoded []uint64) []int64` — 无符号 → 有符号

**验收**:
- 单元测试覆盖：正数、负数、零、MaxInt64、MinInt64、空切片
- 行覆盖率 >= 90%

---

## Task 3: 实现 XOR Float 编码

**文件**: `internal/storage/shard/compression/xor_float.go`

**内容**:
- `XORFloatEncode(values []float64) []byte` — 首个值 8B 原始 + 后续 XOR 压缩
- `XORFloatDecode(data []byte, count int) ([]float64, error)` — 解码

**验收**:
- 单元测试覆盖：相同值序列、递增序列、随机值、NaN/Inf、空切片、单值
- 行覆盖率 >= 90%

---

## Task 4: 实现字典编码 (string)

**文件**: `internal/storage/shard/compression/dict_string.go`

**内容**:
- `DictEncode(values []string) []byte` — 字典 + 索引编码
- `DictDecode(data []byte, count int) ([]string, error)`
- `ShouldUseDict(values []string) bool` — 预判编码是否有收益

**验收**:
- 单元测试覆盖：低基数重复字符串、全唯一字符串、空字符串、空切片、回退判断
- 行覆盖率 >= 90%

---

## Task 5: 实现位图编码 (bool)

**文件**: `internal/storage/shard/compression/bitmap.go`

**内容**:
- `BitmapEncode(values []bool) []byte` — ceil(N/8) 字节，MSB first
- `BitmapDecode(data []byte, count int) []bool`

**验收**:
- 单元测试覆盖：全 true、全 false、交替、奇数长度、空切片
- 行覆盖率 >= 90%

---

## Task 6: 新增 EncodingType + 编码调度入口

**文件**:
- `internal/storage/shard/sstable/encoding.go` — EncodingType 定义
- `internal/storage/shard/compression/encode.go` — EncodeSection / DecodeSection 统一入口

**内容**:
- `EncodingType` 类型及常量定义
- `EncodeSection(typ EncodingType, rawData []byte, rowCount int, fieldType FieldType) ([]byte, error)` — 按类型编码
- `DecodeSection(typ EncodingType, data []byte, rowCount int, fieldType FieldType) ([]byte, error)` — 按类型解码，返回原始格式数据

**验收**:
- 单元测试覆盖每种编码类型的编解码往返
- 行覆盖率 >= 90%

---

## Task 7: Writer 集成

**文件**:
- `internal/storage/shard/sstable/format.go` — SectionEntry 增加 Encoding 字段，更新序列化
- `internal/storage/shard/sstable/writer_close.go` — Close() 中编码各段

**内容**:
1. `SectionEntry` 增加 `Encoding EncodingType` 字段
2. `SectionTable.Marshal/Unmarshal` 更新格式 (19B/entry)
3. `Writer.Close()`: 读取临时文件 → 编码 → 写入最终文件 → 记录 encoding 到 SectionEntry
4. 时间戳段 → DeltaVarint，SID 段 → Varint
5. 字段段根据 FieldType 选择编码

**验收**:
- 写入 + 读取往返验证（每种类型字段）
- 现有 SSTable 测试全部通过（需更新测试数据）
- 行覆盖率 >= 90%

---

## Task 8: Reader 集成

**文件**:
- `internal/storage/shard/sstable/reader.go` — SectionEntry 读取 encoding
- `internal/storage/shard/sstable/reader_blocks.go` — 按 encoding 解码各段

**内容**:
1. `readTimestamps`: 根据 encoding 解码（DeltaVarint 或 Raw）
2. `readSids`: 根据 encoding 解码（Varint 或 Raw）
3. 字段读取: 根据 SectionEntry.Encoding 选择解码器
4. `decodeFieldValue` 适配编码后的数据读取

**验收**:
- 与 Writer 的往返测试
- 现有测试全部通过
- 行覆盖率 >= 90%

---

## Task 9: E2E 验证 + 清理 ✅

**验收**:
- ✅ 所有单元测试通过（compression、sstable、api、engine、query、storage、compaction、memtable、metadata、shard、wal、types）
- ✅ E2E 测试通过：simple_integrity, check_fields, check_schema, wal_test (6/6), persistence_test, restart_recovery, compaction_test (8/8), retention_test, grpc_write_query, write_1k, query_1k, write_10k, query_10k, write_100k, query_100k
- ✅ golangci-lint: 0 issues（已删除 3 个 unused 函数：encodingForFieldType, computeOffsets, copyFile）
- ✅ goimports-reviser 格式化完成
- ✅ 修复字符串编码 bug：encodeStringRaw/decodeStringRaw 从 2B 改为 4B 长度前缀，与 reader.go 的 decodeFieldValue 和 writer_field.go 的 appendFieldValue 一致
- 压缩效果（grpc_write_query 10K points）：Compression Ratio 0.25x，Bytes/Point: 316.66（含 WAL + 索引元数据开销）
