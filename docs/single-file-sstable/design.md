# Design: 单文件 SSTable 格式

## 文件格式

```
sst_{seq}.bin:

┌──────────────────────────────────────────────────────────────┐
│ HEADER (48 bytes)                                            │
│  Magic:             [8]byte  "TSERSTBL"                      │
│  Version:           uint32   (1)                             │
│  RowCount:          uint32   总行数                           │
│  FieldCount:        uint16   字段数                           │
│  BlockCount:        uint16   数据块数                         │
│  BlockSize:         uint16   数据块大小（默认 64KB）           │
│  TimestampsOffset:  uint64   时间戳段在文件中的偏移量           │
│  SidsOffset:        uint64   SID段在文件中的偏移量             │
│  BlockIndexOffset:  uint64   块索引起始偏移量                  │
│  SectionTableOffset:uint64   Section Table 起始偏移量          │
│  Reserved:          [2]byte                                  │
├──────────────────────────────────────────────────────────────┤
│ TIMESTAMPS SECTION                                           │
│  每行 8 字节 (int64 BigEndian)，按 64KB 块写入                 │
│  格式与当前 _timestamps.bin 完全一致                           │
├──────────────────────────────────────────────────────────────┤
│ SIDS SECTION                                                 │
│  每行 8 字节 (uint64 BigEndian)，与 timestamps 一一对应        │
│  格式与当前 _sids.bin 完全一致                                 │
├──────────────────────────────────────────────────────────────┤
│ FIELD SECTIONS (按字段名字典序排列)                             │
│  ┌─ field_bool_1    section ─┐                               │
│  │ 每行 1 字节 (0/1)          │                               │
│  ├─ field_float_1   section ─┤                               │
│  │ 每行 8 字节 (float64)      │                               │
│  ├─ field_float_2   section ─┤                               │
│  │ ...                       │                               │
│  ├─ field_string_1  section ─┤                               │
│  │ 每行 [4B len][data]        │                               │
│  └─ ...                      ┘                               │
│  格式与当前 fields/X.bin 完全一致，按列连续排列                  │
├──────────────────────────────────────────────────────────────┤
│ BLOCK INDEX SECTION                                          │
│  与当前 _index.bin 格式完全一致                                │
│  [magic:8 "TSIDX001"][version:4][count:4][entries:24×N]     │
├──────────────────────────────────────────────────────────────┤
│ SECTION TABLE (文件末尾)                                      │
│  SectionCount: uint16 (固定为 3 + FieldCount)                │
│  For each section:                                           │
│    SectionType: uint8                                        │
│      0=_timestamps, 1=_sids, 2=_index, 3=field              │
│    NameLen:     uint8   (type==3 时为字段名长度, 否则为0)      │
│    Name:        [NameLen]byte                                │
│    Offset:      uint64   该段在文件中的起始字节偏移             │
│    Size:        uint64   该段的字节长度                        │
│  每个 entry 固定 18 字节（不含变长 name）                       │
└──────────────────────────────────────────────────────────────┘
```

## 写入流程

```
NewWriter(shardDir, seq) 
  → 创建 sst_{seq}.bin.tmp 临时文件
  → 写入空 header (48B 占位)
  → 记录各段起始偏移量

WritePoints(points)
  → 为每个字段名创建 buffer
  → 按 64KB block 填充
  → flushBlock() 时:
      timestamps → 写入文件（当前 bufPos 范围）
      sids       → 写入文件（sidBuf 内容）
      每个 field → 写入文件（fieldBuf 内容）
      → 更新 blockIndex

Close()
  → 刷新最后一个 block
  → 记录 BlockIndexOffset
  → 写入 block index (当前 _index.bin 格式)
  → 记录 SectionTableOffset
  → 写入 section table
  → 回 seek 到 offset 0，写入完整 header
  → 关闭文件
  → os.Rename(sst_{seq}.bin.tmp → sst_{seq}.bin)
```

## 读取流程

```
NewReader(filePath, schema)
  → fd = os.Open(filePath)
  → pread(fd, 48) → header
  → pread(fd, SectionTableOffset, ...) → sectionTable
  → pread(fd, BlockIndexOffset, BlockIndexSize) → blockIndex
  → 缓存 fd 用于后续 pread

ReadRange(startTime, endTime)
  → FindBlock(startTime) → 定位起始 block
  → 遍历匹配的 blocks:
      pread(fd, TimestampsOffset + block.Offset, block.RowCount * 8)
      pread(fd, SidsOffset + block.Offset, block.RowCount * 8)
      对每个需要的字段:
        pread(fd, FieldSection.Offset + block.Offset, block.RowCount * fieldSize)
  → 解码返回

Iterator
  → loadBlock(idx):
      pread(fd, TimestampsOffset + entry.Offset, entry.RowCount*8) → ts
      pread(fd, SidsOffset + entry.Offset, entry.RowCount*8) → sids
      对每个字段:
        pread(fd, fieldOffset + entry.Offset, entry.RowCount*fieldSize) → field buf
```

## API 变更

### Writer
```go
// 之前: NewWriter(shardDir string, seq uint64, blockSize int) (*Writer, error)
// → 创建 shardDir/data/sst_{seq}/ 目录

// 之后: 签名不变
// → 创建 shardDir/data/sst_{seq}.bin 文件
```

### Reader
```go
// 之前: NewReader(dataDir string, schema Schema) (*Reader, error)
// dataDir 是 sst_N 目录路径

// 之后: NewReader(filePath string, schema Schema) (*Reader, error)
// filePath 是 sst_N.bin 文件路径
// 签名不变，只是从目录路径变为文件路径
```

### Writer 内部变更
- 移除 `fields     map[string]*os.File` → 改用单一 `*os.File` + section offsets
- 移除 `timestamp  *os.File`
- 移除 `sids       *os.File`
- 添加 `file       *os.File`
- 添加 `sectionOffsets map[string]uint64` 记录各段偏移

### Reader 内部变更
- 移除 `dataDir string` → 改为 `file *os.File` 
- 添加 `sectionTable SectionTable` 缓存段偏移信息
- `readTimestampRange`, `readSidsRange` 改为使用 `pread(file, offset, size)`

## 影响范围

| 文件 | 变更 | 说明 |
|------|------|------|
| `sstable/writer.go` | 重构 | 单 fd 替代多文件 map |
| `sstable/writer_field.go` | 重构 | writeInternalPoint 追加到单文件 |
| `sstable/writer_close.go` | 重写 | flushBlock + Close 写 header/section table |
| `sstable/reader.go` | 重构 | 从 section table 定位各段 |
| `sstable/reader_blocks.go` | 重构 | pread 替代 Seek+Read |
| `sstable/reader_range.go` | 重构 | pread 替代多文件 open |
| `sstable/iterator.go` | 重构 | dataDir→file path |
| `sstable/iterator_block.go` | 重构 | loadBlock 使用 pread |
| `sstable/iterator_next.go` | 不变 | 解码逻辑不变 |
| `sstable/index.go` | 不变 | BlockIndex 格式不变 |
| `shard/shard_io.go` | 适配 | sstDir→sstFile path |
| `shard/shard_flush.go` | 适配 | sstPath 从 dir 变 file |
| `shard/shard_lifecycle.go` | 适配 | 同上 |
| `shard/shard.go` | 适配 | recoverSSTSeq 识别 .bin 后缀 |
| `compaction/merge.go` | 适配 | 路径后缀 |
| `compaction/level.go` | 适配 | 路径后缀 + os.Rename |
| `compaction/compaction.go` | 适配 | sst_*/ 目录 → sst_*.bin 文件 |
| 全部 `_test.go` | 适配 | 所有 sst_0/ 目录路径 → sst_0.bin |

## 设计决策

### 为什么不用 mmap 作为首次实现？
mmap 需要处理跨平台兼容、大文件映射失败、safety 等问题。先实现 pread 版本，section table 格式与 mmap 兼容（同一套偏移量），后续可无缝升级。

### 为什么 Section Table 放在末尾而不是开头？
写入时无法预知各段的大小（block 写入是流式的）。放在末尾可以在 Close() 时一次性写入完整 section table，然后回填 header 中的偏移量。

### SectionTable 固定 per entry 开销
每个 field entry 约 18 + nameLen 字节。10 字段 + 3 系统段 = 13 entries，section table 约 300 字节，相比文件总大小可忽略。
