# 查询性能优化 — 设计方案

## 1. 架构概述

### 当前数据流（问题）

```
Writer.Close()
  → 读取所有 block 的临时数据
  → 对全部行一次性编码（压缩跨 block 连续）
  → 写入单一连续 section

Reader.ReadRange()
  → BlockIndex 过滤出匹配 block
  → readTimestamps() 解码全部 timestamps → 切片
  → readSids() 解码全部 sids → 切片
  → ReadAllDecodedFieldSections(fields, r.header.RowCount)  ← 全量解码!
  → 从全量结果中按 matchingIndices 取子集
```

### 优化后数据流

```
Writer.Close()
  → 对每个 block 独立编码（编码器在 block 边界重置）
  → 在 section 内部连续存放各 block 的编码数据
  → 记录每 block 在各 section 中的字节偏移到 BlockSectionMap

Reader.ReadRange()
  → BlockIndex 过滤出匹配 block
  → 从 BlockSectionMap 获取匹配 block 在各 section 的字节范围
  → 仅读取并解码匹配 block 对应的字节范围
  → 拼接各 block 解码结果
```

## 2. 文件格式变更（Version 2）

### 2.1 新增 FileHeader 字段

```go
// FileHeader v2 新增字段（复用 reserved 区域）
type FileHeader struct {
    Magic              [8]byte
    Version            uint32   // 2（从 1 升级）
    RowCount           uint32
    FieldCount         uint16
    BlockCount         uint16
    BlockSize          uint16
    _                  uint16
    MinTimestamp       int64    // 新增：文件中最小时间戳
    MaxTimestamp       int64    // 新增：文件中最大时间戳
    TimestampsOffset   uint64
    SidsOffset         uint64
    BlockIndexOffset   uint64
    SectionTableOffset uint64
    BlockMapOffset     uint64   // 新增：BlockSectionMap 的文件偏移
    _                  [8]byte  // reserved (缩减)
}
```

Header 保持 64 字节不变：MinTimestamp(8B) + MaxTimestamp(8B) 占用原 TimestampsOffset(8B) 前的 16B padding 空间，BlockMapOffset 占用 reserved 的前 8B。重新排列后：

```
Offset  Size  Field
0       8     Magic
8       4     Version
12      4     RowCount
16      2     FieldCount
18      2     BlockCount
20      2     BlockSize
22      2     _
24      8     MinTimestamp     ← 原 TimestampsOffset，现提前
32      8     MaxTimestamp     ← 原 SidsOffset，现提前
40      8     TimestampsOffset ← 不变
48      8     SidsOffset       ← 不变
56      8     BlockIndexOffset ← 不变
64      8     SectionTableOffset ← 不变
72      8     BlockMapOffset   ← 新字段，但 HeaderSize 仍 64B...

等等，64B 放不下。需要扩展 header 或调整布局。

重新设计：将原 64B header 重新编排：
Offset  Size  Field
0       8     Magic
8       4     Version
12      4     RowCount
16      2     FieldCount
18      2     BlockCount
20      2     BlockSize
22      2     reserved
24      8     MinTimestamp
32      8     MaxTimestamp
40      8     TimestampsOffset
48      8     SidsOffset
56      8     BlockIndexOffset  ← 原来是 40-48

嗯，当前 header 结构：
0-8:    Magic
8-12:   Version
12-16:  RowCount
16-18:  FieldCount
18-20:  BlockCount
20-22:  BlockSize
22-24:  padding
24-32:  TimestampsOffset
32-40:  SidsOffset
40-48:  BlockIndexOffset
48-56:  SectionTableOffset
56-64:  reserved

总共 64B。要加入 MinTimestamp(8B), MaxTimestamp(8B), BlockMapOffset(8B) = 24B 新数据。

方案 A：扩展 HeaderSize 到 96B（保留 32B 扩展空间）
方案 B：将 MinTimestamp/MaxTimestamp 放到 BlockIndex section 中单独存储
方案 C：将 BlockMapOffset 放到 SectionTable 中

我选择方案 B+C：MinTimestamp/MaxTimestamp 在 BlockIndex 中已有（第一个 entry 的 FirstTimestamp 和最后一个 entry 的 LastTimestamp），不需要在 header 中重复。只需在 SectionTable 中新增一个 `_block_map` entry。

实际上更简单：MinTimestamp = BlockIndex.entries[0].FirstTimestamp, MaxTimestamp = BlockIndex.entries[last].LastTimestamp。Reader 在解析完 BlockIndex 后自然就有这些信息了。所以 R3（文件级过滤）不需要 header 变更，只需在 readSSTableFile 打开 reader 后，从 reader.BlockIndex 获取 min/max 即可。

不对，如果要跳过文件而不打开它，就需要在 FileHeader 中有这些数据。或者我们接受先打开文件（读取 header + section table），只跳过后续的 section 读取。

实际上当前流程已经是 打开文件 → 解析 header → 解析 section table → 解析 block index。如果我们在 header 中有 min/max，可以在解析 header 后就决定是否继续。但当前 header 只有 64B，扩展 header 会比较干净。

简化方案：扩展 HeaderSize 到 80B，新增 MinTimestamp、MaxTimestamp、BlockMapOffset。
实际上我们先不管 header 的文件级跳过，这个作为可选优化。先把 block 级按需解码做好。
```

**决定**：不扩展 FileHeader。文件级过滤在解析 BlockIndex 后进行（已有 min/max 信息）。Header 保留 64B 不变，Version 改为 2。

Wait, actually I realize the simplest approach is:

1. Keep the current section layout (all blocks' encoded data in one contiguous section per field)
2. Have each block's data be independently encoded within the section
3. Add a BlockSectionMap that records byte offsets within each section for each block
4. This is stored as a new section `_block_map` in the SectionTable

So no FileHeader change needed at all (except bumping Version to 2, which is optional since we're in dev).

Let me also consider: does this break the concept of "single file"? No, everything is still in one .bin file.

OK let me rewrite the design more carefully.

Actually, I realize I'm overcomplicating this. Let me simplify the design document.

## Key decisions:

1. **Encoding reset at block boundaries**: Each block's data is encoded independently. The temporary files already have per-block raw data, so in `Close()`, instead of reading all blocks and encoding together, we encode each block's raw data separately and concatenate.

2. **BlockSectionMap**: A new section `_block_map` that maps block index → per-section byte offset. Format: array of records, one per section, each containing the section name and an array of uint64 byte offsets (block_count + 1 for sentinel).

Wait, actually the encoding pipeline currently works by:
1. flushBlock writes raw timestamps/sids/fields to temp files
2. Close reads the FULL temp file and encodes everything

To make per-block encoding work, I need to either:
a. Have each block's data in separate temp files
b. Or track byte offsets within the temp files

Actually, looking at the code more carefully: `flushBlock` appends to temp files. So the temp files contain all blocks concatenated in raw format. Then `encodeTimestampsSection` reads the entire temp file and encodes all at once.

The simplest change: Instead of reading the entire temp file, read it block by block. But we need to know where each block starts/ends in the temp file.

Hmm, raw data is fixed-size though:
- Timestamps: 8 bytes per row
- SIDs: 8 bytes per row
- Fields: variable (depends on type)

For fixed-size data (timestamps, SIDs, float64, int64, bool), we can calculate block boundaries easily.
For variable-size data (strings), we can't.

Cleanest approach: For each block flush, write to **separate** temp files, or better, track byte offsets within temp files.

Actually, even simpler: Instead of using temp files at all, accumulate encoded bytes directly at flushBlock time. But that's a bigger refactor.

Let me think about this differently. The SIMPLEST possible change is:

**In Close(), loop over blocks instead of encoding everything at once:**

```go
func (w *Writer) encodeTimestampsSectionPerBlock(rowCount int) ([]byte, []uint64, EncodingType, error) {
    raw, _ := os.ReadFile(tempPath)
    values := compression.ExtractInt64Data(raw, rowCount)
    
    var encoded []byte
    var offsets []uint64
    offset := uint64(0)
    
    for blockIdx := 0; blockIdx < w.blockIndex.Len(); blockIdx++ {
        entry := w.blockIndex.Entry(blockIdx)
        startRow := int(entry.Offset)
        endRow := startRow + int(entry.RowCount)
        blockValues := values[startRow:endRow]
        blockEncoded := compression.EncodeTimestamps(blockValues) // fresh encode
        encoded = append(encoded, blockEncoded...)
        offsets = append(offsets, offset)
        offset += uint64(len(blockEncoded))
    }
    offsets = append(offsets, offset) // sentinel
    return encoded, offsets, EncodingDeltaVarint, nil
}
```

This approach:
- Reads all raw data (same as before)
- Splits into per-block chunks
- Encodes each chunk independently
- Tracks byte offsets
- Concatenates

The raw data read is the same cost as before. The encoding is the same total amount of work. The only change is that the encoder resets at block boundaries.

For the reader side, the offset array allows seeking within the section.

This is clean and minimal. Let me write the design document properly now.<｜end▁of▁thinking｜>

<｜｜DSML｜｜tool_calls>
<｜｜DSML｜｜invoke name="Write">
<｜｜DSML｜｜parameter name="content" string="true"># 查询性能优化 — 设计方案

## 1. 核心思路

将"全量解码后切片"改为"按 block 按需解码"。

**现状**：section 内全量数据一次性编码，Reader 必须全量解码。
**改进**：每个 block 独立编码，字节偏移记录在 BlockSectionMap 中。Reader 找到匹配 block 后，只读取和解码对应字节范围。

## 2. 文件格式变更

### 2.1 FileHeader（64B，不变）

仅 `Version` 从 1 → 2。

### 2.2 新增 Section: `_block_map`

类型：`SectionIndex`（复用），Name：`_block_map`，Encoding：`EncodingRaw`。

格式（二进制）：
```
[section_count:2B]
For each section:
  [name_len:1B][name:variable]       // section 名称（"_timestamps", "_sids", "field_a"...）
  [block_count:4B]                    // block 数量
  [offsets: (block_count+1) * 8B]    // 每个 block 在该 section 内的字节偏移 + sentinel
```

每个 section 的 offsets 数组包含 block_count+1 个 uint64：前 block_count 个是各 block 的起始字节偏移，最后一个 sentinel 是该 section 的总字节大小（方便计算最后一个 block 的大小）。

### 2.3 编码变更

`Close()` 中，`encodeTimestampsSection` / `encodeSidsSection` / `encodeFieldSection` 改为按 block 独立编码：

```
旧：读取全部原始数据 → 一次性编码 → 写入 section
新：读取全部原始数据 → 按 block 行范围分割 → 每 block 独立编码 → 拼接写入 → 记录偏移
```

编码器在 block 边界重置状态（delta 基准值、XOR 前值等），使每个 block 的数据可独立解码。

### 2.4 最终文件布局

```
[FileHeader 64B]
[_timestamps section]  ← 各 block 的独立编码拼接
[_sids section]        ← 同上
[field_a section]      ← 同上
[field_b section]      ← 同上
...
[_index section]       ← BlockIndex（不变，仍用于时间过滤）
[_block_map section]   ← 新增：每个 section 的 per-block 字节偏移
[SectionTable]         ← 包含 _block_map entry
```

## 3. Writer 变更

### 3.1 `writer_close.go` — `Close()`

新增步骤 3.5：在编码各 section 时，收集 per-block 偏移信息。新增步骤 5.5：序列化 BlockSectionMap。

修改 `encodeTimestampsSection` → `encodeTimestampsSectionV2(rowCount) (data, offsets, encoding, error)`
修改 `encodeFieldSection` → `encodeFieldSectionV2(name, rowCount) (data, offsets, encoding, error)`
修改 `encodeSidsSection` → 同上模式。

### 3.2 `writer_close.go` — 新增 `BlockSectionMap`

```go
type BlockSectionMap struct {
    Sections []BlockSectionOffsets
}

type BlockSectionOffsets struct {
    Name    string
    Offsets []uint64  // len = blockCount + 1 (sentinel)
}
```

序列化/反序列化方法写入 `_block_map` section。

## 4. Reader 变更

### 4.1 `reader_range.go` — `readRangeOptimized()`

核心改动：

```go
// 旧代码
rowCount := int(r.header.RowCount)
decodedFields, err := r.ReadAllDecodedFieldSections(fields, rowCount)
// ...从全量结果中按 matchingIndices 取子集

// 新代码
blockRanges := r.blockSectionMap.ReadRanges(matchingBlocks, fieldNames)
decodedFields, err := r.ReadFieldSectionsForBlocks(fields, blockRanges)
// ...每个 block 独立解码后拼接
```

### 4.2 `reader_blocks.go` — 新增按字节范围解码方法

```go
// readTimestampsBlock 只解码指定字节范围的 timestamps
func (r *Reader) readTimestampsBlock(offset, size uint64, rowCount int) ([]int64, error)

// readSidsBlock 只解码指定字节范围的 sids
func (r *Reader) readSidsBlock(offset, size uint64, rowCount int) ([]uint64, error)

// decodeFieldSectionBlock 只解码指定字节范围的字段数据
func (r *Reader) decodeFieldSectionBlock(name string, offset, size uint64, rowCount int) ([]*types.FieldValue, error)
```

### 4.3 `reader_range.go` — 新 `readRangeOptimized` 流程

```
1. BlockIndex.FindBlock(startTime) → 找到候选 block
2. 遍历 block index，收集与 [start, end) 有重叠的 block 索引列表
3. 对每个匹配 block：
   a. 从 BlockSectionMap 获取该 block 在各 section 的字节范围
   b. readTimestampsBlock → 局部解码 timestamps → 过滤匹配行
   c. readSidsBlock → 局部解码
   d. 对每个 field，decodeFieldSectionBlock → 局部解码
   e. 组装 PointRow
4. 汇总返回
```

### 4.4 `reader.go` — 加载 BlockSectionMap

在 `NewReader` 或首次使用时，从 `_block_map` section 解析 `BlockSectionMap`。

## 5. Shard 层优化（可选）

### 5.1 文件级时间过滤

`readSSTableFile` 在解析 header+blockIndex 后，若 `[fileMinTime, fileMaxTime)` 与 `[startTime, endTime)` 无交集，跳过该文件。

BlockIndex 中 entries[0].FirstTimestamp 和 entries[last].LastTimestamp 提供了文件级 min/max。

## 6. 压缩率影响分析

Block 大小默认 64KB，每个 block 包含约 2000-4000 行（取决于字段数量和数据大小）。

| 编码 | 跨 block 连续编码优势 | 独立 block 编码影响 |
|------|---------------------|-------------------|
| Delta-of-Delta (timestamps) | block 间 delta 可能更大 | 影响微小（block 边界多存一个基准值和 delta 基准） |
| Varint (SIDs) | 无（无差值编码） | 无影响 |
| XOR Float | block 间前值差异可能被压缩 | 每 block 首个值需存完整 8B |
| ZigZag Varint | 无（无差值编码） | 无影响 |
| Dict String | 更大的字典表 | 小字典编码效率略低，但 64KB block 足够大 |
| Bitmap Bool | 无 | 无影响 |

预计总压缩率退化 <3%（主要是每 block 的基准值开销）。

## 7. 兼容性

- 文件 Version 升级到 2，v1 文件通过 Version 字段识别
- 开发阶段不需要 v1→v2 迁移，直接使用新格式
- 对外 API 无变化，E2E 测试无需修改
