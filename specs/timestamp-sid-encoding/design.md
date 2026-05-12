# Design: SID Delta 编码优化

## 编码格式

```
原始 SID 序列: [sid0, sid1, sid2, sid3, ...]

编码后:
  [sid0: varint]           // 第一个 SID 原始值
  [sid1-sid0: varint]      // 差值
  [sid2-sid1: varint]      // 差值
  [sid3-sid2: varint]      // 差值
  ...
```

## 编码示例

SID 序列：`[1000000, 1000100, 1000200, 1000300]`

**当前方案**:
```
[varint(1000000), varint(1000100), varint(1000200), varint(1000300)]
≈ [3B, 3B, 3B, 3B] = 12 字节
```

**优化方案**:
```
[varint(1000000), varint(100), varint(100), varint(100)]
≈ [3B, 1B, 1B, 1B] = 6 字节
```

**收益: 50% 空间节省**

## 为什么不需要回退判断

MTS 的 SID 分配机制保证：
- SID 基于 measurement 级别
- 按时间线顺序递增分配
- 不同 tag 对应不同 sid

这意味着在 SSTable 中，**SID 序列几乎是确定性的递增序列**，`delta = sid[i] - sid[i-1]` 通常等于 1 或很小。

因此：**直接使用 Delta 编码，无需回退判断**

## 文件修改

### 1. compression/encode.go

新增函数：

```go
// EncodeSidsDelta 对 SID 序列进行 First-Delta 编码。
// 第一个 SID 存储原始值，后续存储与前一个 SID 的差值。
func EncodeSidsDelta(values []uint64) []byte {
    if len(values) == 0 {
        return nil
    }
    buf := make([]byte, 0, len(values)*4)
    var tmp [10]byte

    // 第一个 SID 原始值
    n := PutVarint(tmp[:], values[0])
    buf = append(buf, tmp[:n]...)

    // 后续 SID 的差值
    for i := 1; i < len(values); i++ {
        delta := values[i] - values[i-1]
        n = PutVarint(tmp[:], delta)
        buf = append(buf, tmp[:n]...)
    }
    return buf
}

// DecodeSidsDelta 解码 First-Delta 编码的 SID。
func DecodeSidsDelta(data []byte, count int) ([]uint64, error) {
    if count == 0 {
        return nil, nil
    }
    if len(data) == 0 {
        return nil, fmt.Errorf("decode sids delta: empty data")
    }

    values := make([]uint64, count)
    pos := 0

    // 第一个 SID
    v, n := Varint(data[pos:])
    pos += n
    values[0] = v

    // 后续 SID 通过差值计算
    for i := 1; i < count; i++ {
        if pos >= len(data) {
            return nil, fmt.Errorf("decode sids delta: truncated at %d", i)
        }
        delta, n := Varint(data[pos:])
        pos += n
        values[i] = values[i-1] + delta
    }
    return values, nil
}
```

### 2. sstable/writer_close.go

修改 `encodeSidsSection` 函数：

```go
// 修改前
func (w *Writer) encodeSidsSection(rowCount int) ([]byte, []uint64, error) {
    raw, err := os.ReadFile(filepath.Join(w.tmpDir, "_sids.bin"))
    values := compression.ExtractUint64Data(raw, rowCount)
    data, offsets := encodePerBlock(w, values, func(vals []uint64) []byte {
        return compression.EncodeSids(vals)  // 直接 Varint
    })
    return data, offsets, EncodingVarint
}

// 修改后
func (w *Writer) encodeSidsSection(rowCount int) ([]byte, []uint64, error) {
    raw, err := os.ReadFile(filepath.Join(w.tmpDir, "_sids.bin"))
    values := compression.ExtractUint64Data(raw, rowCount)
    data, offsets := encodePerBlock(w, values, func(vals []uint64) []byte {
        return compression.EncodeSidsDelta(vals)  // First-Delta
    })
    return data, offsets, EncodingVarint  // encoding 类型不变，编解码器内部统一
}
```

### 3. sstable/reader_blocks.go

修改 SID 解码路径：

```go
// 修改 readSids 函数
func (r *Reader) readSids(expectedCount int) ([]uint64, error) {
    sidOffset, sidSize, _ := r.sectionTable.LookupByType(SectionSids)
    if sidSize == 0 {
        return make([]uint64, expectedCount), nil
    }
    data := make([]byte, sidSize)
    if _, err := r.file.ReadAt(data, int64(sidOffset)); err != nil {
        return nil, err
    }
    return compression.DecodeSidsDelta(data, expectedCount)
}

// 修改 readSidsBlock 函数
func (r *Reader) readSidsBlock(blockIdx int) ([]uint64, error) {
    bso := r.blockSectionMap.Lookup("_sids")
    if bso == nil {
        return nil, fmt.Errorf("no block map entry for _sids")
    }
    offset, size := bso.BlockRange(blockIdx)
    if size == 0 {
        entry := r.blockIndex.Entry(blockIdx)
        return make([]uint64, entry.RowCount), nil
    }

    entry := r.blockIndex.Entry(blockIdx)
    rowCount := int(entry.RowCount)

    sidsOffset, _, _ := r.sectionTable.LookupByType(SectionSids)
    data := make([]byte, size)
    if _, err := r.file.ReadAt(data, int64(sidsOffset+offset)); err != nil {
        return nil, err
    }
    return compression.DecodeSidsDelta(data, rowCount)
}
```

## 性能分析

| 场景 | 当前 Varint | Delta | 节省 |
|------|------------|-------|------|
| SID 递增 1 | ~3B/值 | ~1B/值 | 66% |
| SID 递增 100 | ~3B/值 | ~1-2B/值 | 50% |
| SID 随机大值 | ~3B/值 | ~3-4B/值 | 0% |

**平均收益**: 40-60%

## 实现顺序

1. `compression/encode.go`: 添加 `EncodeSidsDelta` 和 `DecodeSidsDelta`
2. `sstable/writer_close.go`: 修改 `encodeSidsSection`
3. `sstable/reader_blocks.go`: 修改 `readSids` 和 `readSidsBlock`
4. 添加测试验证
