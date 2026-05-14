package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"codeberg.org/micro-ts/mts/internal/storage"
	"codeberg.org/micro-ts/mts/internal/storage/shard/compression"
	"codeberg.org/micro-ts/mts/types"
)

// flushBlock 将当前 block 缓冲写入临时文件。
func (w *Writer) flushBlock() error {
	if w.bufPos == 0 && w.rowCount == 0 {
		return nil
	}

	if _, err := w.timestamp.Write(w.buf[:w.bufPos]); err != nil {
		return fmt.Errorf("write timestamp block: %w", err)
	}

	for _, sid := range w.sidBuf {
		var sidBuf [8]byte
		binary.BigEndian.PutUint64(sidBuf[:], sid)
		if _, err := w.sids.Write(sidBuf[:]); err != nil {
			return fmt.Errorf("write sid block: %w", err)
		}
	}
	w.sidBuf = w.sidBuf[:0]

	for name, buf := range w.fieldBufs {
		// 记录此 block 在 temp 文件中的字节起始偏移
		curOff, err := w.fields[name].Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("seek field %s for offset: %w", name, err)
		}
		w.fieldByteOffsets[name] = append(w.fieldByteOffsets[name], curOff)

		if _, err := w.fields[name].Write(buf); err != nil {
			return fmt.Errorf("write field block %s: %w", name, err)
		}
		w.fieldBufs[name] = w.fieldBufs[name][:0]
	}

	lastTs := int64(binary.BigEndian.Uint64(w.buf[w.bufPos-8:]))
	w.blockIndex.Add(w.firstTs, lastTs, uint32(w.totalRows), uint32(w.rowCount))
	w.totalRows += uint32(w.rowCount)

	w.bufPos = 0
	w.rowCount = 0
	w.firstTs = 0

	return nil
}

// Close 关闭 Writer，编码并合并临时文件到单一 .bin 文件。
func (w *Writer) Close() error {
	if err := w.flushBlock(); err != nil {
		return fmt.Errorf("flush block: %w", err)
	}

	// 关闭所有临时文件
	_ = w.timestamp.Close()
	_ = w.sids.Close()
	for _, f := range w.fields {
		_ = f.Close()
	}

	// 获取字段名并按字典序排序
	fieldNames := make([]string, 0, len(w.fields))
	for name := range w.fields {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)

	// 创建最终的单文件
	outPath := filepath.Join(w.dataDir, fmt.Sprintf("sst_%d.bin", w.seq))
	outFile, err := storage.SafeCreate(outPath, 0600)
	if err != nil {
		_ = os.RemoveAll(w.tmpDir)
		return fmt.Errorf("create output file: %w", err)
	}
	cleanupErr := func(cause error) error {
		_ = outFile.Close()
		_ = os.Remove(outPath)
		_ = os.RemoveAll(w.tmpDir)
		return fmt.Errorf("failed to finalize SSTable: %w", cause)
	}

	// 写入占位 header (64B)
	var placeholder [HeaderSize]byte
	if _, err := outFile.Write(placeholder[:]); err != nil {
		return cleanupErr(err)
	}

	rowCount := int(w.totalRows)

	// 跟踪各段偏移量和大小
	var timestampsOffset, timestampsSize uint64
	var sidsOffset, sidsSize uint64
	type fieldInfo struct {
		offset   uint64
		size     uint64
		encoding EncodingType
	}
	fieldInfoMap := make(map[string]fieldInfo)

	// 构建 BlockSectionMap
	blockMap := &BlockSectionMap{}

	currentOffset := uint64(HeaderSize)

	// 1. 编码并写入 timestamps
	timestampsOffset = currentOffset
	timestampsEncoded, tsOffsets, tsEncoding, err := w.encodeTimestampsSection(rowCount)
	if err != nil {
		return cleanupErr(err)
	}
	if _, err := outFile.Write(timestampsEncoded); err != nil {
		return cleanupErr(err)
	}
	timestampsSize = uint64(len(timestampsEncoded))
	blockMap.Sections = append(blockMap.Sections, BlockSectionOffsets{
		Name: "_timestamps", Offsets: tsOffsets,
	})
	currentOffset += timestampsSize

	// 2. 编码并写入 sids
	sidsOffset = currentOffset
	sidsEncoded, sidOffsets, err := w.encodeSidsSection(rowCount)
	if err != nil {
		return cleanupErr(err)
	}
	if _, err := outFile.Write(sidsEncoded); err != nil {
		return cleanupErr(err)
	}
	sidsSize = uint64(len(sidsEncoded))
	blockMap.Sections = append(blockMap.Sections, BlockSectionOffsets{
		Name: "_sids", Offsets: sidOffsets,
	})
	currentOffset += sidsSize

	// 3. 编码并写入每个 field
	for _, name := range fieldNames {
		fi := fieldInfo{offset: currentOffset}
		encoded, fieldOffsets, enc, err := w.encodeFieldSection(name, rowCount)
		if err != nil {
			return cleanupErr(err)
		}
		if _, err := outFile.Write(encoded); err != nil {
			return cleanupErr(err)
		}
		fi.size = uint64(len(encoded))
		fi.encoding = enc
		fieldInfoMap[name] = fi
		blockMap.Sections = append(blockMap.Sections, BlockSectionOffsets{
			Name: name, Offsets: fieldOffsets,
		})
		currentOffset += fi.size
	}

	// 4. 写入 block index
	blockIndexOffset := currentOffset
	indexData, err := w.encodeBlockIndex()
	if err != nil {
		return cleanupErr(err)
	}
	if _, err := outFile.Write(indexData); err != nil {
		return cleanupErr(err)
	}
	currentOffset += uint64(len(indexData))

	// 5. 写入 _block_map section
	blockMapOffset := currentOffset
	blockMapData := blockMap.Marshal()
	if _, err := outFile.Write(blockMapData); err != nil {
		return cleanupErr(err)
	}
	currentOffset += uint64(len(blockMapData))

	// 6. 构建 Section Table
	sectionTable := SectionTable{
		Entries: []SectionEntry{
			{Type: SectionTimestamps, Name: "_timestamps", Offset: timestampsOffset, Size: timestampsSize, Encoding: tsEncoding, Compression: w.compressAlgo},
			{Type: SectionSids, Name: "_sids", Offset: sidsOffset, Size: sidsSize, Encoding: EncodingVarint, Compression: w.compressAlgo},
			{Type: SectionIndex, Name: "_index", Offset: blockIndexOffset, Size: uint64(len(indexData)), Encoding: EncodingRaw, Compression: CompressionNone},
			{Type: SectionIndex, Name: "_block_map", Offset: blockMapOffset, Size: uint64(len(blockMapData)), Encoding: EncodingRaw, Compression: CompressionNone},
		},
	}
	for _, name := range fieldNames {
		fi := fieldInfoMap[name]
		sectionTable.Entries = append(sectionTable.Entries, SectionEntry{
			Type: SectionField, Name: name, Offset: fi.offset, Size: fi.size, Encoding: fi.encoding, Compression: w.compressAlgo,
		})
	}

	// 7. 写入 Section Table
	sectionTableData := sectionTable.Marshal()
	sectionTableOffset := currentOffset
	if _, err := outFile.Write(sectionTableData); err != nil {
		return cleanupErr(err)
	}

	// 8. 回填 header
	header := FileHeader{
		Magic:              Magic,
		Version:            FileVersion,
		RowCount:           w.totalRows,
		FieldCount:         uint16(len(fieldNames)),
		BlockCount:         uint16(w.blockIndex.Len()),
		BlockSize:          uint16(w.blockSize),
		TimestampsOffset:   timestampsOffset,
		SidsOffset:         sidsOffset,
		BlockIndexOffset:   blockIndexOffset,
		SectionTableOffset: sectionTableOffset,
	}
	headerBuf := header.Marshal()
	if _, err := outFile.WriteAt(headerBuf[:], 0); err != nil {
		return cleanupErr(err)
	}

	if err := outFile.Close(); err != nil {
		_ = os.Remove(outPath)
		_ = os.RemoveAll(w.tmpDir)
		return fmt.Errorf("close output file: %w", err)
	}

	// 清理临时目录
	_ = os.RemoveAll(w.tmpDir)

	return nil
}

// encodeTimestampsSection 逐 block 流式编码时间戳。
func (w *Writer) encodeTimestampsSection(rowCount int) ([]byte, []uint64, EncodingType, error) {
	rawPath := filepath.Join(w.tmpDir, "_timestamps.bin")
	f, err := os.Open(rawPath)
	if err != nil {
		return nil, nil, EncodingRaw, fmt.Errorf("open timestamps temp: %w", err)
	}
	defer func() { _ = f.Close() }()

	var encoded []byte
	offsets := make([]uint64, 0, w.blockIndex.Len()+1)
	off := uint64(0)
	offsets = append(offsets, off)

	for i := 0; i < w.blockIndex.Len(); i++ {
		entry := w.blockIndex.Entry(i)
		n := int(entry.RowCount)
		raw := make([]byte, n*8)
		if _, err := io.ReadFull(f, raw); err != nil {
			return nil, nil, EncodingRaw, fmt.Errorf("read timestamps block %d: %w", i, err)
		}
		values := compression.ExtractInt64Data(raw, n)
		blockData := compression.EncodeTimestamps(values)
		compressed, _ := CompressBlock(blockData, w.compressAlgo)
		encoded = append(encoded, compressed...)
		off += uint64(len(compressed))
		offsets = append(offsets, off)
	}
	return encoded, offsets, EncodingDeltaVarint, nil
}

// encodeSidsSection 逐 block 流式编码 SID。
func (w *Writer) encodeSidsSection(rowCount int) ([]byte, []uint64, error) {
	rawPath := filepath.Join(w.tmpDir, "_sids.bin")
	f, err := os.Open(rawPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open sids temp: %w", err)
	}
	defer func() { _ = f.Close() }()

	var encoded []byte
	offsets := make([]uint64, 0, w.blockIndex.Len()+1)
	off := uint64(0)
	offsets = append(offsets, off)

	for i := 0; i < w.blockIndex.Len(); i++ {
		entry := w.blockIndex.Entry(i)
		n := int(entry.RowCount)
		raw := make([]byte, n*8)
		if _, err := io.ReadFull(f, raw); err != nil {
			return nil, nil, fmt.Errorf("read sids block %d: %w", i, err)
		}
		values := compression.ExtractUint64Data(raw, n)
		blockData := compression.EncodeSidsDelta(values)
		compressed, _ := CompressBlock(blockData, w.compressAlgo)
		encoded = append(encoded, compressed...)
		off += uint64(len(compressed))
		offsets = append(offsets, off)
	}
	return encoded, offsets, nil
}

// encodeFieldSection 逐 block 流式编码字段段（定长类型），变长类型（string）回退到全量读取。
func (w *Writer) encodeFieldSection(name string, rowCount int) ([]byte, []uint64, EncodingType, error) {
	ft := w.schema.Fields[name]
	rawPath := filepath.Join(w.tmpDir, "fields", name+".bin")

	switch ft {
	case FieldTypeFloat64:
		return w.encodeFixedFieldSection(rawPath, ft, 8, rowCount,
			func(raw []byte, n int) ([]byte, error) {
				values := compression.ExtractFloat64Data(raw, n)
				return compression.EncodeFloat64Values(values), nil
			}, EncodingXORFloat)
	case FieldTypeInt64:
		return w.encodeFixedFieldSection(rawPath, ft, 8, rowCount,
			func(raw []byte, n int) ([]byte, error) {
				values := compression.ExtractInt64Data(raw, n)
				return compression.EncodeInt64Values(values), nil
			}, EncodingZigZagVarint)
	case FieldTypeBool:
		return w.encodeFixedFieldSection(rawPath, ft, 1, rowCount,
			func(raw []byte, n int) ([]byte, error) {
				values := compression.ExtractBoolData(raw, n)
				return compression.EncodeBoolValues(values), nil
			}, EncodingBitmapBool)
	case FieldTypeString:
		return w.encodeStringFieldSection(name, rowCount)
	default:
		// raw bytes: 直接按 block 边界切片，不做编码
		raw, err := os.ReadFile(rawPath)
		if err != nil {
			return nil, nil, EncodingRaw, fmt.Errorf("read field %s temp: %w", name, err)
		}
		data, offsets := encodePerBlockRaw(w, raw, rowCount)
		return data, offsets, EncodingRaw, nil
	}
}

// encodeStringFieldSection 逐块流式编码字符串字段段（变长类型）。
//
// 与 encodeFixedFieldSection 相似，使用 fieldByteOffsets 中记录的字节偏移
// 逐块从 temp 文件中读取原始字符串数据，避免全量 os.ReadFile。
// 每块独立进行字典编码（有收益时自动启用），prepend 1 字节格式标志。
func (w *Writer) encodeStringFieldSection(name string, rowCount int) ([]byte, []uint64, EncodingType, error) {
	rawPath := filepath.Join(w.tmpDir, "fields", name+".bin")
	f, err := os.Open(rawPath)
	if err != nil {
		return nil, nil, EncodingRaw, fmt.Errorf("open field temp %s: %w", rawPath, err)
	}
	defer func() { _ = f.Close() }()

	byteOffsets := w.fieldByteOffsets[name]
	if len(byteOffsets) != w.blockIndex.Len() {
		return nil, nil, EncodingRaw, fmt.Errorf("field %s: byte offset count %d != block count %d",
			name, len(byteOffsets), w.blockIndex.Len())
	}

	var encoded []byte
	offsets := make([]uint64, 0, w.blockIndex.Len()+1)
	off := uint64(0)
	offsets = append(offsets, off)

	for i := 0; i < w.blockIndex.Len(); i++ {
		entry := w.blockIndex.Entry(i)
		n := int(entry.RowCount)

		// 定位到该 block 的字节偏移
		if _, err := f.Seek(byteOffsets[i], io.SeekStart); err != nil {
			return nil, nil, EncodingRaw, fmt.Errorf("seek field %s block %d: %w", name, i, err)
		}

		// 计算该 block 的字节大小
		var blockBytes int64
		if i+1 < len(byteOffsets) {
			blockBytes = byteOffsets[i+1] - byteOffsets[i]
		} else {
			fi, err := f.Stat()
			if err != nil {
				return nil, nil, EncodingRaw, fmt.Errorf("stat field %s temp: %w", name, err)
			}
			blockBytes = fi.Size() - byteOffsets[i]
		}

		raw := make([]byte, blockBytes)
		if _, err := io.ReadFull(f, raw); err != nil {
			return nil, nil, EncodingRaw, fmt.Errorf("read field %s block %d: %w", name, i, err)
		}

		values := compression.ExtractStringData(raw, n)
		blockData, isDict := compression.EncodeStringValues(values)

		// prepend 1 字节格式标志: 0=raw, 1=dict
		var flag byte
		if isDict {
			flag = 1
		}
		blockData = append([]byte{flag}, blockData...)

		compressed, _ := CompressBlock(blockData, w.compressAlgo)
		encoded = append(encoded, compressed...)
		off += uint64(len(compressed))
		offsets = append(offsets, off)
	}

	return encoded, offsets, EncodingDictString, nil
}

// encodeFixedFieldSection 逐 block 流式编码定长字段。
func (w *Writer) encodeFixedFieldSection(
	rawPath string, _ FieldType, bytesPerRow int, rowCount int,
	encodeFn func(raw []byte, n int) ([]byte, error),
	encType EncodingType,
) ([]byte, []uint64, EncodingType, error) {
	f, err := os.Open(rawPath)
	if err != nil {
		return nil, nil, EncodingRaw, fmt.Errorf("open field temp %s: %w", rawPath, err)
	}
	defer func() { _ = f.Close() }()

	var encoded []byte
	offsets := make([]uint64, 0, w.blockIndex.Len()+1)
	off := uint64(0)
	offsets = append(offsets, off)

	for i := 0; i < w.blockIndex.Len(); i++ {
		entry := w.blockIndex.Entry(i)
		n := int(entry.RowCount)
		raw := make([]byte, n*bytesPerRow)
		if _, err := io.ReadFull(f, raw); err != nil {
			return nil, nil, EncodingRaw, fmt.Errorf("read field block %d: %w", i, err)
		}
		blockData, err := encodeFn(raw, n)
		if err != nil {
			return nil, nil, EncodingRaw, err
		}
		compressed, _ := CompressBlock(blockData, w.compressAlgo)
		encoded = append(encoded, compressed...)
		off += uint64(len(compressed))
		offsets = append(offsets, off)
	}
	return encoded, offsets, encType, nil
}


// encodePerBlockRaw 对原始字节数据按 block 的行范围切片。
// raw 是未处理的原始数据，每行占固定字节数（由 rowSize 隐式确定）。
// 用于无法用泛型 encodeFn 描述的编码路径（如 raw 回退）。
func encodePerBlockRaw(w *Writer, raw []byte, rowCount int) ([]byte, []uint64) {
	var encoded []byte
	offsets := make([]uint64, 0, w.blockIndex.Len()+1)
	offset := uint64(0)
	offsets = append(offsets, offset)

	bytesPerRow := len(raw) / rowCount

	for i := 0; i < w.blockIndex.Len(); i++ {
		entry := w.blockIndex.Entry(i)
		start := int(entry.Offset) * bytesPerRow
		end := start + int(entry.RowCount)*bytesPerRow
		blockData := raw[start:end]
		compressed, _ := CompressBlock(blockData, w.compressAlgo)
		encoded = append(encoded, compressed...)
		offset += uint64(len(compressed))
		offsets = append(offsets, offset)
	}
	return encoded, offsets
}

// encodeBlockIndex 将 BlockIndex 序列化为字节。
func (w *Writer) encodeBlockIndex() ([]byte, error) {
	idx := w.blockIndex
	count := idx.Len()

	size := 16 + count*24
	buf := make([]byte, 0, size)

	var header [16]byte
	copy(header[0:8], IndexMagic[:])
	binary.BigEndian.PutUint32(header[8:12], IndexVersion)
	binary.BigEndian.PutUint32(header[12:16], uint32(count))
	buf = append(buf, header[:]...)

	for i := 0; i < count; i++ {
		e := idx.Entry(i)
		var entry [24]byte
		binary.BigEndian.PutUint64(entry[0:8], uint64(e.FirstTimestamp))
		binary.BigEndian.PutUint64(entry[8:16], uint64(e.LastTimestamp))
		binary.BigEndian.PutUint32(entry[16:20], e.Offset)
		binary.BigEndian.PutUint32(entry[20:24], e.RowCount)
		buf = append(buf, entry[:]...)
	}

	return buf, nil
}

// detectFieldType 检测字段类型。
func detectFieldType(val any) FieldType {
	if val == nil {
		return FieldTypeFloat64
	}

	if fv, ok := val.(*types.FieldValue); ok {
		if fv == nil || fv.Value == nil {
			return FieldTypeFloat64
		}
		switch fv.Value.(type) {
		case *types.FieldValue_FloatValue:
			return FieldTypeFloat64
		case *types.FieldValue_IntValue:
			return FieldTypeInt64
		case *types.FieldValue_StringValue:
			return FieldTypeString
		case *types.FieldValue_BoolValue:
			return FieldTypeBool
		}
		return FieldTypeFloat64
	}

	switch val.(type) {
	case float64:
		return FieldTypeFloat64
	case int64:
		return FieldTypeInt64
	case string:
		return FieldTypeString
	case bool:
		return FieldTypeBool
	}
	return FieldTypeFloat64
}
