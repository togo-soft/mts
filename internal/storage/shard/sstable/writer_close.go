package sstable

import (
	"encoding/binary"
	"fmt"
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
	cleanupErr := func() error {
		_ = outFile.Close()
		_ = os.Remove(outPath)
		_ = os.RemoveAll(w.tmpDir)
		return fmt.Errorf("failed to finalize SSTable")
	}

	// 写入占位 header (64B)
	var placeholder [HeaderSize]byte
	if _, err := outFile.Write(placeholder[:]); err != nil {
		return cleanupErr()
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

	// 1. 编码并写入 timestamps（per-block V2）
	timestampsOffset = currentOffset
	timestampsEncoded, tsOffsets, tsEncoding, err := w.encodeTimestampsSectionV2(rowCount)
	if err != nil {
		return cleanupErr()
	}
	if _, err := outFile.Write(timestampsEncoded); err != nil {
		return cleanupErr()
	}
	timestampsSize = uint64(len(timestampsEncoded))
	blockMap.Sections = append(blockMap.Sections, BlockSectionOffsets{
		Name: "_timestamps", Offsets: tsOffsets,
	})
	currentOffset += timestampsSize

	// 2. 编码并写入 sids（per-block V2）
	sidsOffset = currentOffset
	sidsEncoded, sidOffsets, err := w.encodeSidsSectionV2(rowCount)
	if err != nil {
		return cleanupErr()
	}
	if _, err := outFile.Write(sidsEncoded); err != nil {
		return cleanupErr()
	}
	sidsSize = uint64(len(sidsEncoded))
	blockMap.Sections = append(blockMap.Sections, BlockSectionOffsets{
		Name: "_sids", Offsets: sidOffsets,
	})
	currentOffset += sidsSize

	// 3. 编码并写入每个 field（per-block V2）
	for _, name := range fieldNames {
		fi := fieldInfo{offset: currentOffset}
		encoded, fieldOffsets, enc, err := w.encodeFieldSectionV2(name, rowCount)
		if err != nil {
			return cleanupErr()
		}
		if _, err := outFile.Write(encoded); err != nil {
			return cleanupErr()
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
		return cleanupErr()
	}
	if _, err := outFile.Write(indexData); err != nil {
		return cleanupErr()
	}
	currentOffset += uint64(len(indexData))

	// 5. 写入 _block_map section
	blockMapOffset := currentOffset
	blockMapData := blockMap.Marshal()
	if _, err := outFile.Write(blockMapData); err != nil {
		return cleanupErr()
	}
	currentOffset += uint64(len(blockMapData))

	// 6. 构建 Section Table
	sectionTable := SectionTable{
		Entries: []SectionEntry{
			{Type: SectionTimestamps, Name: "_timestamps", Offset: timestampsOffset, Size: timestampsSize, Encoding: tsEncoding},
			{Type: SectionSids, Name: "_sids", Offset: sidsOffset, Size: sidsSize, Encoding: EncodingVarint},
			{Type: SectionIndex, Name: "_index", Offset: blockIndexOffset, Size: uint64(len(indexData)), Encoding: EncodingRaw},
			{Type: SectionIndex, Name: "_block_map", Offset: blockMapOffset, Size: uint64(len(blockMapData)), Encoding: EncodingRaw},
		},
	}
	for _, name := range fieldNames {
		fi := fieldInfoMap[name]
		sectionTable.Entries = append(sectionTable.Entries, SectionEntry{
			Type: SectionField, Name: name, Offset: fi.offset, Size: fi.size, Encoding: fi.encoding,
		})
	}

	// 7. 写入 Section Table
	sectionTableData := sectionTable.Marshal()
	sectionTableOffset := currentOffset
	if _, err := outFile.Write(sectionTableData); err != nil {
		return cleanupErr()
	}

	// 8. 回填 header
	header := FileHeader{
		Magic:              MagicV2,
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
		return cleanupErr()
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

// encodeTimestampsSectionV2 按 block 独立编码时间戳，返回编码数据和 per-block 字节偏移。
func (w *Writer) encodeTimestampsSectionV2(rowCount int) ([]byte, []uint64, EncodingType, error) {
	rawPath := filepath.Join(w.tmpDir, "_timestamps.bin")
	raw, err := os.ReadFile(rawPath)
	if err != nil {
		return nil, nil, EncodingRaw, fmt.Errorf("read timestamps temp: %w", err)
	}
	values := compression.ExtractInt64Data(raw, rowCount)
	data, offsets := encodePerBlock(w, values, func(vals []int64) []byte {
		return compression.EncodeTimestamps(vals)
	})
	return data, offsets, EncodingDeltaVarint, nil
}

// encodeSidsSectionV2 按 block 独立编码 SID，返回编码数据和 per-block 字节偏移。
func (w *Writer) encodeSidsSectionV2(rowCount int) ([]byte, []uint64, error) {
	rawPath := filepath.Join(w.tmpDir, "_sids.bin")
	raw, err := os.ReadFile(rawPath)
	if err != nil {
		return nil, nil, fmt.Errorf("read sids temp: %w", err)
	}
	values := compression.ExtractUint64Data(raw, rowCount)
	data, offsets := encodePerBlock(w, values, func(vals []uint64) []byte {
		return compression.EncodeSids(vals)
	})
	return data, offsets, nil
}

// encodeFieldSectionV2 按 block 独立编码字段段，返回编码数据、per-block 字节偏移和编码类型。
func (w *Writer) encodeFieldSectionV2(name string, rowCount int) ([]byte, []uint64, EncodingType, error) {
	ft := w.schema.Fields[name]
	rawPath := filepath.Join(w.tmpDir, "fields", name+".bin")
	raw, err := os.ReadFile(rawPath)
	if err != nil {
		return nil, nil, EncodingRaw, fmt.Errorf("read field %s temp: %w", name, err)
	}

	switch ft {
	case FieldTypeFloat64:
		values := compression.ExtractFloat64Data(raw, rowCount)
		data, offsets := encodePerBlock(w, values, func(vals []float64) []byte {
			return compression.EncodeFloat64Values(vals)
		})
		return data, offsets, EncodingXORFloat, nil
	case FieldTypeInt64:
		values := compression.ExtractInt64Data(raw, rowCount)
		data, offsets := encodePerBlock(w, values, func(vals []int64) []byte {
			return compression.EncodeInt64Values(vals)
		})
		return data, offsets, EncodingZigZagVarint, nil
	case FieldTypeString:
		values := compression.ExtractStringData(raw, rowCount)
		data, offsets := encodePerBlock(w, values, func(vals []string) []byte {
			return compression.EncodeStringValuesRaw(vals)
		})
		return data, offsets, EncodingRaw, nil
	case FieldTypeBool:
		values := compression.ExtractBoolData(raw, rowCount)
		data, offsets := encodePerBlock(w, values, func(vals []bool) []byte {
			return compression.EncodeBoolValues(vals)
		})
		return data, offsets, EncodingBitmapBool, nil
	default:
		// raw bytes: 直接按 block 边界切片，不做编码
		data, offsets := encodePerBlockRaw(w, raw, rowCount)
		return data, offsets, EncodingRaw, nil
	}
}

// encodePerBlock 将全部行数据按 BlockIndex 分块后独立编码，返回拼接数据和 per-block 字节偏移。
func encodePerBlock[T any](w *Writer, values []T, encodeFn func([]T) []byte) ([]byte, []uint64) {
	var encoded []byte
	offsets := make([]uint64, 0, w.blockIndex.Len()+1)
	offset := uint64(0)
	offsets = append(offsets, offset)
	for i := 0; i < w.blockIndex.Len(); i++ {
		entry := w.blockIndex.Entry(i)
		start := int(entry.Offset)
		end := start + int(entry.RowCount)
		blockData := encodeFn(values[start:end])
		encoded = append(encoded, blockData...)
		offset += uint64(len(blockData))
		offsets = append(offsets, offset)
	}
	return encoded, offsets
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
		encoded = append(encoded, blockData...)
		offset += uint64(len(blockData))
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
