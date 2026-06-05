package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"codeberg.org/micro-ts/mts/internal/storage/shard/compression"
)

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
		raw, err := os.ReadFile(rawPath)
		if err != nil {
			return nil, nil, EncodingRaw, fmt.Errorf("read field %s temp: %w", name, err)
		}
		data, offsets := encodePerBlockRaw(w, raw, rowCount)
		return data, offsets, EncodingRaw, nil
	}
}

// encodeStringFieldSection 逐块流式编码字符串字段段（变长类型）。
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

		if _, err := f.Seek(byteOffsets[i], io.SeekStart); err != nil {
			return nil, nil, EncodingRaw, fmt.Errorf("seek field %s block %d: %w", name, i, err)
		}

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
