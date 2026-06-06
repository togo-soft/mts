package sstable

import (
	"encoding/binary"
	"io"
	"log/slog"
	"math"
	"os"

	"codeberg.org/micro-ts/mts/types"
)

// Reader 是 SSTable 的读取器。
type Reader struct {
	file            *os.File
	header          FileHeader
	sectionTable    SectionTable
	blockIndex      *BlockIndex
	blockSectionMap *BlockSectionMap // 每个 section 内各 block 的字节偏移
	zoneMapIndex    *ZoneMapIndex    // 块级统计，用于谓词下推跳过
	Flags           uint16           // 文件头标志位 (FlagSorted=0 有序, FlagUnordered=1 无序)
	schema          Schema
}

// NewReader 创建 SSTable 读取器，接收 .bin 文件路径和 schema。
func NewReader(filePath string, schema Schema) (*Reader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	// 读取 header (64B)
	var headerBuf [HeaderSize]byte
	if _, err := io.ReadFull(f, headerBuf[:]); err != nil {
		_ = f.Close()
		return nil, err
	}
	header, err := UnmarshalFileHeader(headerBuf)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	// 读取 section table
	fileInfo, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	stSize := int64(header.SectionTableOffset)
	sectionDataSize := fileInfo.Size() - stSize
	if sectionDataSize < 0 || sectionDataSize > 64*1024 {
		_ = f.Close()
		return nil, ErrInvalidIndex
	}
	sectionData := make([]byte, sectionDataSize)
	if _, err := f.ReadAt(sectionData, stSize); err != nil {
		_ = f.Close()
		return nil, err
	}
	sectionTable, err := UnmarshalSectionTable(sectionData)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	// 读取 block index
	idxOffset, idxSize, _ := sectionTable.LookupByType(SectionIndex)
	blockIndex := &BlockIndex{}
	if idxSize > 0 {
		idxData := make([]byte, idxSize)
		if _, err := f.ReadAt(idxData, int64(idxOffset)); err != nil {
			slog.Warn("failed to read block index, reader may return empty data", "path", filePath, "error", err)
			blockIndex = nil
		} else if err := blockIndex.parse(idxData); err != nil {
			slog.Warn("failed to parse block index, reader may return empty data", "path", filePath, "error", err)
			blockIndex = nil
		}
	}

	// 读取 block section map
	var blockSectionMap *BlockSectionMap
	bmOffset, bmSize := sectionTable.Lookup("_block_map")
	if bmSize > 0 {
		bmData := make([]byte, bmSize)
		if _, err := f.ReadAt(bmData, int64(bmOffset)); err == nil {
			blockSectionMap, _ = UnmarshalBlockSectionMap(bmData)
		}
	}

	// 读取 zone map
	var zoneMapIndex *ZoneMapIndex
	zmOffset, zmSize := sectionTable.Lookup("_zone_map")
	if zmSize > 0 {
		zmData := make([]byte, zmSize)
		if _, err := f.ReadAt(zmData, int64(zmOffset)); err == nil {
			zoneMapIndex, _ = UnmarshalZoneMapIndex(zmData)
		}
	}

	return &Reader{
		file:            f,
		header:          header,
		Flags:           header.Flags,
		sectionTable:    sectionTable,
		blockIndex:      blockIndex,
		blockSectionMap: blockSectionMap,
		zoneMapIndex:    zoneMapIndex,
		schema:          schema,
	}, nil
}

// Close 关闭读取器，可重复调用。
func (r *Reader) Close() error {
	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		return err
	}
	return nil
}

// GetBlockIndex 返回 BlockIndex。
func (r *Reader) GetBlockIndex() *BlockIndex {
	return r.blockIndex
}

// TimestampsOffset 返回 timestamps 段的文件偏移量。
func (r *Reader) TimestampsOffset() uint64 {
	return r.header.TimestampsOffset
}

// SidsOffset 返回 sids 段的文件偏移量。
func (r *Reader) SidsOffset() uint64 {
	return r.header.SidsOffset
}

// ReadAll 读取 SSTable 中的所有数据，使用编码感知的解码器。
func (r *Reader) ReadAll(fields []string) ([]*types.PointRow, error) {
	rowCount := int(r.header.RowCount)

	timestamps, err := r.readTimestamps()
	if err != nil {
		return nil, err
	}
	sids, err := r.readSids(rowCount)
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		fields = r.sectionTable.FieldNames()
	}

	decodedFields, err := r.ReadAllDecodedFieldSections(fields, rowCount)
	if err != nil {
		return nil, err
	}

	rows := make([]*types.PointRow, rowCount)
	for i := range rowCount {
		row := &types.PointRow{
			Timestamp: timestamps[i],
		}
		if i < len(sids) {
			row.Sid = sids[i]
		}
		row.Fields = make([]*types.FieldEntry, 0, len(fields))
		for _, name := range fields {
			if vals, ok := decodedFields[name]; ok && i < len(vals) {
				row.Fields = append(row.Fields, &types.FieldEntry{Key: name, Value: vals[i]})
			}
		}
		rows[i] = row
	}

	return rows, nil
}

// computeFieldOffsets 计算单个字段所有条目的字节偏移量。
func (r *Reader) computeFieldOffsets(name string, data []byte, rowCount int) []int {
	offsets := make([]int, rowCount)
	fieldType := r.schema.Fields[name]
	pos := 0
	for i := range rowCount {
		offsets[i] = pos
		if pos >= len(data) {
			continue
		}
		size := r.fieldSize(data[pos:], fieldType)
		pos += size
	}
	return offsets
}

// fieldSize 计算单个字段值的大小。
func (r *Reader) fieldSize(data []byte, fieldType FieldType) int {
	switch fieldType {
	case FieldTypeFloat64, FieldTypeInt64:
		return 8
	case FieldTypeString:
		if len(data) < 4 {
			return len(data)
		}
		return 4 + int(binary.BigEndian.Uint32(data))
	case FieldTypeBool:
		return 1
	default:
		return 8
	}
}

// decodeFieldValue 解码字段值（用于原始编码）。
func (r *Reader) decodeFieldValue(data []byte, offset int, fieldName string) *types.FieldValue {
	fieldType := r.schema.Fields[fieldName]

	switch fieldType {
	case FieldTypeFloat64:
		if offset+8 > len(data) {
			return types.NewFieldValue(float64(0))
		}
		bits := binary.BigEndian.Uint64(data[offset : offset+8])
		return types.NewFieldValue(math.Float64frombits(bits))
	case FieldTypeInt64:
		if offset+8 > len(data) {
			return types.NewFieldValue(int64(0))
		}
		bits := binary.BigEndian.Uint64(data[offset : offset+8])
		return types.NewFieldValue(int64(bits))
	case FieldTypeString:
		if offset+4 > len(data) {
			return types.NewFieldValue("")
		}
		strLen := binary.BigEndian.Uint32(data[offset : offset+4])
		start := offset + 4
		end := start + int(strLen)
		if end > len(data) {
			return types.NewFieldValue(string(data[start:]))
		}
		return types.NewFieldValue(string(data[start:end]))
	case FieldTypeBool:
		if offset >= len(data) {
			return types.NewFieldValue(false)
		}
		return types.NewFieldValue(data[offset] != 0)
	default:
		return types.NewFieldValue(nil)
	}
}
