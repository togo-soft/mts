package sstable

import (
	"encoding/binary"
	"io"
	"math"
	"os"

	"codeberg.org/micro-ts/mts/types"
)

// Reader 是 SSTable 的读取器。
type Reader struct {
	file         *os.File
	header       FileHeader
	sectionTable SectionTable
	blockIndex   *BlockIndex
	schema       Schema
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
			blockIndex = nil
		} else if err := blockIndex.parse(idxData); err != nil {
			blockIndex = nil
		}
	}

	return &Reader{
		file:         f,
		header:       header,
		sectionTable: sectionTable,
		blockIndex:   blockIndex,
		schema:       schema,
	}, nil
}

// Close 关闭读取器。
func (r *Reader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// HasBlockIndex 返回是否有可用的 BlockIndex。
func (r *Reader) HasBlockIndex() bool {
	return r.blockIndex != nil && r.blockIndex.Len() > 0
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

// ReadAll 读取 SSTable 中的所有数据。
func (r *Reader) ReadAll(fields []string) ([]*types.PointRow, error) {
	tsOffset, _, _ := r.sectionTable.LookupByType(SectionTimestamps)
	sidOffset, _, _ := r.sectionTable.LookupByType(SectionSids)
	rowCount := int(r.header.RowCount)

	timestamps := make([]int64, rowCount)
	tsData := make([]byte, rowCount*8)
	if _, err := r.file.ReadAt(tsData, int64(tsOffset)); err != nil {
		return nil, err
	}
	for i := 0; i < rowCount; i++ {
		timestamps[i] = int64(binary.BigEndian.Uint64(tsData[i*8:]))
	}

	sids := make([]uint64, rowCount)
	sidData := make([]byte, rowCount*8)
	if n, _ := r.file.ReadAt(sidData, int64(sidOffset)); n == rowCount*8 {
		for i := 0; i < rowCount; i++ {
			sids[i] = binary.BigEndian.Uint64(sidData[i*8:])
		}
	}

	if len(fields) == 0 {
		fields = r.sectionTable.FieldNames()
	}

	fieldData := make(map[string][]byte)
	for _, name := range fields {
		fOffset, fSize := r.sectionTable.Lookup(name)
		if fSize == 0 {
			continue
		}
		data := make([]byte, fSize)
		if _, err := r.file.ReadAt(data, int64(fOffset)); err != nil {
			return nil, err
		}
		fieldData[name] = data
	}

	offsets := r.computeOffsets(fields, fieldData, rowCount)

	rows := make([]*types.PointRow, rowCount)
	for i := 0; i < rowCount; i++ {
		row := &types.PointRow{
			Sid:       sids[i],
			Timestamp: timestamps[i],
			Tags:      nil,
			Fields:    make(map[string]*types.FieldValue),
		}
		for _, name := range fields {
			row.Fields[name] = r.decodeFieldValue(fieldData[name], offsets[name][i], name)
		}
		rows[i] = row
	}

	return rows, nil
}

// computeOffsets 预计算每个字段每个条目的字节偏移量。
func (r *Reader) computeOffsets(fields []string, fieldData map[string][]byte, rowCount int) map[string][]int {
	offsets := make(map[string][]int)
	for _, name := range fields {
		offsets[name] = r.computeFieldOffsets(name, fieldData[name], rowCount)
	}
	return offsets
}

// computeFieldOffsets 计算单个字段所有条目的字节偏移量。
func (r *Reader) computeFieldOffsets(name string, data []byte, rowCount int) []int {
	offsets := make([]int, rowCount)
	fieldType := r.schema.Fields[name]
	pos := 0
	for i := 0; i < rowCount; i++ {
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

// decodeFieldValue 解码字段值。
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
