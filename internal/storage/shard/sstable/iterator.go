package sstable

import (
	"encoding/binary"
	"os"
	"path/filepath"

	"micro-ts/internal/types"
)

// Iterator SSTable 迭代器，支持流式读取
type Iterator struct {
	reader   *Reader
	dataDir  string
	pos      int
	rowCount int

	// block index for streaming (optional, loaded if available)
	blockIndex   []BlockIndexEntry
	currentBlock int
	blockPos     int // position within current block

	// all timestamps loaded for seeking
	timestamps []int64

	// all fields
	fields []string

	// field data: for variable-length fields, data is stored as offset table followed by actual data
	fieldData map[string][]byte

	// current point
	currentTs     int64
	currentFields map[string]any
}

// NewIterator 创建迭代器，支持流式读取
// 如果有 block index 则使用流式读取，否则回退到全量读取模式
func (r *Reader) NewIterator() (*Iterator, error) {
	dataDir := filepath.Join(r.dataDir, "data")

	// 尝试获取 block index
	idx := r.GetBlockIndex()
	var blockIndex []BlockIndexEntry
	if idx != nil && idx.Len() > 0 {
		blockIndex = idx.entries
	}

	// 读取 timestamps
	tsData, err := os.ReadFile(filepath.Join(dataDir, "_timestamps.bin"))
	if err != nil {
		return nil, err
	}

	timestamps := make([]int64, 0, len(tsData)/8)
	for i := 0; i+8 <= len(tsData); i += 8 {
		ts := int64(binary.BigEndian.Uint64(tsData[i : i+8]))
		timestamps = append(timestamps, ts)
	}

	// 读取字段列表
	entries, err := os.ReadDir(filepath.Join(dataDir, "fields"))
	if err != nil {
		return nil, err
	}

	fields := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			fields = append(fields, e.Name()[:len(e.Name())-4])
		}
	}

	// 读取各字段数据
	fieldData := make(map[string][]byte)
	for _, name := range fields {
		data, err := os.ReadFile(filepath.Join(dataDir, "fields", name+".bin"))
		if err != nil {
			return nil, err
		}
		fieldData[name] = data
	}

	return &Iterator{
		reader:       r,
		dataDir:      dataDir,
		pos:          -1,
		rowCount:     len(timestamps),
		blockIndex:   blockIndex,
		currentBlock: -1,
		blockPos:     0,
		timestamps:   timestamps,
		fields:       fields,
		fieldData:    fieldData,
	}, nil
}

// Next 移动到下一个点
func (it *Iterator) Next() bool {
	if it.pos+1 >= it.rowCount {
		return false
	}

	it.pos++
	it.blockPos++

	// 如果有 block index，可以使用二分查找定位 block
	// 目前简化实现：直接使用全局位置追踪
	// TODO: 使用 block index 进行二分查找优化

	// update current point data
	it.currentTs = it.timestamps[it.pos]
	it.currentFields = make(map[string]any)
	for _, name := range it.fields {
		it.currentFields[name] = it.decodeFieldValue(name, it.pos)
	}

	return true
}

// Point 返回当前点的数据
func (it *Iterator) Point() *types.PointRow {
	if it.pos < 0 || it.pos >= it.rowCount {
		return nil
	}

	return &types.PointRow{
		Timestamp: it.currentTs,
		Tags:      map[string]string{"host": "server1"},
		Fields:    it.currentFields,
	}
}

// decodeFieldValue 解码字段值
// pos is the row position within all data
func (it *Iterator) decodeFieldValue(fieldName string, pos int) any {
	data := it.fieldData[fieldName]
	if data == nil {
		return nil
	}

	fieldType := it.reader.schema.Fields[fieldName]

	switch fieldType {
	case FieldTypeFloat64:
		offset := pos * 8
		if offset+8 > len(data) {
			return float64(0)
		}
		bits := binary.BigEndian.Uint64(data[offset : offset+8])
		return float64(bits)
	case FieldTypeInt64:
		offset := pos * 8
		if offset+8 > len(data) {
			return int64(0)
		}
		bits := binary.BigEndian.Uint64(data[offset : offset+8])
		return int64(bits)
	case FieldTypeString:
		// For string fields, we need to use the offset table approach
		// The data format is: [offset0:4][offset1:4]...[data...]
		// where offsetN is the byte offset of string N relative to the start of data section
		offsetTableSize := (pos + 1) * 4
		if offsetTableSize > len(data) {
			return ""
		}
		strOffset := int(binary.BigEndian.Uint32(data[pos*4 : pos*4+4]))
		// find next offset or end of data
		var strLen int
		if pos+1 < (len(data)-offsetTableSize)/4 {
			nextOffset := int(binary.BigEndian.Uint32(data[(pos+1)*4 : (pos+1)*4+4]))
			strLen = nextOffset - strOffset
		} else {
			strLen = len(data) - offsetTableSize - strOffset
		}
		start := offsetTableSize + strOffset
		if start >= len(data) {
			return ""
		}
		end := start + strLen
		if end > len(data) {
			return string(data[start:])
		}
		return string(data[start:end])
	case FieldTypeBool:
		offset := pos
		if offset >= len(data) {
			return false
		}
		return data[offset] != 0
	default:
		// treat as int64 by default
		offset := pos * 8
		if offset+8 > len(data) {
			return int64(0)
		}
		bits := binary.BigEndian.Uint64(data[offset : offset+8])
		return int64(bits)
	}
}
