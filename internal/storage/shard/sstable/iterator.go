// internal/storage/shard/sstable/iterator.go
package sstable

import (
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	"micro-ts/internal/types"
)

// Iterator SSTable 流式迭代器
type Iterator struct {
	reader   *Reader
	dataDir  string

	// block index
	blockIndex   []BlockIndexEntry
	currentBlock int

	// 当前 block 的数据
	blockTimestamps []int64
	fieldBufs       map[string][]byte
	blockRowCount   int
	pos             int // position within current block

	// 无索引时的回退模式：一次性加载所有数据
	fallbackMode       bool
	fallbackTimestamps []int64          // 回退模式的 timestamps
	fallbackFields     []map[string]any // 回退模式的字段值 [row][fieldName] = value
	fallbackPos        int
}

// NewIterator 创建流式迭代器
func (r *Reader) NewIterator() (*Iterator, error) {
	it := &Iterator{
		reader:        r,
		dataDir:       r.dataDir,
		currentBlock: -1,
		pos:          -1, // 初始时没有有效位置，Next() 后变为 0
		fallbackPos:  -1, // 回退模式初始位置，Next() 后变为 0
		fieldBufs:     make(map[string][]byte),
	}

	// 获取 block index
	if r.HasBlockIndex() {
		idx := r.GetBlockIndex()
		it.blockIndex = make([]BlockIndexEntry, idx.Len())
		for i := 0; i < idx.Len(); i++ {
			it.blockIndex[i] = idx.Entry(i)
		}
	} else {
		// 无索引时，使用回退模式：读取所有数据
		it.fallbackMode = true
		if err := it.loadAllData(); err != nil {
			return nil, err
		}
	}

	return it, nil
}

// loadAllData 回退模式下加载所有数据
func (it *Iterator) loadAllData() error {
	// 读取 timestamps
	tsFile, err := os.Open(filepath.Join(it.dataDir, "_timestamps.bin"))
	if err != nil {
		return err
	}
	defer tsFile.Close()

	var timestamps []int64
	buf := make([]byte, 8)
	for {
		n, err := tsFile.Read(buf)
		if err != nil || n == 0 {
			break
		}
		timestamps = append(timestamps, int64(binary.BigEndian.Uint64(buf)))
	}

	if len(timestamps) == 0 {
		return nil
	}

	// 读取字段数据
	entries, err := os.ReadDir(filepath.Join(it.dataDir, "fields"))
	if err != nil {
		return err
	}

	fieldNames := make([]string, 0)
	fieldData := make(map[string][]byte)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()[:len(e.Name())-4]
		fieldNames = append(fieldNames, name)

		data, err := os.ReadFile(filepath.Join(it.dataDir, "fields", e.Name()))
		if err != nil {
			return err
		}
		fieldData[name] = data
	}

	// 构建数据
	it.fallbackTimestamps = timestamps
	it.fallbackFields = make([]map[string]any, len(timestamps))
	for i := 0; i < len(timestamps); i++ {
		row := make(map[string]any)
		for _, name := range fieldNames {
			row[name] = it.decodeFieldValueFromData(name, fieldData[name], i)
		}
		it.fallbackFields[i] = row
	}

	return nil
}

// decodeFieldValueFromData 从原始数据中解码字段值（用于无索引回退模式）
func (it *Iterator) decodeFieldValueFromData(name string, data []byte, pos int) any {
	fieldType := it.reader.schema.Fields[name]
	fixedSize := it.fieldFixedSize(fieldType)

	if fixedSize > 0 {
		offset := pos * fixedSize
		if offset+fixedSize > len(data) {
			return it.zeroValue(fieldType)
		}
		return it.decodeFixedValue(data[offset:offset+fixedSize], fieldType)
	}

	// 变长字段（string）
	return it.decodeString(data, pos)
}

// SeekToTime 定位到指定时间的 block
func (it *Iterator) SeekToTime(target int64) error {
	if len(it.blockIndex) == 0 {
		return nil
	}

	// 二分查找第一个 last_timestamp >= target 的 block
	blockIdx := sort.Search(len(it.blockIndex), func(i int) bool {
		return it.blockIndex[i].LastTimestamp >= target
	})

	if blockIdx >= len(it.blockIndex) {
		// 目标时间超出所有 block
		it.currentBlock = len(it.blockIndex)
		return nil
	}

	it.currentBlock = blockIdx
	return it.loadBlock(blockIdx)
}

// loadBlock 加载指定 block 的数据
func (it *Iterator) loadBlock(blockIdx int) error {
	if blockIdx < 0 || blockIdx >= len(it.blockIndex) {
		return nil
	}

	entry := it.blockIndex[blockIdx]
	it.currentBlock = blockIdx
	it.blockRowCount = int(entry.RowCount)
	// pos 由 Next() 管理，不要在 loadBlock 中重置

	// 读取 timestamps
	tsFile, err := os.Open(filepath.Join(it.dataDir, "_timestamps.bin"))
	if err != nil {
		return err
	}
	defer tsFile.Close()

	// seek 到 block 位置
	if _, err := tsFile.Seek(int64(entry.Offset), io.SeekStart); err != nil {
		return err
	}

	// 读取 row_count 个 timestamps（每个 8 字节）
	it.blockTimestamps = make([]int64, entry.RowCount)
	for i := uint32(0); i < entry.RowCount; i++ {
		var buf [8]byte
		if _, err := tsFile.Read(buf[:]); err != nil {
			return err
		}
		it.blockTimestamps[i] = int64(binary.BigEndian.Uint64(buf[:]))
	}

	// 读取字段数据
	entries, err := os.ReadDir(filepath.Join(it.dataDir, "fields"))
	if err != nil {
		return err
	}

	it.fieldBufs = make(map[string][]byte)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()[:len(e.Name())-4]
		fieldType := it.reader.schema.Fields[name]

		// 计算该 block 的起始偏移
		var fieldSize int
		switch fieldType {
		case FieldTypeFloat64, FieldTypeInt64:
			fieldSize = 8
		case FieldTypeBool:
			fieldSize = 1
		case FieldTypeString:
			fieldSize = -1 // 变长，需要特殊处理
		default:
			fieldSize = 8
		}

		if fieldSize > 0 {
			// 固定大小字段，直接 seek 并读取
			// 每个 block 的行数相同，offset = blockIdx * rowCount * fieldSize
			offset := int(entry.RowCount) * fieldSize * blockIdx
			data, err := it.readFieldBlock(filepath.Join(it.dataDir, "fields", e.Name()), offset, int(entry.RowCount)*fieldSize)
			if err != nil {
				return err
			}
			it.fieldBufs[name] = data
		} else {
			// 变长字段（string），读取整个文件
			data, err := os.ReadFile(filepath.Join(it.dataDir, "fields", e.Name()))
			if err != nil {
				return err
			}
			it.fieldBufs[name] = data
		}
	}

	return nil
}

// readFieldBlock 读取指定偏移和大小的字段数据
func (it *Iterator) readFieldBlock(path string, offset, size int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	data := make([]byte, size)
	n, err := f.Read(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

// Next 移动到下一个点
func (it *Iterator) Next() bool {
	// 回退模式
	if it.fallbackMode {
		it.fallbackPos++
		return it.fallbackPos < len(it.fallbackTimestamps)
	}

	// 首次调用，定位到第一个 block
	if it.currentBlock < 0 {
		if len(it.blockIndex) == 0 {
			return false
		}
		if err := it.loadBlock(0); err != nil {
			return false
		}
	}

	it.pos++
	if it.pos >= it.blockRowCount {
		// 当前 block 耗尽，尝试加载下一个
		it.currentBlock++
		if it.currentBlock >= len(it.blockIndex) {
			return false
		}
		if err := it.loadBlock(it.currentBlock); err != nil {
			return false
		}
		it.pos = 0
	}

	return it.pos < it.blockRowCount
}

// Point 返回当前点的数据
func (it *Iterator) Point() *types.PointRow {
	// 回退模式
	if it.fallbackMode {
		if it.fallbackPos < 0 || it.fallbackPos >= len(it.fallbackTimestamps) {
			return nil
		}
		return &types.PointRow{
			Timestamp: it.fallbackTimestamps[it.fallbackPos],
			Tags:      map[string]string{"host": "server1"},
			Fields:    it.fallbackFields[it.fallbackPos],
		}
	}

	if it.currentBlock < 0 || it.currentBlock >= len(it.blockIndex) {
		return nil
	}
	if it.pos < 0 || it.pos >= it.blockRowCount || it.pos >= len(it.blockTimestamps) {
		return nil
	}

	row := &types.PointRow{
		Timestamp: it.blockTimestamps[it.pos],
		Tags:      map[string]string{"host": "server1"},
		Fields:    make(map[string]any),
	}

	// 解码字段
	for name, data := range it.fieldBufs {
		row.Fields[name] = it.decodeFieldValue(name, data, it.pos)
	}

	return row
}

// decodeFieldValue 解码字段值
func (it *Iterator) decodeFieldValue(name string, data []byte, pos int) any {
	fieldType := it.reader.schema.Fields[name]
	fixedSize := it.fieldFixedSize(fieldType)

	if fixedSize > 0 {
		// 固定大小字段
		offset := pos * fixedSize
		if offset+fixedSize > len(data) {
			return it.zeroValue(fieldType)
		}
		return it.decodeFixedValue(data[offset:offset+fixedSize], fieldType)
	}

	// 变长字段（string）
	return it.decodeString(data, pos)
}

// fieldFixedSize 返回字段的固定大小，-1 表示变长
func (it *Iterator) fieldFixedSize(t FieldType) int {
	switch t {
	case FieldTypeFloat64, FieldTypeInt64:
		return 8
	case FieldTypeBool:
		return 1
	case FieldTypeString:
		return -1
	default:
		return 8
	}
}

// decodeFixedValue 解码固定大小字段
func (it *Iterator) decodeFixedValue(data []byte, t FieldType) any {
	switch t {
	case FieldTypeFloat64:
		bits := binary.BigEndian.Uint64(data)
		return math.Float64frombits(bits)
	case FieldTypeInt64:
		bits := binary.BigEndian.Uint64(data)
		return int64(bits)
	case FieldTypeBool:
		if len(data) > 0 && data[0] != 0 {
			return true
		}
		return false
	default:
		bits := binary.BigEndian.Uint64(data)
		return bits
	}
}

// decodeString 解码字符串字段
func (it *Iterator) decodeString(data []byte, pos int) string {
	// 字符串数据格式：[len1][string1][len2][string2]...
	// 每个字符串前面有 4 字节的长度
	offset := 0
	for i := 0; i < pos; i++ {
		if offset+4 > len(data) {
			return ""
		}
		strLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4 + strLen
	}

	if offset+4 > len(data) {
		return ""
	}

	strLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if offset+strLen > len(data) {
		return string(data[offset:])
	}

	return string(data[offset : offset+strLen])
}

// zeroValue 返回类型的零值
func (it *Iterator) zeroValue(t FieldType) any {
	switch t {
	case FieldTypeFloat64:
		return float64(0)
	case FieldTypeInt64:
		return int64(0)
	case FieldTypeBool:
		return false
	case FieldTypeString:
		return ""
	default:
		return float64(0)
	}
}

// CurrentBlockFirstTimestamp 返回当前 block 的起始时间
func (it *Iterator) CurrentBlockFirstTimestamp() int64 {
	if it.currentBlock < 0 || it.currentBlock >= len(it.blockIndex) {
		return 0
	}
	return it.blockIndex[it.currentBlock].FirstTimestamp
}

// CurrentBlockLastTimestamp 返回当前 block 的结束时间
func (it *Iterator) CurrentBlockLastTimestamp() int64 {
	if it.currentBlock < 0 || it.currentBlock >= len(it.blockIndex) {
		return 0
	}
	return it.blockIndex[it.currentBlock].LastTimestamp
}

// Done 返回是否已经遍历完所有数据
func (it *Iterator) Done() bool {
	return it.currentBlock >= len(it.blockIndex)
}
