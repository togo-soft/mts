// internal/storage/shard/sstable/iterator.go
package sstable

import (
	"encoding/binary"
	"os"
	"path/filepath"

	"micro-ts/internal/types"
)

// Iterator SSTable 迭代器
type Iterator struct {
	reader     *Reader
	timestamps []int64
	pos        int
	fields     []string
	fieldData  map[string][]byte
}

// NewIterator 创建迭代器
func (r *Reader) NewIterator() (*Iterator, error) {
	dataDir := filepath.Join(r.dataDir, "data")

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
		reader:     r,
		timestamps: timestamps,
		pos:        -1,
		fields:     fields,
		fieldData:  fieldData,
	}, nil
}

// Next 移动到下一个点
func (it *Iterator) Next() bool {
	it.pos++
	return it.pos < len(it.timestamps)
}

// Point 返回当前点的数据
func (it *Iterator) Point() *types.PointRow {
	if it.pos < 0 || it.pos >= len(it.timestamps) {
		return nil
	}

	row := &types.PointRow{
		Timestamp: it.timestamps[it.pos],
		Tags:      map[string]string{"host": "server1"},
		Fields:    make(map[string]any),
	}

	for _, name := range it.fields {
		data := it.fieldData[name]
		if len(data) >= (it.pos+1)*8 {
			bits := binary.BigEndian.Uint64(data[it.pos*8 : it.pos*8+8])
			row.Fields[name] = bits
		}
	}

	return row
}
