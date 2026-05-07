// Package sstable 实现 SSTable 读取功能。
package sstable

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"codeberg.org/micro-ts/mts/types"
)

// Reader 是 SSTable 的读取器，支持索引查询和范围查询。
type Reader struct {
	dataDir    string
	schema     Schema
	blockIndex *BlockIndex
}

// NewReader 创建 SSTable 读取器。
func NewReader(dataDir string) (*Reader, error) {
	r := &Reader{dataDir: dataDir}
	if err := r.readSchema(); err != nil {
		r.schema = Schema{Fields: make(map[string]FieldType)}
	}

	r.blockIndex = &BlockIndex{}
	indexFile := filepath.Join(dataDir, "_index.bin")
	if err := r.blockIndex.Read(indexFile); err != nil {
		r.blockIndex = nil
	}

	return r, nil
}

func (r *Reader) readSchema() error {
	schemaFile, err := os.Open(filepath.Join(r.dataDir, "_schema.json"))
	if err != nil {
		return err
	}
	defer func() { _ = schemaFile.Close() }()

	data, err := io.ReadAll(schemaFile)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &r.schema)
}

// Close 关闭读取器，释放资源。
func (r *Reader) Close() error {
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

// ReadAll 读取 SSTable 中的所有数据。
func (r *Reader) ReadAll(fields []string) ([]*types.PointRow, error) {
	dataDir := r.dataDir

	tsFile, err := os.Open(filepath.Join(dataDir, "_timestamps.bin"))
	if err != nil {
		return nil, err
	}
	timestamps, err := r.readTimestamps(tsFile)
	if closeErr := tsFile.Close(); closeErr != nil {
		return nil, closeErr
	}
	if err != nil {
		return nil, err
	}

	sids, err := r.readSids(dataDir, len(timestamps))
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		entries, err := os.ReadDir(filepath.Join(dataDir, "fields"))
		if err == nil {
			for _, e := range entries {
				if !e.IsDir() {
					fields = append(fields, e.Name()[:len(e.Name())-4])
				}
			}
		}
	}

	fieldData := make(map[string][]byte)
	for _, name := range fields {
		f, err := os.Open(filepath.Join(dataDir, "fields", name+".bin"))
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(f)
		if closeErr := f.Close(); closeErr != nil {
			return nil, closeErr
		}
		if err != nil {
			return nil, err
		}
		fieldData[name] = data
	}

	offsets := r.computeOffsets(fields, fieldData, len(timestamps))

	rows := make([]*types.PointRow, len(timestamps))
	for i, ts := range timestamps {
		row := &types.PointRow{
			Sid:       sids[i],
			Timestamp: ts,
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

// computeOffsets 预计算每个字段每个条目的字节偏移量
func (r *Reader) computeOffsets(fields []string, fieldData map[string][]byte, rowCount int) map[string][]int {
	offsets := make(map[string][]int)
	for _, name := range fields {
		offsets[name] = r.computeFieldOffsets(name, fieldData[name], rowCount)
	}
	return offsets
}

// computeFieldOffsets 计算单个字段所有条目的字节偏移量
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

// fieldSize 计算单个字段值的大小
func (r *Reader) fieldSize(data []byte, fieldType FieldType) int {
	switch fieldType {
	case FieldTypeFloat64, FieldTypeInt64:
		return 8
	case FieldTypeString:
		if len(data) < 4 {
			return len(data)
		}
		strLen := binary.BigEndian.Uint32(data)
		return 4 + int(strLen)
	case FieldTypeBool:
		return 1
	default:
		return 8
	}
}
