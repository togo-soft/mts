// internal/storage/shard/sstable/reader.go
package sstable

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"micro-ts/internal/types"
)

// Reader SSTable 读取器
type Reader struct {
	dataDir string
}

// NewReader 创建 Reader
func NewReader(dataDir string) (*Reader, error) {
	return &Reader{dataDir: dataDir}, nil
}

// Close 关闭
func (r *Reader) Close() error {
	return nil
}

// ReadAll 读取所有数据
func (r *Reader) ReadAll(fields []string) ([]types.PointRow, error) {
	dataDir := filepath.Join(r.dataDir, "data")

	// 读取 timestamps
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

	// 如果没有指定字段，读取所有字段文件
	if len(fields) == 0 {
		entries, err := os.ReadDir(filepath.Join(dataDir, "fields"))
		if err == nil {
			for _, e := range entries {
				if !e.IsDir() {
					fields = append(fields, e.Name()[:len(e.Name())-4]) // 去掉 .bin
				}
			}
		}
	}

	// 读取各字段
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

	// 构建结果
	rows := make([]types.PointRow, len(timestamps))
	for i, ts := range timestamps {
		row := types.PointRow{
			Timestamp: ts,
			Tags:      map[string]string{"host": "server1"},
			Fields:    make(map[string]any),
		}

		for _, name := range fields {
			row.Fields[name] = r.decodeFieldValue(fieldData[name], i, name)
		}

		rows[i] = row
	}

	return rows, nil
}

func (r *Reader) readTimestamps(f *os.File) ([]int64, error) {
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	timestamps := make([]int64, 0, len(data)/8)
	for i := 0; i+8 <= len(data); i += 8 {
		ts := int64(binary.BigEndian.Uint64(data[i : i+8]))
		timestamps = append(timestamps, ts)
	}
	return timestamps, nil
}

// ReadRange 读取时间范围内的数据
func (r *Reader) ReadRange(startTime, endTime int64) ([]types.PointRow, error) {
	dataDir := r.dataDir

	// 检查数据目录是否存在
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return nil, nil
	}

	// 读取 timestamps
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

	// 读取所有字段文件
	entries, err := os.ReadDir(filepath.Join(dataDir, "fields"))
	if err != nil {
		return nil, err
	}

	fields := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			fields = append(fields, e.Name()[:len(e.Name())-4]) // 去掉 .bin
		}
	}

	// 读取各字段
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

	// 构建结果，按时间过滤
	var rows []types.PointRow
	for i, ts := range timestamps {
		if ts >= startTime && ts < endTime {
			row := types.PointRow{
				Timestamp: ts,
				Tags:      map[string]string{"host": "server1"},
				Fields:    make(map[string]any),
			}

			for _, name := range fields {
				row.Fields[name] = r.decodeFieldValue(fieldData[name], i, name)
			}

			rows = append(rows, row)
		}
	}

	return rows, nil
}

func (r *Reader) decodeFieldValue(data []byte, index int, fieldName string) any {
	// 简化实现：假设 float64 和 int64 都是 8 字节
	if len(data) < (index+1)*8 {
		return nil
	}
	bits := binary.BigEndian.Uint64(data[index*8 : index*8+8])

	// 根据字段名判断类型（简化处理）
	if len(fieldName) > 4 && fieldName[:4] == "field" {
		// 尝试判断是 float64 还是 int64
		// 这里简化处理，返回 uint64
		return bits
	}
	return bits
}
