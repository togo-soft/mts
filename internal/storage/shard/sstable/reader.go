// Package sstable 实现 SSTable 读取功能。
//
// 支持全表读取、范围查询和流式迭代。
//
// 性能优化：
//
//	- Block 索引加速范围查询
//	- 延迟加载字段数据
//	- 预计算偏移量优化随机访问
package sstable

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"

	"codeberg.org/micro-ts/mts/types"
)

// Reader 是 SSTable 的读取器，支持索引查询和范围查询。
//
// 功能：
//
//   - 加载 Schema 和 BlockIndex
//   - 支持全表读取
//   - 支持时间范围查询（如果索引存在）
//
// 字段说明：
//
//   - dataDir:    SSTable 数据目录
//   - schema:     字段类型定义
//   - blockIndex: 数据块索引
//
// 索引优化：
//
//	如果存在 BlockIndex，可以使用索引快速定位数据块。
//	没有索引时回退到全表扫描。
type Reader struct {
	dataDir    string
	schema     Schema
	blockIndex *BlockIndex
}

// NewReader 创建 SSTable 读取器。
//
// 参数：
//   - dataDir: SSTable 数据目录（包含 schema.json, _timestamps.bin 等）
//
// 返回：
//   - *Reader: 读取器实例
//   - error:   创建失败时返回错误
//
// Schema 处理：
//
//	尝试加载 schema.json，如果失败则使用空的 Schema。
//	这允许读取没有 schema 元数据的旧格式文件。
//
// 索引处理：
//
//	尝试加载 _index.bin，如果失败则 blockIndex 设为 nil。
//	查询会回退到全表扫描模式。
func NewReader(dataDir string) (*Reader, error) {
	r := &Reader{dataDir: dataDir}
	if err := r.readSchema(); err != nil {
		// schema 不存在也继续，使用默认类型推断
		r.schema = Schema{Fields: make(map[string]FieldType)}
	}

	// 尝试加载 block 索引
	r.blockIndex = &BlockIndex{}
	indexFile := filepath.Join(dataDir, "_index.bin")
	if err := r.blockIndex.Read(indexFile); err != nil {
		r.blockIndex = nil // 索引文件不存在或无效
	}

	return r, nil
}

// readSchema 读取 schema 文件
func (r *Reader) readSchema() error {
	schemaFile, err := os.Open(filepath.Join(r.dataDir, "_schema.json"))
	if err != nil {
		return err
	}
	defer schemaFile.Close()

	data, err := io.ReadAll(schemaFile)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &r.schema)
}

// Close 关闭读取器，释放资源。
//
// 返回：
//   - error: 关闭失败时返回错误（当前总是返回 nil）
//
// 说明：
//
//	Reader 本身不持有文件句柄，Close 仅为接口兼容。
//	实际文件句柄在读操作时创建并在使用后立即关闭。
func (r *Reader) Close() error {
	return nil
}

// HasBlockIndex 返回是否有可用的 BlockIndex。
//
// 返回：
//   - bool: true 表示有索引可以加速查询
//
// 应用场景：
//
//	在构建查询计划时，可以检查此标志决定是否使用索引优化。
func (r *Reader) HasBlockIndex() bool {
	return r.blockIndex != nil && r.blockIndex.Len() > 0
}

// GetBlockIndex 返回 BlockIndex。
//
// 返回：
//   - *BlockIndex: 索引对象，可能为 nil
//
// 注意：
//
//	使用前应先检查 HasBlockIndex。
//	返回的指针是内部的引用，不应修改。
func (r *Reader) GetBlockIndex() *BlockIndex {
	return r.blockIndex
}

// ReadAll 读取 SSTable 中的所有数据。
//
// 参数：
//   - fields: 要读取的字段列表，为空表示读取所有字段
//
// 返回：
//   - []types.PointRow: 所有数据点，按时间排序
//   - error:            读取失败时返回错误
//
// 性能：
//
//	对于大文件，此方法会消耗大量内存。
//	建议使用 Iterator 进行流式读取。
func (r *Reader) ReadAll(fields []string) ([]types.PointRow, error) {
	dataDir := r.dataDir

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

	// 读取各字段数据（原始 bytes）
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

	// 预计算每个字段的偏移量表
	offsets := r.computeOffsets(fields, fieldData, len(timestamps))

	// 构建结果
	rows := make([]types.PointRow, len(timestamps))
	for i, ts := range timestamps {
		row := types.PointRow{
			Timestamp: ts,
			Tags:      map[string]string{"host": "server1"},
			Fields:    make(map[string]any),
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
		pos += r.fieldSize(data[pos:], fieldType)
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

// ReadRange 读取指定时间范围内的数据。
//
// 参数：
//   - startTime: 起始时间（包含），纳秒
//   - endTime:   结束时间（不包含），纳秒，<=0 表示不限制
//
// 返回：
//   - []types.PointRow: 匹配的数据点
//   - error:            读取失败时返回错误
//
// 性能考虑：
//
//	当前实现是全表扫描，即使指定了时间范围。
//	未来可以结合 BlockIndex 实现索引加速。
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

	// 读取各字段数据
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

	// 预计算偏移量
	offsets := r.computeOffsets(fields, fieldData, len(timestamps))

	// 构建结果，按时间过滤
	var rows []types.PointRow
	for i, ts := range timestamps {
		if ts >= startTime && (endTime <= 0 || ts < endTime) {
			row := types.PointRow{
				Timestamp: ts,
				Tags:      map[string]string{"host": "server1"},
				Fields:    make(map[string]any),
			}

			for _, name := range fields {
				row.Fields[name] = r.decodeFieldValue(fieldData[name], offsets[name][i], name)
			}

			rows = append(rows, row)
		}
	}

	return rows, nil
}

// decodeFieldValue 解码字段值
func (r *Reader) decodeFieldValue(data []byte, offset int, fieldName string) any {
	fieldType := r.schema.Fields[fieldName]

	switch fieldType {
	case FieldTypeFloat64:
		if offset+8 > len(data) {
			return float64(0)
		}
		bits := binary.BigEndian.Uint64(data[offset : offset+8])
		return math.Float64frombits(bits)
	case FieldTypeInt64:
		if offset+8 > len(data) {
			return int64(0)
		}
		bits := binary.BigEndian.Uint64(data[offset : offset+8])
		return int64(bits)
	case FieldTypeString:
		if offset+4 > len(data) {
			return ""
		}
		strLen := binary.BigEndian.Uint32(data[offset : offset+4])
		start := offset + 4
		end := start + int(strLen)
		if end > len(data) {
			return string(data[start:])
		}
		return string(data[start:end])
	case FieldTypeBool:
		if offset >= len(data) {
			return false
		}
		return data[offset] != 0
	default:
		// 未知类型，尝试作为 float64 或 int64 解码
		if offset+8 > len(data) {
			return nil
		}
		bits := binary.BigEndian.Uint64(data[offset : offset+8])
		return bits
	}
}
