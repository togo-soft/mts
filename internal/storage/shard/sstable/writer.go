// internal/storage/shard/sstable/writer.go
package sstable

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"micro-ts/internal/storage"
	"micro-ts/internal/types"
)

// FieldType 字段类型
type FieldType string

const (
	FieldTypeFloat64 FieldType = "float64"
	FieldTypeInt64   FieldType = "int64"
	FieldTypeString  FieldType = "string"
	FieldTypeBool    FieldType = "bool"
)

// Schema schema 文件结构
type Schema struct {
	Fields map[string]FieldType `json:"fields"`
}

// Magic 魔数 "TSERPEG"
var Magic = [8]byte{0x54, 0x53, 0x45, 0x52, 0x50, 0x45, 0x47, 0x46}

// Version 版本
const Version = 1

// BlockSize 默认块大小 64KB
const BlockSize = 64 * 1024

// Writer SSTable 写入器
type Writer struct {
	shardDir   string
	seq        uint64
	dataDir    string
	timestamp  *os.File
	fields     map[string]*os.File
	schema     Schema
	blockIndex *BlockIndex

	// 块缓冲
	buf        []byte
	bufPos     int
	firstTs    int64
	rowCount   uint32

	// 每列的缓冲（用于收集一个 block 的数据）
	fieldBufs  map[string][]byte
	fieldSizes map[string]int  // 每行该字段的固定大小
}

// NewBlockIndex 创建空的 BlockIndex
func NewBlockIndex() *BlockIndex {
	return &BlockIndex{
		entries: make([]BlockIndexEntry, 0),
	}
}

// NewWriter 创建 Writer
func NewWriter(shardDir string, seq uint64) (*Writer, error) {
	// 使用 seq 创建独立的子目录，避免不同 SSTable 之间的冲突
	dataDir := filepath.Join(shardDir, "data", fmt.Sprintf("sst_%d", seq))
	if err := storage.SafeMkdirAll(dataDir, 0700); err != nil {
		return nil, err
	}

	fieldsDir := filepath.Join(dataDir, "fields")
	if err := storage.SafeMkdirAll(fieldsDir, 0700); err != nil {
		return nil, err
	}

	tsFile, err := storage.SafeCreate(filepath.Join(dataDir, "_timestamps.bin"), 0600)
	if err != nil {
		return nil, err
	}

	return &Writer{
		shardDir:   shardDir,
		seq:        seq,
		dataDir:    dataDir,
		timestamp:  tsFile,
		fields:     make(map[string]*os.File),
		schema:     Schema{Fields: make(map[string]FieldType)},
		blockIndex: NewBlockIndex(),
		buf:        make([]byte, BlockSize),
		bufPos:     0,
		rowCount:   0,
		fieldBufs:  make(map[string][]byte),
		fieldSizes: make(map[string]int),
	}, nil
}

// WritePoints 写入一批 points
func (w *Writer) WritePoints(points []*types.Point) error {
	// 收集所有字段名并检测类型
	fieldNames := make(map[string]bool)
	for _, p := range points {
		for name, val := range p.Fields {
			fieldNames[name] = true
			if _, exists := w.schema.Fields[name]; !exists {
				w.schema.Fields[name] = detectFieldType(val)
			}
		}
	}

	// 打开字段文件
	for name := range fieldNames {
		f, err := storage.SafeOpenFile(
			filepath.Join(w.dataDir, "fields", name+".bin"),
			os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return err
		}
		w.fields[name] = f

		// 初始化 field buffer 和计算固定大小
		w.fieldBufs[name] = make([]byte, 0, BlockSize)
		w.fieldSizes[name] = w.fieldTypeSize(w.schema.Fields[name])
	}

	// 写入 points 到 block buffer
	for _, p := range points {
		if err := w.writePoint(p); err != nil {
			return err
		}
	}

	return nil
}

// fieldTypeSize 返回字段类型的固定大小
func (w *Writer) fieldTypeSize(t FieldType) int {
	switch t {
	case FieldTypeFloat64, FieldTypeInt64:
		return 8
	case FieldTypeBool:
		return 1
	case FieldTypeString:
		return -1 // 变长
	default:
		return 8
	}
}

// writePoint 将单个 point 写入 block buffer
func (w *Writer) writePoint(p *types.Point) error {
	// 检查是否需要 flush block
	if w.bufPos >= BlockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	// 记录第一个时间戳
	if w.rowCount == 0 {
		w.firstTs = p.Timestamp
	}

	// 编码并写入 timestamp
	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(p.Timestamp))
	copy(w.buf[w.bufPos:w.bufPos+8], tsBuf[:])
	w.bufPos += 8

	// 写入各字段到各自的 buffer
	for name := range w.fields {
		val, ok := p.Fields[name]
		if !ok {
			// 字段不存在，写入零值
			val = w.zeroValue(w.schema.Fields[name])
		}
		w.appendFieldValue(name, val)
	}

	w.rowCount++
	return nil
}

// zeroValue 返回类型的零值
func (w *Writer) zeroValue(t FieldType) any {
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

// appendFieldValue 将字段值追加到 field buffer
func (w *Writer) appendFieldValue(name string, val any) {
	buf := w.fieldBufs[name]

	switch v := val.(type) {
	case float64:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], math.Float64bits(v))
		buf = append(buf, b[:]...)
	case int64:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(v))
		buf = append(buf, b[:]...)
	case string:
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(v)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, v...)
	case bool:
		if v {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}
	w.fieldBufs[name] = buf
}

// flushBlock 将当前 block 缓冲写入文件
func (w *Writer) flushBlock() error {
	if w.bufPos == 0 && w.rowCount == 0 {
		return nil
	}

	// 获取当前文件偏移量
	info, err := w.timestamp.Stat()
	if err != nil {
		return err
	}
	offset := uint32(info.Size())

	// 写入 timestamps block
	if _, err := w.timestamp.Write(w.buf[:w.bufPos]); err != nil {
		return err
	}

	// 写入各字段 block
	for name, buf := range w.fieldBufs {
		if _, err := w.fields[name].Write(buf); err != nil {
			return err
		}
		// 清空 buffer
		w.fieldBufs[name] = w.fieldBufs[name][:0]
	}

	// 记录 block 索引
	// 当前 block 的行数 = bufPos / 8 (每个 timestamp 8 字节)
	blockRowCount := uint32(w.bufPos / 8)
	lastTs := w.firstTs + int64(blockRowCount-1)*int64(time.Second)
	w.blockIndex.Add(w.firstTs, lastTs, offset, blockRowCount)

	// 重置
	w.bufPos = 0
	w.rowCount = 0
	w.firstTs = 0

	return nil
}

// Close 关闭
func (w *Writer) Close() error {
	if err := w.flushBlock(); err != nil {
		return err
	}
	if err := w.writeSchema(); err != nil {
		return err
	}
	if err := w.writeBlockIndex(); err != nil {
		return err
	}
	if w.timestamp != nil {
		if err := w.timestamp.Close(); err != nil {
			return err
		}
	}
	for _, f := range w.fields {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

// writeBlockIndex 写入 block index 文件
func (w *Writer) writeBlockIndex() error {
	indexFile := filepath.Join(w.dataDir, "_index.bin")
	return w.blockIndex.Write(indexFile)
}

// writeSchema 写入 schema 文件
func (w *Writer) writeSchema() error {
	schemaFile, err := storage.SafeCreate(filepath.Join(w.dataDir, "_schema.json"), 0600)
	if err != nil {
		return err
	}
	defer schemaFile.Close()

	data, err := json.Marshal(w.schema)
	if err != nil {
		return err
	}
	_, err = schemaFile.Write(data)
	return err
}

// detectFieldType 检测字段类型
func detectFieldType(val any) FieldType {
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
