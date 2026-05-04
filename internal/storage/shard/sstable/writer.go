// internal/storage/shard/sstable/writer.go
package sstable

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"

	"micro-ts/internal/storage"
	"micro-ts/internal/storage/shard/compression"
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
	buf        []byte
	bufPos     int
	firstTs    int64
	rowCount   uint32
}

// NewBlockIndex 创建空的 BlockIndex
func NewBlockIndex() *BlockIndex {
	return &BlockIndex{
		entries: make([]BlockIndexEntry, 0),
	}
}

// NewWriter 创建 Writer
func NewWriter(shardDir string, seq uint64) (*Writer, error) {
	dataDir := filepath.Join(shardDir, "data")
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
	}, nil
}

// WritePoints 写入一批 points
func (w *Writer) WritePoints(points []*types.Point) error {
	// 收集所有字段名并检测类型
	fieldNames := make(map[string]bool)
	for _, p := range points {
		for name, val := range p.Fields {
			fieldNames[name] = true
			// 检测字段类型（只检测一次）
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
	}

	// 写入 points 到 block buffer
	for _, p := range points {
		if err := w.writePoint(p); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) writeFieldValue(f *os.File, val any) error {
	switch v := val.(type) {
	case float64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(v))
		_, err := f.Write(buf[:])
		return err
	case int64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(v))
		_, err := f.Write(buf[:])
		return err
	case string:
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(len(v)))
		if _, err := f.Write(buf[:]); err != nil {
			return err
		}
		_, err := f.WriteString(v)
		return err
	case bool:
		if v {
			_, err := f.Write([]byte{1})
			return err
		}
		_, err := f.Write([]byte{0})
		return err
	}
	return nil
}

// writePoint 将单个 point 写入 block buffer
func (w *Writer) writePoint(p *types.Point) error {
	if w.bufPos >= BlockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	if w.rowCount == 0 {
		w.firstTs = p.Timestamp
	}
	// Write timestamp to buffer (for block indexing)
	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(p.Timestamp))
	copy(w.buf[w.bufPos:w.bufPos+8], tsBuf[:])
	w.bufPos += 8
	// Write fields directly to field files (maintain original structure)
	for name, f := range w.fields {
		if val, ok := p.Fields[name]; ok {
			if err := w.writeFieldValue(f, val); err != nil {
				return err
			}
		}
	}
	w.rowCount++
	return nil
}

// writeFieldValueToBuf 将字段值写入缓冲区
func (w *Writer) writeFieldValueToBuf(val any) error {
	switch v := val.(type) {
	case float64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(v))
		copy(w.buf[w.bufPos:w.bufPos+8], buf[:])
		w.bufPos += 8
		return nil
	case int64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(v))
		copy(w.buf[w.bufPos:w.bufPos+8], buf[:])
		w.bufPos += 8
		return nil
	case string:
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(v)))
		copy(w.buf[w.bufPos:w.bufPos+4], lenBuf[:])
		w.bufPos += 4
		copy(w.buf[w.bufPos:w.bufPos+len(v)], v)
		w.bufPos += len(v)
		return nil
	case bool:
		if v {
			w.buf[w.bufPos] = 1
		} else {
			w.buf[w.bufPos] = 0
		}
		w.bufPos++
		return nil
	}
	return nil
}

// flushBlock 将当前 block 缓冲写入文件
func (w *Writer) flushBlock() error {
	if w.bufPos == 0 {
		return nil
	}
	info, err := w.timestamp.Stat()
	if err != nil {
		return err
	}
	offset := uint32(info.Size())
	if _, err := w.timestamp.Write(w.buf[:w.bufPos]); err != nil {
		return err
	}
	lastTs := w.firstTs + int64(w.rowCount-1)*1000
	w.blockIndex.Add(w.firstTs, lastTs, offset, w.rowCount)
	w.bufPos = 0
	w.rowCount = 0
	w.firstTs = 0
	return nil
}

// WriteTimestampBlock 写入时间戳块
func (w *Writer) WriteTimestampBlock(timestamps []int64) error {
	// 计算 delta 编码
	deltas := compression.DeltaEncode(timestamps)

	// 分配缓冲区
	buf := make([]byte, BlockSize)
	pos := 0

	// 写入 header
	copy(buf[pos:pos+8], Magic[:])
	pos += 8
	binary.BigEndian.PutUint32(buf[pos:pos+4], Version)
	pos += 4
	// block_count 后续填写
	blockCountOffset := pos
	pos += 4

	// 计算压缩数据
	encoded := w.encodeDeltas(deltas)

	// 写入数据
	binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(len(encoded)))
	pos += 4
	copy(buf[pos:], encoded)
	pos += len(encoded)

	// 回填 block_count (当前为 1)
	binary.BigEndian.PutUint32(buf[blockCountOffset:blockCountOffset+4], 1)

	_, err := w.timestamp.Write(buf[:pos])
	return err
}

func (w *Writer) encodeDeltas(deltas []int64) []byte {
	// 简单实现：直接用 varint 编码
	buf := make([]byte, len(deltas)*16)
	pos := 0
	for _, d := range deltas {
		n := compression.PutVarint(buf[pos:], uint64(d))
		pos += n
	}
	return buf[:pos]
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
	return FieldTypeFloat64 // 默认
}
