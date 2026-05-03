// internal/storage/shard/sstable/writer.go
package sstable

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"

	"micro-ts/internal/storage"
	"micro-ts/internal/storage/shard/compression"
	"micro-ts/internal/types"
)

// Magic 魔数 "TSERPEG"
var Magic = [8]byte{0x54, 0x53, 0x45, 0x52, 0x50, 0x45, 0x47, 0x46}

// Version 版本
const Version = 1

// BlockSize 默认块大小 64KB
const BlockSize = 64 * 1024

// Writer SSTable 写入器
type Writer struct {
	shardDir  string
	seq       uint64
	dataDir   string
	timestamp *os.File
	fields    map[string]*os.File
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
		shardDir:  shardDir,
		seq:       seq,
		dataDir:   dataDir,
		timestamp: tsFile,
		fields:    make(map[string]*os.File),
	}, nil
}

// WritePoints 写入一批 points
func (w *Writer) WritePoints(points []*types.Point) error {
	// 收集所有字段名
	fieldNames := make(map[string]bool)
	for _, p := range points {
		for name := range p.Fields {
			fieldNames[name] = true
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

	// 写入 timestamp
	for _, p := range points {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(p.Timestamp))
		if _, err := w.timestamp.Write(buf[:]); err != nil {
			return err
		}
	}

	// 写入各字段
	for name, f := range w.fields {
		for _, p := range points {
			if val, ok := p.Fields[name]; ok {
				if err := w.writeFieldValue(f, val); err != nil {
					return err
				}
			}
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
