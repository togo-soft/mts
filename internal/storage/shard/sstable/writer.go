// internal/storage/shard/sstable/writer.go
package sstable

import (
	"encoding/binary"
	"os"

	"micro-ts/internal/storage"
	"micro-ts/internal/storage/shard/compression"
)

// Magic 魔数 "TSERPEG"
var Magic = [8]byte{0x54, 0x53, 0x45, 0x52, 0x50, 0x45, 0x47, 0x46}

// Version 版本
const Version = 1

// BlockSize 默认块大小 64KB
const BlockSize = 64 * 1024

// Writer SSTable 写入器
type Writer struct {
	file *os.File
	path string
}

// NewWriter 创建 Writer
func NewWriter(path string) (*Writer, error) {
	f, err := storage.SafeCreate(path, 0600)
	if err != nil {
		return nil, err
	}

	return &Writer{
		file: f,
		path: path,
	}, nil
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

	_, err := w.file.Write(buf[:pos])
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
	return w.file.Close()
}
