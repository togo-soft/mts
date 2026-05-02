// internal/storage/shard/sstable/reader.go
package sstable

import (
	"os"
)

// Reader SSTable 读取器
type Reader struct {
	file *os.File
	path string
}

// NewReader 创建 Reader
func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{file: f, path: path}, nil
}

// Close 关闭
func (r *Reader) Close() error {
	return r.file.Close()
}
