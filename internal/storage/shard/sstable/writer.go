// Package sstable 实现 SSTable（Sorted String Table）存储格式。
package sstable

import (
	"fmt"
	"os"
	"path/filepath"

	"codeberg.org/micro-ts/mts/internal/storage"
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
	blockSize  int
	dataDir    string
	timestamp  *os.File
	sids       *os.File
	fields     map[string]*os.File
	schema     Schema
	blockIndex *BlockIndex

	buf      []byte
	bufPos   int
	firstTs  int64
	rowCount uint32

	sidBuf     []uint64
	fieldBufs  map[string][]byte
	fieldSizes map[string]int
}

// NewBlockIndex 创建空的 BlockIndex
func NewBlockIndex() *BlockIndex {
	return &BlockIndex{
		entries: make([]BlockIndexEntry, 0),
	}
}

// NewWriter 创建 SSTable Writer。
func NewWriter(shardDir string, seq uint64, blockSize int) (*Writer, error) {
	if blockSize <= 0 {
		blockSize = BlockSize
	}

	dataDir := filepath.Join(shardDir, "data", fmt.Sprintf("sst_%d", seq))
	if err := storage.SafeMkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	fieldsDir := filepath.Join(dataDir, "fields")
	if err := storage.SafeMkdirAll(fieldsDir, 0700); err != nil {
		return nil, fmt.Errorf("create fields dir: %w", err)
	}

	tsFile, err := storage.SafeCreate(filepath.Join(dataDir, "_timestamps.bin"), 0600)
	if err != nil {
		return nil, fmt.Errorf("create timestamp file: %w", err)
	}

	sidFile, err := storage.SafeCreate(filepath.Join(dataDir, "_sids.bin"), 0600)
	if err != nil {
		return nil, fmt.Errorf("create sids file: %w", err)
	}

	return &Writer{
		shardDir:   shardDir,
		seq:        seq,
		blockSize:  blockSize,
		dataDir:    dataDir,
		timestamp:  tsFile,
		sids:       sidFile,
		fields:     make(map[string]*os.File),
		schema:     Schema{Fields: make(map[string]FieldType)},
		blockIndex: NewBlockIndex(),
		buf:        make([]byte, blockSize),
		bufPos:     0,
		rowCount:   0,
		fieldBufs:  make(map[string][]byte),
		fieldSizes: make(map[string]int),
	}, nil
}
