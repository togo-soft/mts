package sstable

import (
	"fmt"
	"os"
	"path/filepath"

	"codeberg.org/micro-ts/mts/internal/storage"
)

// FieldType 字段类型。
type FieldType string

const (
	FieldTypeFloat64 FieldType = "float64"
	FieldTypeInt64   FieldType = "int64"
	FieldTypeString  FieldType = "string"
	FieldTypeBool    FieldType = "bool"
)

// Schema 描述 SSTable 的字段结构。
type Schema struct {
	Fields map[string]FieldType `json:"fields"`
}

// BlockSize 默认块大小 64KB。
const BlockSize = 64 * 1024

// Writer SSTable 写入器。
type Writer struct {
	shardDir   string
	seq        uint64
	blockSize  int
	dataDir    string
	tmpDir     string // 临时目录，存放各段的中间文件
	timestamp  *os.File
	sids       *os.File
	fields     map[string]*os.File
	schema     Schema
	blockIndex *BlockIndex

	buf       []byte
	bufPos    int
	firstTs   int64
	rowCount  uint32
	totalRows uint32

	sidBuf           []uint64
	fieldBufs        map[string][]byte
	fieldSizes       map[string]int
	fieldByteOffsets map[string][]int64 // 每个 block 在各 field temp 文件中的字节起始偏移

	compressAlgo CompressionAlgorithm
}

// NewWriter 创建 SSTable Writer。
// 在 shardDir/data/ 下创建 sst_{seq}.bin 单文件。
func NewWriter(shardDir string, seq uint64, blockSize int, compressAlgo CompressionAlgorithm) (*Writer, error) {
	if blockSize <= 0 {
		blockSize = BlockSize
	}

	dataDir := filepath.Join(shardDir, "data")
	if err := storage.SafeMkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	// 使用临时目录存储各段中间数据，Close 时合并到单文件
	tmpDir := filepath.Join(dataDir, fmt.Sprintf(".sst_%d_tmp", seq))
	if err := storage.SafeMkdirAll(tmpDir, 0700); err != nil {
		return nil, fmt.Errorf("create tmp dir: %w", err)
	}

	tsFile, err := storage.SafeCreate(filepath.Join(tmpDir, "_timestamps.bin"), 0600)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("create timestamp temp file: %w", err)
	}

	sidFile, err := storage.SafeCreate(filepath.Join(tmpDir, "_sids.bin"), 0600)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("create sids temp file: %w", err)
	}

	w := &Writer{
		shardDir:         shardDir,
		seq:              seq,
		blockSize:        blockSize,
		dataDir:          dataDir,
		tmpDir:           tmpDir,
		timestamp:        tsFile,
		sids:             sidFile,
		fields:           make(map[string]*os.File),
		schema:           Schema{Fields: make(map[string]FieldType)},
		blockIndex:       NewBlockIndex(),
		buf:              make([]byte, blockSize),
		bufPos:           0,
		rowCount:         0,
		fieldBufs:        make(map[string][]byte),
		fieldSizes:       make(map[string]int),
		fieldByteOffsets: make(map[string][]int64),
		compressAlgo:     compressAlgo,
	}

	return w, nil
}

// NewBlockIndex 创建空的 BlockIndex。
func NewBlockIndex() *BlockIndex {
	return &BlockIndex{
		entries: make([]BlockIndexEntry, 0),
	}
}

// Schema 返回写入过程中检测到的 schema。
func (w *Writer) Schema() Schema {
	return w.schema
}
