package sstable

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
)

// loadBlock 加载指定 block 的数据
func (it *Iterator) loadBlock(blockIdx int) error {
	if blockIdx < 0 || blockIdx >= len(it.blockIndex) {
		return nil
	}

	entry := it.blockIndex[blockIdx]
	it.currentBlock = blockIdx
	it.blockRowCount = int(entry.RowCount)

	tsFile, err := os.Open(filepath.Join(it.dataDir, "_timestamps.bin"))
	if err != nil {
		return err
	}
	defer func() { _ = tsFile.Close() }()

	if _, err := tsFile.Seek(int64(entry.Offset), io.SeekStart); err != nil {
		return err
	}

	it.blockTimestamps = make([]int64, entry.RowCount)
	for i := uint32(0); i < entry.RowCount; i++ {
		var buf [8]byte
		if _, err := tsFile.Read(buf[:]); err != nil {
			return err
		}
		it.blockTimestamps[i] = int64(binary.BigEndian.Uint64(buf[:]))
	}

	entries, err := os.ReadDir(filepath.Join(it.dataDir, "fields"))
	if err != nil {
		return err
	}

	it.fieldBufs = make(map[string][]byte)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()[:len(e.Name())-4]
		fieldType := it.reader.schema.Fields[name]

		var fieldSize int
		switch fieldType {
		case FieldTypeFloat64, FieldTypeInt64:
			fieldSize = 8
		case FieldTypeBool:
			fieldSize = 1
		case FieldTypeString:
			fieldSize = -1
		default:
			fieldSize = 8
		}

		if fieldSize > 0 {
			offset := int(entry.RowCount) * fieldSize * blockIdx
			data, err := it.readFieldBlock(filepath.Join(it.dataDir, "fields", e.Name()), offset, int(entry.RowCount)*fieldSize)
			if err != nil {
				return err
			}
			it.fieldBufs[name] = data
		} else {
			data, err := os.ReadFile(filepath.Join(it.dataDir, "fields", e.Name()))
			if err != nil {
				return err
			}
			it.fieldBufs[name] = data
		}
	}

	return nil
}

// readFieldBlock 读取指定偏移和大小的字段数据
func (it *Iterator) readFieldBlock(path string, offset, size int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	data := make([]byte, size)
	n, err := f.Read(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}
