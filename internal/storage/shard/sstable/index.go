// internal/storage/shard/sstable/index.go
package sstable

import (
	"encoding/binary"
	"os"
	"sort"

	"micro-ts/internal/storage"
)

// IndexMagic 索引文件魔数 "TSIDX001"
var IndexMagic = [8]byte{0x54, 0x53, 0x49, 0x44, 0x58, 0x30, 0x30, 0x31}

// IndexVersion 索引版本
const IndexVersion = 1

// BlockIndexEntry 单个 block 的索引条目
type BlockIndexEntry struct {
	FirstTimestamp int64  // block 内第一个时间戳
	LastTimestamp  int64  // block 内最后一个时间戳
	Offset         uint32 // block 在 timestamps.bin 文件中的偏移
	RowCount       uint32 // 该 block 的行数
}

// BlockIndex 索引管理器
type BlockIndex struct {
	entries []BlockIndexEntry
}

// ErrInvalidIndex 无效索引错误
var ErrInvalidIndex = &IndexError{msg: "invalid index file"}

type IndexError struct {
	msg string
}

func (e *IndexError) Error() string {
	return e.msg
}

// Write 写入索引到指定文件
func (idx *BlockIndex) Write(file string) error {
	f, err := storage.SafeCreate(file, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	// 写入 header
	var header [16]byte
	copy(header[0:8], IndexMagic[:])
	binary.BigEndian.PutUint32(header[8:12], IndexVersion)
	binary.BigEndian.PutUint32(header[12:16], uint32(len(idx.entries)))
	if _, err := f.Write(header[:]); err != nil {
		return err
	}

	// 写入 entries
	for _, e := range idx.entries {
		var buf [24]byte
		binary.BigEndian.PutUint64(buf[0:8], uint64(e.FirstTimestamp))
		binary.BigEndian.PutUint64(buf[8:16], uint64(e.LastTimestamp))
		binary.BigEndian.PutUint32(buf[16:20], e.Offset)
		binary.BigEndian.PutUint32(buf[20:24], e.RowCount)
		if _, err := f.Write(buf[:]); err != nil {
			return err
		}
	}

	return nil
}

// Read 从指定文件读取索引
func (idx *BlockIndex) Read(file string) error {
	data, err := storage.SafeOpenFile(file, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer data.Close()

	stat, err := data.Stat()
	if err != nil {
		return err
	}

	fileSize := stat.Size()
	if fileSize < 16 {
		return ErrInvalidIndex
	}

	buf := make([]byte, fileSize)
	n, err := data.Read(buf)
	if err != nil {
		return err
	}
	if int64(n) != fileSize {
		return ErrInvalidIndex
	}

	// 验证 magic
	if string(buf[0:8]) != string(IndexMagic[:]) {
		return ErrInvalidIndex
	}

	// 解析 header
	version := binary.BigEndian.Uint32(buf[8:12])
	if version != IndexVersion {
		return ErrInvalidIndex
	}

	blockCount := binary.BigEndian.Uint32(buf[12:16])
	entrySize := 24
	if 16+int(blockCount)*entrySize > len(buf) {
		return ErrInvalidIndex
	}

	// 解析 entries
	idx.entries = make([]BlockIndexEntry, blockCount)
	for i := uint32(0); i < blockCount; i++ {
		off := 16 + int(i)*entrySize
		idx.entries[i] = BlockIndexEntry{
			FirstTimestamp: int64(binary.BigEndian.Uint64(buf[off : off+8])),
			LastTimestamp:  int64(binary.BigEndian.Uint64(buf[off+8 : off+16])),
			Offset:         binary.BigEndian.Uint32(buf[off+16 : off+20]),
			RowCount:       binary.BigEndian.Uint32(buf[off+20 : off+24]),
		}
	}

	return nil
}

// FindBlock 二分查找第一个 last_timestamp >= target 的 block
// 返回 block 索引，如果不存在返回 len(entries)
func (idx *BlockIndex) FindBlock(target int64) int {
	return sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].LastTimestamp >= target
	})
}

// Len 返回 entry 数量
func (idx *BlockIndex) Len() int {
	return len(idx.entries)
}

// Entry 返回指定索引的 entry
func (idx *BlockIndex) Entry(i int) BlockIndexEntry {
	return idx.entries[i]
}

// Add 添加一个 block 的索引
func (idx *BlockIndex) Add(firstTs, lastTs int64, offset uint32, rowCount uint32) {
	idx.entries = append(idx.entries, BlockIndexEntry{
		FirstTimestamp: firstTs,
		LastTimestamp:  lastTs,
		Offset:         offset,
		RowCount:       rowCount,
	})
}
