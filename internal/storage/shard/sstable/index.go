// internal/storage/shard/sstable/index.go
package sstable

import (
	"encoding/binary"
	"os"
	"sort"

	"codeberg.org/micro-ts/mts/internal/storage"
)

// IndexMagic 是索引文件的魔数。
//
// 值："TSIDX001"
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

// BlockIndex 是 SSTable 的数据块索引。
//
// 为每个数据块维护索引信息，支持快速定位时间范围对应的数据块。
//
// 索引条目：
//
//	FirstTimestamp:  Block 中第一个时间戳
//	LastTimestamp:   Block 中最后一个时间戳
//	Offset:          在 timestamps.bin 中的偏移量
//	RowCount:        Block 中的行数
//
// 查询优化：
//
//	使用二分查找（FindBlock）定位目标 Block。
//	避免扫描整个文件。
//
// 文件格式：
//
//	[magic:8][version:4][count:4][entries...]
//	每个 entry 24 字节。
type BlockIndex struct {
	entries []BlockIndexEntry
}

// ErrInvalidIndex 无效索引错误
var ErrInvalidIndex = &IndexError{msg: "invalid index file"}

// IndexError 表示索引操作错误。
//
// 包含具体的错误信息。
type IndexError struct {
	msg string
}

func (e *IndexError) Error() string {
	return e.msg
}

// Write 将索引写入文件。
//
// 参数：
//   - file: 目标文件路径
//
// 返回：
//   - error: 写入失败时返回错误
//
// 文件格式：
//
//	[magic:8][version:4][count:4][entries...]
//	每个 entry 24 字节
//
// 错误处理：
//
//	如果写入失败，可能产生不完整的文件。
//	调用方应考虑原子写入或删除不完整文件。
func (idx *BlockIndex) Write(file string) error {
	f, err := storage.SafeCreate(file, 0600)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

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

// Read 从文件读取索引。
//
// 参数：
//   - file: 源文件路径
//
// 返回：
//   - error: 读取失败时返回错误
//
// 验证：
//
//	检查文件大小、魔数和版本。
//	任何验证失败都返回 ErrInvalidIndex。
func (idx *BlockIndex) Read(file string) error {
	data, err := storage.SafeOpenFile(file, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer func() {
		_ = data.Close()
	}()

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

// FindBlock 二分查找第一个包含或晚于目标时间的 Block。
//
// 参数：
//   - target: 目标时间戳
//
// 返回：
//   - int: Block 索引，如果不存在返回 len(entries)
//
// 查找逻辑：
//
//	查找第一个 LastTimestamp >= target 的 Block。
//	因为 Block 按时间排序且互不重叠，这一定位是有效的。
//
// 使用示例：
//
//	idx := blockIndex.FindBlock(startTime)
//	for i := idx; i < blockIndex.Len(); i++ {
//	    entry := blockIndex.Entry(i)
//	    if entry.FirstTimestamp > endTime {
//	        break // 超出范围
//	    }
//	    // 读取此 block...
//	}
func (idx *BlockIndex) FindBlock(target int64) int {
	return sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].LastTimestamp >= target
	})
}

// Len 返回索引条目数量。
//
// 返回：
//   - int: Block 数量
func (idx *BlockIndex) Len() int {
	return len(idx.entries)
}

// Entry 返回指定索引的 Block 条目。
//
// 参数：
//   - i: Block 索引
//
// 返回：
//   - BlockIndexEntry: Block 信息
//
// panic：
//
//	如果 i < 0 或 i >= Len()，会导致 panic。
func (idx *BlockIndex) Entry(i int) BlockIndexEntry {
	return idx.entries[i]
}

// Add 添加一个 Block 的索引条目。
//
// 参数：
//   - firstTs:  Block 起始时间
//   - lastTs:   Block 结束时间
//   - offset:   Block 在文件中的偏移量
//   - rowCount: Block 行数
//
// 注意：
//
//	应该按时间顺序调用 Add，添加的 Block 时间不应重叠。
func (idx *BlockIndex) Add(firstTs, lastTs int64, offset uint32, rowCount uint32) {
	idx.entries = append(idx.entries, BlockIndexEntry{
		FirstTimestamp: firstTs,
		LastTimestamp:  lastTs,
		Offset:         offset,
		RowCount:       rowCount,
	})
}
