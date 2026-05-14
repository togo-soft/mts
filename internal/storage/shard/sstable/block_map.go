package sstable

import (
	"encoding/binary"
	"fmt"
)

// BlockSectionMap 记录每个 section 内各 block 的字节偏移，支持按 block 粒度定位数据。
// 通过 _block_map section 持久化到文件。
type BlockSectionMap struct {
	Sections []BlockSectionOffsets
}

// BlockSectionOffsets 是单个 section 内各 block 的字节偏移。
//
// Offsets 长度为 blockCount+1，最后一个元素为 sentinel（section 总字节数），
// 方便计算最后一个 block 的大小：size = Offsets[i+1] - Offsets[i]。
type BlockSectionOffsets struct {
	Name    string
	Offsets []uint64
}

// BlockCount 返回 block 数量。
func (bso *BlockSectionOffsets) BlockCount() int {
	if len(bso.Offsets) == 0 {
		return 0
	}
	return len(bso.Offsets) - 1
}

// BlockRange 返回第 blockIdx 个 block 的 (offset, size)，blockIdx 从 0 开始。
func (bso *BlockSectionOffsets) BlockRange(blockIdx int) (offset, size uint64) {
	if blockIdx < 0 || blockIdx >= bso.BlockCount() {
		return 0, 0
	}
	offset = bso.Offsets[blockIdx]
	size = bso.Offsets[blockIdx+1] - offset
	return
}

// Marshal 序列化 BlockSectionMap。
//
// 格式:
//
//	[section_count:2B]
//	For each section:
//	  [name_len:1B][name:variable]
//	  [block_count:4B]
//	  [offsets: (block_count+1)*8B]
func (m *BlockSectionMap) Marshal() []byte {
	size := 2 // section_count
	for _, s := range m.Sections {
		size += 1 + len(s.Name) + 4 + len(s.Offsets)*8
	}
	buf := make([]byte, 0, size)

	var count [2]byte
	binary.BigEndian.PutUint16(count[:], uint16(len(m.Sections)))
	buf = append(buf, count[:]...)

	for _, s := range m.Sections {
		buf = append(buf, byte(len(s.Name)))
		buf = append(buf, s.Name...)

		var blockCount [4]byte
		cnt := s.BlockCount()
		binary.BigEndian.PutUint32(blockCount[:], uint32(cnt))
		buf = append(buf, blockCount[:]...)

		for _, off := range s.Offsets {
			var offBuf [8]byte
			binary.BigEndian.PutUint64(offBuf[:], off)
			buf = append(buf, offBuf[:]...)
		}
	}
	return buf
}

// UnmarshalBlockSectionMap 从字节反序列化。
func UnmarshalBlockSectionMap(data []byte) (*BlockSectionMap, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("block section map too short: %d bytes", len(data))
	}
	sectionCount := int(binary.BigEndian.Uint16(data[0:2]))
	pos := 2

	sections := make([]BlockSectionOffsets, 0, sectionCount)
	for i := range sectionCount {
		if pos+1 > len(data) {
			return nil, fmt.Errorf("block section map truncated at section %d header", i)
		}
		nameLen := int(data[pos])
		pos++
		if pos+nameLen > len(data) {
			return nil, fmt.Errorf("block section map truncated at section %d name", i)
		}
		name := string(data[pos : pos+nameLen])
		pos += nameLen

		if pos+4 > len(data) {
			return nil, fmt.Errorf("block section map truncated at section %d block count", i)
		}
		blockCount := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4

		offsetCount := blockCount + 1
		if pos+offsetCount*8 > len(data) {
			return nil, fmt.Errorf("block section map truncated at section %d offsets (need %d bytes)",
				i, offsetCount*8)
		}
		offsets := make([]uint64, offsetCount)
		for j := range offsetCount {
			offsets[j] = binary.BigEndian.Uint64(data[pos : pos+8])
			pos += 8
		}
		sections = append(sections, BlockSectionOffsets{
			Name:    name,
			Offsets: offsets,
		})
	}

	return &BlockSectionMap{Sections: sections}, nil
}

// Lookup 按 section 名称查找。
func (m *BlockSectionMap) Lookup(name string) *BlockSectionOffsets {
	for i := range m.Sections {
		if m.Sections[i].Name == name {
			return &m.Sections[i]
		}
	}
	return nil
}
