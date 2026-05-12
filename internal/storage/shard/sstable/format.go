package sstable

import (
	"encoding/binary"
	"fmt"
)

// Magic 是 SSTable 文件的魔数 "TSERSTBL"。
var Magic = [8]byte{0x54, 0x53, 0x45, 0x52, 0x53, 0x54, 0x42, 0x4C}

// FileVersion 单文件 SSTable 格式版本。
const FileVersion uint32 = 2

// HeaderSize 文件头固定大小 (64 字节)。
const HeaderSize = 64

// SectionType 段类型。
type SectionType uint8

const (
	SectionTimestamps SectionType = 0
	SectionSids       SectionType = 1
	SectionIndex      SectionType = 2
	SectionField      SectionType = 3
)

// FileHeader 是单文件 SSTable 的文件头 (64 字节)。
type FileHeader struct {
	Magic              [8]byte // "TSERSTBL"
	Version            uint32
	RowCount           uint32
	FieldCount         uint16
	BlockCount         uint16
	BlockSize          uint16
	_                  uint16 // padding
	TimestampsOffset   uint64
	SidsOffset         uint64
	BlockIndexOffset   uint64
	SectionTableOffset uint64
	_                  [8]byte // reserved
}

// Marshal 将 FileHeader 序列化为 64 字节。
func (h *FileHeader) Marshal() [HeaderSize]byte {
	var buf [HeaderSize]byte
	copy(buf[0:8], h.Magic[:])
	binary.BigEndian.PutUint32(buf[8:12], h.Version)
	binary.BigEndian.PutUint32(buf[12:16], h.RowCount)
	binary.BigEndian.PutUint16(buf[16:18], h.FieldCount)
	binary.BigEndian.PutUint16(buf[18:20], h.BlockCount)
	binary.BigEndian.PutUint16(buf[20:22], h.BlockSize)
	binary.BigEndian.PutUint64(buf[24:32], h.TimestampsOffset)
	binary.BigEndian.PutUint64(buf[32:40], h.SidsOffset)
	binary.BigEndian.PutUint64(buf[40:48], h.BlockIndexOffset)
	binary.BigEndian.PutUint64(buf[48:56], h.SectionTableOffset)
	return buf
}

// UnmarshalFileHeader 从 64 字节反序列化 FileHeader。
func UnmarshalFileHeader(data [HeaderSize]byte) (FileHeader, error) {
	var h FileHeader
	copy(h.Magic[:], data[0:8])
	if h.Magic != Magic {
		return h, fmt.Errorf("invalid magic: expected %q, got %q", Magic, h.Magic)
	}
	h.Version = binary.BigEndian.Uint32(data[8:12])
	if h.Version != FileVersion {
		return h, fmt.Errorf("unsupported version: %d (expected %d)", h.Version, FileVersion)
	}
	h.RowCount = binary.BigEndian.Uint32(data[12:16])
	h.FieldCount = binary.BigEndian.Uint16(data[16:18])
	h.BlockCount = binary.BigEndian.Uint16(data[18:20])
	h.BlockSize = binary.BigEndian.Uint16(data[20:22])
	h.TimestampsOffset = binary.BigEndian.Uint64(data[24:32])
	h.SidsOffset = binary.BigEndian.Uint64(data[32:40])
	h.BlockIndexOffset = binary.BigEndian.Uint64(data[40:48])
	h.SectionTableOffset = binary.BigEndian.Uint64(data[48:56])
	return h, nil
}

// SectionEntry 描述文件中一个段的元数据。
type SectionEntry struct {
	Type        SectionType
	Name        string // 仅字段段有值
	Offset      uint64
	Size        uint64
	Encoding    EncodingType
	Compression CompressionAlgorithm
}

// SectionTable 是文件末尾的段目录。
type SectionTable struct {
	Entries []SectionEntry
}

// sectionEntrySize 每个 entry 的固定开销 (Type+Encoding+Compression+NameLen+Offset+Size)。
const sectionEntrySize = 20

// Marshal 序列化 SectionTable。
// 格式: [count:2B][reserved:2B][entries...]
// 每个 entry: [type:1B][encoding:1B][compression:1B][nameLen:1B][offset:8B][size:8B][name:nameLen]
func (st *SectionTable) Marshal() []byte {
	nameLenSum := 0
	for _, e := range st.Entries {
		nameLenSum += len(e.Name)
	}
	buf := make([]byte, 0, 4+len(st.Entries)*sectionEntrySize+nameLenSum)

	var count [2]byte
	binary.BigEndian.PutUint16(count[:], uint16(len(st.Entries)))
	buf = append(buf, count[:]...)
	buf = append(buf, 0, 0) // reserved

	for _, e := range st.Entries {
		buf = append(buf, byte(e.Type), byte(e.Encoding), byte(e.Compression), byte(len(e.Name)))
		var off [8]byte
		binary.BigEndian.PutUint64(off[:], e.Offset)
		buf = append(buf, off[:]...)
		binary.BigEndian.PutUint64(off[:], e.Size)
		buf = append(buf, off[:]...)
		if len(e.Name) > 0 {
			buf = append(buf, e.Name...)
		}
	}
	return buf
}

// UnmarshalSectionTable 反序列化 SectionTable。
func UnmarshalSectionTable(data []byte) (SectionTable, error) {
	if len(data) < 4 {
		return SectionTable{}, fmt.Errorf("section table too short: %d bytes", len(data))
	}
	count := int(binary.BigEndian.Uint16(data[0:2]))
	st := SectionTable{Entries: make([]SectionEntry, 0, count)}
	pos := 4 // skip count + reserved

	for i := 0; i < count; i++ {
		if pos+sectionEntrySize > len(data) {
			return SectionTable{}, fmt.Errorf("section table truncated at entry %d", i)
		}
		e := SectionEntry{
			Type:        SectionType(data[pos]),
			Encoding:    EncodingType(data[pos+1]),
			Compression: CompressionAlgorithm(data[pos+2]),
			Name:        "",
			Offset:      binary.BigEndian.Uint64(data[pos+4 : pos+12]),
			Size:        binary.BigEndian.Uint64(data[pos+12 : pos+20]),
		}
		nameLen := int(data[pos+3])
		pos += sectionEntrySize
		if nameLen > 0 {
			if pos+nameLen > len(data) {
				return SectionTable{}, fmt.Errorf("section table name truncated at entry %d", i)
			}
			e.Name = string(data[pos : pos+nameLen])
			pos += nameLen
		}
		st.Entries = append(st.Entries, e)
	}
	return st, nil
}

// Lookup 按名称查找段，返回 offset 和 size。未找到返回 0,0。
func (st *SectionTable) Lookup(name string) (offset, size uint64) {
	for _, e := range st.Entries {
		if e.Name == name {
			return e.Offset, e.Size
		}
	}
	return 0, 0
}

// LookupByType 按类型查找段。
func (st *SectionTable) LookupByType(typ SectionType) (offset, size uint64, name string) {
	for _, e := range st.Entries {
		if e.Type == typ {
			return e.Offset, e.Size, e.Name
		}
	}
	return 0, 0, ""
}

// FieldNames 返回所有字段段名称。
func (st *SectionTable) FieldNames() []string {
	names := make([]string, 0)
	for _, e := range st.Entries {
		if e.Type == SectionField {
			names = append(names, e.Name)
		}
	}
	return names
}

// LookupEncoding 按名称查找段编码。未找到返回 EncodingRaw。
func (st *SectionTable) LookupEncoding(name string) EncodingType {
	for _, e := range st.Entries {
		if e.Name == name {
			return e.Encoding
		}
	}
	return EncodingRaw
}

// LookupCompression 按名称查找段压缩算法。未找到返回 CompressionNone。
func (st *SectionTable) LookupCompression(name string) CompressionAlgorithm {
	for _, e := range st.Entries {
		if e.Name == name {
			return e.Compression
		}
	}
	return CompressionNone
}
