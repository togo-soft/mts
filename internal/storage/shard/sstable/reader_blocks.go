package sstable

import (
	"encoding/binary"
)

// readTimestamps 从文件中读取全部 timestamps。
func (r *Reader) readTimestamps() ([]int64, error) {
	tsOffset, tsSize, _ := r.sectionTable.LookupByType(SectionTimestamps)
	if tsSize == 0 {
		return nil, nil
	}
	data := make([]byte, tsSize)
	if _, err := r.file.ReadAt(data, int64(tsOffset)); err != nil {
		return nil, err
	}
	return decodeTimestampBatch(data), nil
}

// readTimestampRange 读取指定偏移和行数的 timestamps。
// offset 和 numRows 来自 BlockIndexEntry（offset 是行号而不是字节偏移）。
func (r *Reader) readTimestampRange(offset uint32, numRows uint32) ([]int64, error) {
	tsSectionOffset, _, _ := r.sectionTable.LookupByType(SectionTimestamps)
	byteOffset := int64(tsSectionOffset) + int64(offset)*8
	bytesNeeded := int(numRows) * 8
	data := make([]byte, bytesNeeded)
	if _, err := r.file.ReadAt(data, byteOffset); err != nil {
		return nil, err
	}
	return decodeTimestampBatch(data), nil
}

// readSids 读取全部 sids。
func (r *Reader) readSids(expectedCount int) ([]uint64, error) {
	sidOffset, sidSize, _ := r.sectionTable.LookupByType(SectionSids)
	if sidSize == 0 {
		return make([]uint64, expectedCount), nil
	}
	data := make([]byte, sidSize)
	if _, err := r.file.ReadAt(data, int64(sidOffset)); err != nil {
		return nil, err
	}
	return decodeSidBatch(data), nil
}

// readSidsRange 读取指定偏移和行数的 sids。
func (r *Reader) readSidsRange(offset uint32, numRows uint32) ([]uint64, error) {
	sidSectionOffset, _, _ := r.sectionTable.LookupByType(SectionSids)
	byteOffset := int64(sidSectionOffset) + int64(offset)*8
	bytesNeeded := int(numRows) * 8
	data := make([]byte, bytesNeeded)
	if _, err := r.file.ReadAt(data, byteOffset); err != nil {
		return nil, err
	}
	return decodeSidBatch(data), nil
}

func decodeTimestampBatch(data []byte) []int64 {
	timestamps := make([]int64, 0, len(data)/8)
	for i := 0; i+8 <= len(data); i += 8 {
		ts := int64(binary.BigEndian.Uint64(data[i : i+8]))
		timestamps = append(timestamps, ts)
	}
	return timestamps
}

func decodeSidBatch(data []byte) []uint64 {
	sids := make([]uint64, 0, len(data)/8)
	for i := 0; i+8 <= len(data); i += 8 {
		sid := binary.BigEndian.Uint64(data[i : i+8])
		sids = append(sids, sid)
	}
	return sids
}
