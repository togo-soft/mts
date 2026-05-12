// Package wal 实现 Write-Ahead Log。
//
// Segment 文件格式:
//
//	Header (14B): Magic(4B) + Version(2B) + Flags(2B) + SegmentNum(4B) + Reserved(2B)
//	Record: CRC32(4B) + Type(1B) + Length(4B) + Payload(N) + Padding(0-7B)
//
// 文件命名: <generation>_<segment>.wal
//
//	generation: 16位 hex (Unix秒)
//	segment:    8位 hex (序号)
package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// 魔数 "D0C0A1FE" — 用于标识有效的 WAL 文件。
const magicNumber uint32 = 0xD0C0A1FE

// 当前格式版本。
const currentVersion uint16 = 1

// Segment Header Flags。
const (
	FlagNone       uint16 = 0x0000 // 无压缩
	FlagCompressed uint16 = 0x0001 // LZ4 压缩
)

// 记录类型。
const (
	TypePointData byte = 0x01 // Point 数据记录
	TypeMeta      byte = 0x02 // 元信息记录
	TypePad       byte = 0xFF // 填充记录
)

// segmentHeader 大小: 4 + 2 + 2 + 4 + 2 = 14 字节。
const segmentHeaderSize = 14

const recordHeaderSize = 9 // CRC32(4) + Type(1) + Length(4)

var ieeeTable = crc32.MakeTable(crc32.IEEE)

// ErrShortWrite 表示写入的字节数少于预期。
var ErrShortWrite = errors.New("short write")

// crc32Sum 计算 CRC32-IEEE。
func crc32Sum(data []byte) uint32 {
	return crc32.Checksum(data, ieeeTable)
}

// pad8 返回 8 字节对齐所需的 padding 字节数。
func pad8(length int) int {
	rem := length % 8
	if rem == 0 {
		return 0
	}
	return 8 - rem
}

// encodeSegmentHeader 编码 segment 文件头到 dst（14 字节）。
// flags: 压缩标志位，如 FlagCompressed。
func encodeSegmentHeader(dst []byte, segmentNum uint32, flags uint16) {
	binary.BigEndian.PutUint32(dst[0:4], magicNumber)
	binary.BigEndian.PutUint16(dst[4:6], currentVersion)
	binary.BigEndian.PutUint16(dst[6:8], flags)
	binary.BigEndian.PutUint32(dst[8:12], segmentNum)
	binary.BigEndian.PutUint16(dst[12:14], 0) // reserved
}

// decodeSegmentHeader 解码 segment 文件头。
// 返回 (version, segmentNum, flags, error)。
func decodeSegmentHeader(data []byte) (version uint16, segmentNum uint32, flags uint16, err error) {
	magic := binary.BigEndian.Uint32(data[0:4])
	if magic != magicNumber {
		return 0, 0, 0, &FormatError{Reason: "invalid magic number"}
	}
	version = binary.BigEndian.Uint16(data[4:6])
	flags = binary.BigEndian.Uint16(data[6:8])
	segmentNum = binary.BigEndian.Uint32(data[8:12])
	return version, segmentNum, flags, nil
}

// FormatError 表示格式错误。
type FormatError struct {
	Reason string
}

func (e *FormatError) Error() string {
	return "wal format error: " + e.Reason
}

// EncodeRecord 将 payload 编码为 WAL 记录。
// 返回完整记录: CRC32 + type + len + payload + padding。
func EncodeRecord(dst []byte, typ byte, payload []byte) []byte {
	bodyLen := 1 + 4 + len(payload) // type + len + payload
	padding := pad8(4 + bodyLen)    // CRC32 + body + padding
	totalLen := 4 + bodyLen + padding

	dst = dst[:0]
	dst = append(dst, 0, 0, 0, 0) // CRC32 placeholder

	dst = append(dst, typ)

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	dst = append(dst, lenBuf[:]...)

	dst = append(dst, payload...)

	for i := 0; i < padding; i++ {
		dst = append(dst, 0)
	}

	// 计算 CRC32（覆盖 type + len + payload，不含 padding）
	crcInput := dst[4 : 4+bodyLen] // 跳过 CRC32 placeholder
	crc := crc32Sum(crcInput)
	binary.BigEndian.PutUint32(dst[0:4], crc)

	return dst[:totalLen]
}

// RecordSize 返回编码后记录的字节数。
func RecordSize(payloadLen int) int {
	bodyLen := 1 + 4 + payloadLen
	return 4 + bodyLen + pad8(4+bodyLen)
}
