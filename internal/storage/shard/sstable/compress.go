package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/golang/snappy"
	lz4 "github.com/pierrec/lz4/v4"
)

// crcTable 是 CRC32C (Castagnoli) 查找表，硬件加速友好。
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// CompressionAlgorithm 通用块压缩算法。
type CompressionAlgorithm uint8

const (
	CompressionNone   CompressionAlgorithm = 0 // 无压缩（默认）
	CompressionSnappy CompressionAlgorithm = 1 // Snappy 压缩
	CompressionLZ4    CompressionAlgorithm = 2 // LZ4 压缩
)

// String 返回算法名称。
func (c CompressionAlgorithm) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionSnappy:
		return "snappy"
	case CompressionLZ4:
		return "lz4"
	default:
		return fmt.Sprintf("unknown(%d)", c)
	}
}

// CompressBlock 压缩已编码的 block 数据并追加 CRC32C 校验和。
//
// 输出格式：
//   - CompressionNone: [raw_data][crc32:4B BigEndian]
//   - CompressionSnappy/LZ4: [uncompressedLen:4B BigEndian][compressed_data][crc32:4B BigEndian]
//
// CRC32C 覆盖整个 payload（不含末尾 4B CRC 自身）：
//   - 压缩块：CRC([uncompressedLen:4B] + [compressed_data])
//   - 无压缩：CRC([raw_data])
func CompressBlock(data []byte, algo CompressionAlgorithm) ([]byte, error) {
	switch algo {
	case CompressionNone:
		crc := crc32.Checksum(data, crcTable)
		result := make([]byte, len(data)+4)
		copy(result, data)
		binary.BigEndian.PutUint32(result[len(result)-4:], crc)
		return result, nil
	case CompressionSnappy:
		encoded := snappy.Encode(nil, data)
		payload := make([]byte, 4+len(encoded))
		binary.BigEndian.PutUint32(payload[:4], uint32(len(data)))
		copy(payload[4:], encoded)
		crc := crc32.Checksum(payload, crcTable)
		result := make([]byte, len(payload)+4)
		copy(result, payload)
		binary.BigEndian.PutUint32(result[len(result)-4:], crc)
		return result, nil
	case CompressionLZ4:
		buf := make([]byte, lz4.CompressBlockBound(len(data)))
		n, err := lz4.CompressBlock(data, buf, nil)
		if err != nil {
			return nil, fmt.Errorf("lz4 compress: %w", err)
		}
		payload := make([]byte, 4+n)
		binary.BigEndian.PutUint32(payload[:4], uint32(len(data)))
		copy(payload[4:], buf[:n])
		crc := crc32.Checksum(payload, crcTable)
		result := make([]byte, len(payload)+4)
		copy(result, payload)
		binary.BigEndian.PutUint32(result[len(result)-4:], crc)
		return result, nil
	default:
		return nil, fmt.Errorf("unknown compression algorithm: %d", algo)
	}
}

// DecompressBlock 解压 CompressBlock 压缩的数据，并验证 CRC32C 校验和。
//
// 返回解压后的原始数据；CRC 不匹配时返回错误。
func DecompressBlock(data []byte, algo CompressionAlgorithm) ([]byte, error) {
	switch algo {
	case CompressionNone:
		if len(data) < 4 {
			return nil, fmt.Errorf("uncompressed data too short for crc: %d bytes", len(data))
		}
		payload := data[:len(data)-4]
		expectedCRC := binary.BigEndian.Uint32(data[len(data)-4:])
		if actual := crc32.Checksum(payload, crcTable); actual != expectedCRC {
			return nil, fmt.Errorf("crc32c mismatch: expected %08x, got %08x", expectedCRC, actual)
		}
		return payload, nil
	case CompressionSnappy:
		if len(data) < 8 {
			return nil, fmt.Errorf("snappy data too short: %d bytes", len(data))
		}
		payload := data[:len(data)-4]
		expectedCRC := binary.BigEndian.Uint32(data[len(data)-4:])
		if actual := crc32.Checksum(payload, crcTable); actual != expectedCRC {
			return nil, fmt.Errorf("snappy crc32c mismatch: expected %08x, got %08x", expectedCRC, actual)
		}
		origLen := binary.BigEndian.Uint32(payload[:4])
		decoded, err := snappy.Decode(nil, payload[4:])
		if err != nil {
			return nil, fmt.Errorf("snappy decode: %w", err)
		}
		_ = origLen // snappy 自带长度验证
		return decoded, nil
	case CompressionLZ4:
		if len(data) < 8 {
			return nil, fmt.Errorf("lz4 data too short: %d bytes", len(data))
		}
		payload := data[:len(data)-4]
		expectedCRC := binary.BigEndian.Uint32(data[len(data)-4:])
		if actual := crc32.Checksum(payload, crcTable); actual != expectedCRC {
			return nil, fmt.Errorf("lz4 crc32c mismatch: expected %08x, got %08x", expectedCRC, actual)
		}
		origLen := binary.BigEndian.Uint32(payload[:4])
		decoded := make([]byte, origLen)
		n, err := lz4.UncompressBlock(payload[4:], decoded)
		if err != nil {
			return nil, fmt.Errorf("lz4 decode: %w", err)
		}
		return decoded[:n], nil
	default:
		return nil, fmt.Errorf("unknown compression algorithm: %d", algo)
	}
}
