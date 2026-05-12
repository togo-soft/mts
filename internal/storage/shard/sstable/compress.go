package sstable

import (
	"encoding/binary"
	"fmt"

	"github.com/golang/snappy"
	lz4 "github.com/pierrec/lz4/v4"
)

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

// CompressBlock 压缩已编码的 block 数据。
// 压缩后格式: [uncompressedLen:4B BigEndian][compressed_data]
// 对于 CompressionNone，直接返回原始数据（无 header）。
func CompressBlock(data []byte, algo CompressionAlgorithm) ([]byte, error) {
	switch algo {
	case CompressionNone:
		return data, nil
	case CompressionSnappy:
		encoded := snappy.Encode(nil, data)
		result := make([]byte, 4+len(encoded))
		binary.BigEndian.PutUint32(result[:4], uint32(len(data)))
		copy(result[4:], encoded)
		return result, nil
	case CompressionLZ4:
		buf := make([]byte, lz4.CompressBlockBound(len(data)))
		n, err := lz4.CompressBlock(data, buf, nil)
		if err != nil {
			return nil, fmt.Errorf("lz4 compress: %w", err)
		}
		result := make([]byte, 4+n)
		binary.BigEndian.PutUint32(result[:4], uint32(len(data)))
		copy(result[4:], buf[:n])
		return result, nil
	default:
		return nil, fmt.Errorf("unknown compression algorithm: %d", algo)
	}
}

// DecompressBlock 解压 CompressBlock 压缩的数据。
// algo=CompressionNone 时直接返回原始数据。
func DecompressBlock(data []byte, algo CompressionAlgorithm) ([]byte, error) {
	switch algo {
	case CompressionNone:
		return data, nil
	case CompressionSnappy:
		if len(data) < 4 {
			return nil, fmt.Errorf("snappy data too short: %d bytes", len(data))
		}
		origLen := binary.BigEndian.Uint32(data[:4])
		decoded, err := snappy.Decode(nil, data[4:])
		if err != nil {
			return nil, fmt.Errorf("snappy decode: %w", err)
		}
		_ = origLen // snappy 自带长度验证
		return decoded, nil
	case CompressionLZ4:
		if len(data) < 4 {
			return nil, fmt.Errorf("lz4 data too short: %d bytes", len(data))
		}
		origLen := binary.BigEndian.Uint32(data[:4])
		decoded := make([]byte, origLen)
		n, err := lz4.UncompressBlock(data[4:], decoded)
		if err != nil {
			return nil, fmt.Errorf("lz4 decode: %w", err)
		}
		return decoded[:n], nil
	default:
		return nil, fmt.Errorf("unknown compression algorithm: %d", algo)
	}
}
