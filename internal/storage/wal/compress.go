package wal

import (
	"encoding/binary"

	lz4 "github.com/pierrec/lz4/v4"
)

// CompressPayload 使用 LZ4 压缩 payload。
// 如果压缩后更大，则存储原始数据（标记为未压缩）。
// 格式: [flag:1B][size:4B][data]
// flag=0 表示未压缩，flag=1 表示压缩
func CompressPayload(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, nil
	}

	// 预分配足够空间：1字节flag + 4字节原始大小 + 压缩数据
	dst := make([]byte, 5+len(payload)*2)

	// 尝试压缩
	n, err := lz4.CompressBlock(payload, dst[5:], nil)
	if err != nil {
		return nil, err
	}

	compressedSize := 5 + n
	if n > 0 && n < len(payload) {
		// 压缩有效，存储压缩数据
		dst[0] = 1 // flag = compressed
		binary.BigEndian.PutUint32(dst[1:5], uint32(len(payload)))
		return dst[:compressedSize], nil
	}

	// 压缩无效或没节省空间，存储原始数据
	// 格式：flag=0 + 原始大小 + 原始数据
	result := make([]byte, 5+len(payload))
	result[0] = 0 // flag = not compressed
	binary.BigEndian.PutUint32(result[1:5], uint32(len(payload)))
	copy(result[5:], payload)
	return result, nil
}

// DecompressPayload 解压 payload。
// 格式: [flag:1B][size:4B][data]
func DecompressPayload(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}
	if len(src) < 5 {
		return nil, &CompressionError{Reason: "payload too short"}
	}

	flag := src[0]
	originalSize := int(binary.BigEndian.Uint32(src[1:5]))

	if originalSize <= 0 || originalSize > 256*1024*1024 {
		return nil, &CompressionError{Reason: "invalid original size"}
	}

	if flag == 0 {
		// 未压缩，直接返回原始数据
		return src[5 : 5+originalSize], nil
	}

	// 压缩数据，解压
	dst := make([]byte, originalSize)
	n, err := lz4.UncompressBlock(src[5:], dst)
	if err != nil {
		return nil, err
	}
	return dst[:n], nil
}

// CompressionError 表示压缩/解压错误。
type CompressionError struct {
	Reason string
}

func (e *CompressionError) Error() string {
	return "compression error: " + e.Reason
}
