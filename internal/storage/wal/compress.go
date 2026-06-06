package wal

import (
	"encoding/binary"
	"fmt"

	lz4 "github.com/pierrec/lz4/v4"
)

// minCompressSize 是尝试 LZ4 压缩的最小 payload 大小。
// 小于此值的 payload 压缩率很低（5 字节头开销占比大），直接跳过。
const minCompressSize = 80

// CompressPayload 使用 LZ4 压缩 payload。
// 如果压缩后更大，则存储原始数据（标记为未压缩）。
// 格式: [flag:1B][size:4B][data]
// flag=0 表示未压缩，flag=1 表示压缩。
//
// 返回值:
//   - data: 压缩后的数据（含 5 字节头）
//   - release: 调用方使用完 data 后必须调用此函数归还底层缓冲区
//   - err: 压缩失败
func CompressPayload(payload []byte) (data []byte, release func(), err error) {
	if len(payload) == 0 {
		return nil, nil, fmt.Errorf("compress: empty payload")
	}

	// 小 payload 直接存储未压缩，避免无效的 LZ4 压缩尝试
	if len(payload) < minCompressSize {
		raw := getBuf(5 + len(payload))
		raw = raw[:5+len(payload)]
		raw[0] = 0
		binary.BigEndian.PutUint32(raw[1:5], uint32(len(payload)))
		copy(raw[5:], payload)
		return raw, func() { putBuf(raw) }, nil
	}

	// 从池中获取足够大的缓冲区：5字节头 + 压缩数据（最坏情况比原数据大）
	buf := getBuf(5 + len(payload)*2)

	// 尝试压缩到 buf[5:]（lz4.CompressBlock 需要目标缓冲区有足够容量）
	n, compErr := lz4.CompressBlock(payload, buf[5:cap(buf)], nil)
	if compErr != nil {
		putBuf(buf)
		return nil, nil, compErr
	}

	if n > 0 && n < len(payload) {
		// 压缩有效，使用压缩数据
		buf = buf[:5+n]
		buf[0] = 1 // flag = compressed
		binary.BigEndian.PutUint32(buf[1:5], uint32(len(payload)))
		return buf, func() { putBuf(buf) }, nil
	}

	// 压缩无效，归还大缓冲区，分配精确大小存原始数据
	putBuf(buf)

	raw := getBuf(5 + len(payload))
	raw = raw[:5+len(payload)]
	raw[0] = 0 // flag = not compressed
	binary.BigEndian.PutUint32(raw[1:5], uint32(len(payload)))
	copy(raw[5:], payload)
	return raw, func() { putBuf(raw) }, nil
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
