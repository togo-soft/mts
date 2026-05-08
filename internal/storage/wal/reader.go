package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
)

// ErrCorruptRecord 表示发现一条损坏的 WAL 记录。
var ErrCorruptRecord = &FormatError{Reason: "CRC mismatch"}

// readSegmentHeader 读取并验证 segment 文件头。
func readSegmentHeader(file *os.File) (version uint16, segmentNum uint32, err error) {
	var buf [segmentHeaderSize]byte
	n, err := io.ReadFull(file, buf[:])
	if err != nil {
		return 0, 0, err
	}
	_ = n
	return decodeSegmentHeader(buf[:])
}

// readRecords 从文件指定偏移开始流式读取 WAL 记录。
// 对每条有效记录调用 fn(payload)，遇到 CRC 错误跳过并告警。
// 返回最终文件偏移。
func readRecords(file *os.File, startPos int64, fn func(payload []byte) error) (int64, error) {
	if _, err := file.Seek(startPos, 0); err != nil {
		return startPos, err
	}

	pos := startPos
	var headerBuf [recordHeaderSize]byte

	for {
		n, err := io.ReadFull(file, headerBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return pos, nil
			}
			return pos, err
		}
		_ = n
		pos += recordHeaderSize

		expectedCRC := binary.BigEndian.Uint32(headerBuf[0:4])
		recType := headerBuf[4]
		payloadLen := binary.BigEndian.Uint32(headerBuf[5:9])

		if payloadLen > 256*1024*1024 {
			slog.Warn("WAL record too large, stopping replay",
				"offset", pos-recordHeaderSize,
				"payloadLen", payloadLen)
			return pos, nil
		}

		payload := make([]byte, payloadLen)
		if payloadLen > 0 {
			if _, err := io.ReadFull(file, payload); err != nil {
				slog.Warn("WAL incomplete record, stopping replay",
					"offset", pos, "error", err)
				return pos, nil
			}
		}
		pos += int64(payloadLen)

		// recordHeaderSize 已包含 CRC32(4) + type(1) + length(4)，无需再加 4
		recordBodySize := int64(recordHeaderSize) + int64(payloadLen)
		padding := pad8(int(recordBodySize))
		if padding > 0 {
			if _, err := file.Seek(int64(padding), io.SeekCurrent); err != nil {
				return pos, err
			}
			pos += int64(padding)
		}

		actualCRC := crc32Sum(headerBuf[4:9])
		actualCRC = crc32Update(actualCRC, payload)
		if actualCRC != expectedCRC {
			slog.Warn("WAL CRC mismatch, skipping record",
				"offset", pos-recordBodySize,
				"expected", expectedCRC,
				"actual", actualCRC)
			continue
		}

		if recType == TypePointData || recType == TypeMeta {
			if err := fn(payload); err != nil {
				return pos, err
			}
		}
	}
}

// crc32Update 增量更新 CRC32 值。
func crc32Update(crc uint32, data []byte) uint32 {
	return crc32.Update(crc, ieeeTable, data)
}
