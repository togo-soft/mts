package writer

import (
	"encoding/binary"
	"fmt"
	"sync"

	"codeberg.org/micro-ts/mts/types"
)

const pointVersion byte = 2

var walBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 256)
		return &buf
	},
}

// serializePointForWALPooled 将 ts + sid + FieldData 序列化到池化缓冲区。
func serializePointForWALPooled(ts int64, sid uint64, fieldData []byte) ([]byte, func()) {
	size := 1 + 8 + 8 + len(fieldData)
	bufPtr := walBufPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	buf[0] = pointVersion
	binary.BigEndian.PutUint64(buf[1:9], uint64(ts))
	binary.BigEndian.PutUint64(buf[9:17], sid)
	copy(buf[17:], fieldData)
	*bufPtr = buf

	release := func() {
		walBufPool.Put(bufPtr)
	}
	return buf, release
}

// serializePointDirect 合并字段序列化与 WAL 序列化为单步操作。
// 消除 serializeFieldsFromMap 的独立池分配+复制和 serializePointForWALPooled 的二次复制，
// 单次遍历 fields map 直接写入 WAL 池缓冲区。
//
// 返回:
//   - mp: MemPoint，FieldData 为独立副本（MemTable 持有）
//   - walData: WAL 完整记录（池缓冲区，用完需 walRelease() 归还）
//   - walRelease: 归还 WAL 缓冲区的函数
func serializePointDirect(ts int64, sid uint64, fields map[string]*types.FieldValue) (types.MemPoint, []byte, func()) {
	fieldSize := types.FieldDataSize(fields)
	// WAL 格式: version(1) + ts(8) + sid(8) + FieldData
	totalSize := 17 + fieldSize

	bufPtr := walBufPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < totalSize {
		buf = make([]byte, totalSize)
	} else {
		buf = buf[:totalSize]
	}

	// WAL 头
	buf[0] = pointVersion
	binary.BigEndian.PutUint64(buf[1:9], uint64(ts))
	binary.BigEndian.PutUint64(buf[9:17], sid)

	// 直接序列化字段到 WAL 缓冲区（复用 AppendFieldData）
	fieldPart := types.AppendFieldData(buf[17:17], fields)
	_ = fieldPart // fieldPart 是 buf[17:] 的子切片，buf 已包含完整数据

	*bufPtr = buf

	// 为 MemPoint 复制 FieldData（buf 归还池后数据失效）
	fieldData := make([]byte, len(buf)-17)
	copy(fieldData, buf[17:])

	mp := types.MemPoint{
		Timestamp: ts,
		Sid:       sid,
		FieldData: fieldData,
	}

	release := func() {
		walBufPool.Put(bufPtr)
	}
	return mp, buf, release
}

// deserializeFromWAL 从 WAL 完整格式解析出 MemPoint。
func deserializeFromWAL(data []byte) (types.MemPoint, error) {
	if len(data) < 19 {
		return types.MemPoint{}, fmt.Errorf("wal data too short: %d bytes", len(data))
	}
	if data[0] != pointVersion {
		return types.MemPoint{}, fmt.Errorf("unsupported point version: %d (expected %d)", data[0], pointVersion)
	}
	ts := int64(binary.BigEndian.Uint64(data[1:9]))
	sid := binary.BigEndian.Uint64(data[9:17])
	fieldData := make([]byte, len(data)-17)
	copy(fieldData, data[17:])
	return types.MemPoint{
		Timestamp: ts,
		Sid:       sid,
		FieldData: fieldData,
	}, nil
}
