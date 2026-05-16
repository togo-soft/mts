package wal

import (
	"encoding/binary"
	"fmt"
	"sync"

	"codeberg.org/micro-ts/mts/types"
)

const pointVersion byte = 2

var walBufPool = sync.Pool{New: func() any { return make([]byte, 0, 256) }}

// SerializePoint 序列化 point 为 WAL 格式: version(1B) + ts(8B) + sid(8B) + fieldData(N)
// 返回 data 和 release 函数，调用 release 将 buffer 归还池
func SerializePoint(ts int64, sid uint64, fieldData []byte) ([]byte, func()) {
	buf := walBufPool.Get().([]byte)
	buf = buf[:0]
	buf = append(buf, pointVersion)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint64(buf[1:9], uint64(ts))
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint64(buf[9:17], sid)
	buf = append(buf, fieldData...)
	return buf, func() { walBufPool.Put(buf[:0]) }
}

// DeserializePoint 从 WAL payload 反序列化 MemPoint
func DeserializePoint(data []byte) (types.MemPoint, error) {
	if len(data) < 17 {
		return types.MemPoint{}, fmt.Errorf("wal data too short: %d bytes", len(data))
	}
	version := data[0]
	if version != pointVersion {
		return types.MemPoint{}, fmt.Errorf("unsupported wal version: %d", version)
	}
	ts := int64(binary.LittleEndian.Uint64(data[1:9]))
	sid := binary.LittleEndian.Uint64(data[9:17])
	fieldData := make([]byte, len(data)-17)
	copy(fieldData, data[17:])
	return types.MemPoint{
		Timestamp: ts,
		Sid:       sid,
		FieldData: fieldData,
	}, nil
}
