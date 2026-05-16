package wal

import (
	"encoding/binary"
	"fmt"
	"sync"

	"codeberg.org/micro-ts/mts/types"
)

const pointVersion byte = 3

var walBufPool = sync.Pool{New: func() any { return make([]byte, 0, 256) }}

// SerializePoint 序列化 point 为 WAL 格式 v3:
// version(1B) + dbLen(2B,LE) + db + measLen(2B,LE) + meas + ts(8B,LE) + sid(8B,LE) + fieldData(N)
// 返回 data 和 release 函数，调用 release 将 buffer 归还池。
func SerializePoint(db, meas string, ts int64, sid uint64, fieldData []byte) ([]byte, func()) {
	buf := walBufPool.Get().([]byte)
	buf = buf[:0]
	buf = append(buf, pointVersion)
	// db
	putUint16LE(&buf, uint16(len(db)))
	buf = append(buf, db...)
	// meas
	putUint16LE(&buf, uint16(len(meas)))
	buf = append(buf, meas...)
	// ts + sid
	putUint64LE(&buf, uint64(ts))
	putUint64LE(&buf, sid)
	// fieldData
	buf = append(buf, fieldData...)
	return buf, func() { walBufPool.Put(buf[:0]) } //nolint:staticcheck // SA6002: []byte is pointer-like, allocation unavoidable for interface boxing
}

// DeserializePoint 从 WAL payload 反序列化 MemPoint。
// 支持 version=1/2（旧版，无 db/meas）和 version=3（当前）。
func DeserializePoint(data []byte) (types.MemPoint, error) {
	if len(data) < 2 {
		return types.MemPoint{}, fmt.Errorf("wal data too short: %d bytes", len(data))
	}
	version := data[0]
	switch version {
	case 1, 2:
		// 旧版：version(1B) + ts(8B) + sid(8B) + fieldData(N)
		if len(data) < 17 {
			return types.MemPoint{}, fmt.Errorf("wal v%d data too short: %d bytes", version, len(data))
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
	case 3:
		// 新版：version(1B) + dbLen(2B) + db + measLen(2B) + meas + ts(8B) + sid(8B) + fieldData(N)
		if len(data) < 21 {
			return types.MemPoint{}, fmt.Errorf("wal v%d data too short: %d bytes", version, len(data))
		}
		pos := 1
		dbLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		db := string(data[pos : pos+dbLen])
		pos += dbLen
		measLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		meas := string(data[pos : pos+measLen])
		pos += measLen
		ts := int64(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		sid := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		fieldData := make([]byte, len(data)-pos)
		copy(fieldData, data[pos:])
		return types.MemPoint{
			Database:    db,
			Measurement: meas,
			Timestamp:   ts,
			Sid:         sid,
			FieldData:   fieldData,
		}, nil
	default:
		return types.MemPoint{}, fmt.Errorf("unsupported wal version: %d", version)
	}
}

func putUint16LE(buf *[]byte, v uint16) {
	*buf = append(*buf, byte(v), byte(v>>8))
}

func putUint64LE(buf *[]byte, v uint64) {
	*buf = append(*buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
		byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
}
