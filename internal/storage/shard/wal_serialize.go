package shard

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"codeberg.org/micro-ts/mts/types"
)

const pointVersion byte = 2

// walBufPool 池化 WAL 序列化缓冲区，减少 per-point 内存分配。
var walBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 256)
		return &buf
	},
}

// serializePointForWALPooled 将 ts + sid + FieldData 序列化到池化缓冲区。
// 格式: Version(1B) + Timestamp(8B) + Sid(8B) + FieldData
// 返回序列化数据和释放函数。调用者在 WAL 写入完成后必须调用 release 归还 buffer。
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

// serializeInternalPoint 将 InternalPoint 序列化为紧凑二进制格式。
//
// 格式 (v2):
//
//	Version(1B) + Timestamp(8B) + Sid(8B) + FieldCount(2B)
//	+ [KeyLen(2B) + Key + Type(1B) + Value]...
//
// Tags 不写入 WAL，因为 AllocateSID 已将 Tags→Sid 映射持久化到 boltDB，
// replay 时通过 GetTagsBySID 从 boltDB 恢复。
func serializeInternalPoint(ip types.InternalPoint) ([]byte, error) {
	size := 1 + 8 + 8 + 2 // version + ts + sid + fieldCount
	for _, f := range ip.Fields {
		size += 2 + len(f.Key) + 1 // keyLen + key + type
		switch v := f.Value.GetValue().(type) {
		case *types.FieldValue_FloatValue, *types.FieldValue_IntValue:
			size += 8
		case *types.FieldValue_StringValue:
			size += 2 + len(v.StringValue)
		case *types.FieldValue_BoolValue:
			size += 1
		}
	}

	buf := make([]byte, 0, size)
	buf = append(buf, pointVersion)

	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(ip.Timestamp))
	buf = append(buf, tsBuf[:]...)

	binary.BigEndian.PutUint64(tsBuf[:], ip.Sid)
	buf = append(buf, tsBuf[:]...)

	var fc [2]byte
	binary.BigEndian.PutUint16(fc[:], uint16(len(ip.Fields)))
	buf = append(buf, fc[:]...)

	for _, f := range ip.Fields {
		buf = appendU16(buf, uint16(len(f.Key)))
		buf = append(buf, f.Key...)

		switch val := f.Value.GetValue().(type) {
		case *types.FieldValue_FloatValue:
			buf = append(buf, 0)
			var vb [8]byte
			binary.BigEndian.PutUint64(vb[:], math.Float64bits(val.FloatValue))
			buf = append(buf, vb[:]...)
		case *types.FieldValue_IntValue:
			buf = append(buf, 1)
			var vb [8]byte
			binary.BigEndian.PutUint64(vb[:], uint64(val.IntValue))
			buf = append(buf, vb[:]...)
		case *types.FieldValue_StringValue:
			buf = append(buf, 2)
			buf = appendU16(buf, uint16(len(val.StringValue)))
			buf = append(buf, val.StringValue...)
		case *types.FieldValue_BoolValue:
			buf = append(buf, 3)
			if val.BoolValue {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		}
	}

	return buf, nil
}

// deserializeFromWAL 从 WAL 完整格式解析出 MemPoint。
// FieldData 需要 copy，因为 WAL replay 的 data 缓冲区会被重用。
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

func appendU16(buf []byte, v uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return append(buf, b[:]...)
}

// deserializeInternalPoint 从字节反序列化为 InternalPoint。
func deserializeInternalPoint(data []byte) (types.InternalPoint, error) {
	if len(data) < 19 {
		return types.InternalPoint{}, fmt.Errorf("point data too short: %d bytes", len(data))
	}

	version := data[0]
	if version != pointVersion {
		return types.InternalPoint{}, fmt.Errorf("unsupported point version: %d (expected %d)", version, pointVersion)
	}

	pos := 1
	ts := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8

	sid := binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8

	fieldCount := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2

	fields := make([]types.InternalField, 0, fieldCount)
	for range fieldCount {
		if pos+2 > len(data) {
			return types.InternalPoint{}, fmt.Errorf("point data too short for field key len")
		}
		kLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+kLen > len(data) {
			return types.InternalPoint{}, fmt.Errorf("point data too short for field key")
		}
		key := string(data[pos : pos+kLen])
		pos += kLen

		if pos+1 > len(data) {
			return types.InternalPoint{}, fmt.Errorf("point data too short for field type")
		}
		typ := data[pos]
		pos++

		var fv *types.FieldValue
		switch typ {
		case 0:
			if pos+8 > len(data) {
				return types.InternalPoint{}, fmt.Errorf("point data too short for float64")
			}
			val := math.Float64frombits(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			fv = types.NewFieldValue(val)
		case 1:
			if pos+8 > len(data) {
				return types.InternalPoint{}, fmt.Errorf("point data too short for int64")
			}
			val := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			fv = types.NewFieldValue(val)
		case 2:
			if pos+2 > len(data) {
				return types.InternalPoint{}, fmt.Errorf("point data too short for string len")
			}
			vLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2
			if pos+vLen > len(data) {
				return types.InternalPoint{}, fmt.Errorf("point data too short for string value")
			}
			val := string(data[pos : pos+vLen])
			pos += vLen
			fv = types.NewFieldValue(val)
		case 3:
			if pos+1 > len(data) {
				return types.InternalPoint{}, fmt.Errorf("point data too short for bool")
			}
			val := data[pos] == 1
			pos++
			fv = types.NewFieldValue(val)
		default:
			return types.InternalPoint{}, fmt.Errorf("unknown field type: %d", typ)
		}

		fields = append(fields, types.InternalField{Key: key, Value: fv})
	}

	return types.InternalPoint{
		Timestamp: ts,
		Fields:    fields,
		Sid:       sid,
	}, nil
}
