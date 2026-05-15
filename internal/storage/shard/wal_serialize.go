// Package shard 内部数据点序列化（仅测试使用）。
package shard

import (
	"encoding/binary"
	"fmt"
	"math"

	"codeberg.org/micro-ts/mts/types"
)

const pointVersion byte = 2

// serializeInternalPoint 将 InternalPoint 序列化为紧凑二进制格式。
func serializeInternalPoint(ip types.InternalPoint) ([]byte, error) {
	size := 1 + 8 + 8 + 2
	for _, f := range ip.Fields {
		size += 2 + len(f.Key) + 1
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
			bits := binary.BigEndian.Uint64(data[pos : pos+8])
			fv = types.NewFieldValue(math.Float64frombits(bits))
			pos += 8
		case 1:
			if pos+8 > len(data) {
				return types.InternalPoint{}, fmt.Errorf("point data too short for int64")
			}
			fv = types.NewFieldValue(int64(binary.BigEndian.Uint64(data[pos : pos+8])))
			pos += 8
		case 2:
			if pos+2 > len(data) {
				return types.InternalPoint{}, fmt.Errorf("point data too short for string len")
			}
			sLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2
			if pos+sLen > len(data) {
				return types.InternalPoint{}, fmt.Errorf("point data too short for string value")
			}
			fv = types.NewFieldValue(string(data[pos : pos+sLen]))
			pos += sLen
		case 3:
			if pos+1 > len(data) {
				return types.InternalPoint{}, fmt.Errorf("point data too short for bool")
			}
			fv = types.NewFieldValue(data[pos] == 1)
			pos++
		default:
			return types.InternalPoint{}, fmt.Errorf("unsupported field type: %d", typ)
		}
		fields = append(fields, types.InternalField{Key: key, Value: fv})
	}

	return types.InternalPoint{
		Timestamp: ts,
		Sid:       sid,
		Fields:    fields,
	}, nil
}
