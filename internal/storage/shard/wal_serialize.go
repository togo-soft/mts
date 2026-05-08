package shard

import (
	"encoding/binary"
	"fmt"
	"math"

	"codeberg.org/micro-ts/mts/types"
)

const pointVersion byte = 1

// serializePoint 将 Point 序列化为 length-prefixed 字节格式。
//
// 格式:
//
//	Version(1B) + Flags(1B) + Timestamp(8B) + TagCount(2B)
//	+ [KeyLen(2B) + Key + ValLen(2B) + Value]...
//	+ FieldCount(2B) + [KeyLen(2B) + Key + Type(1B) + Value]...
func serializePoint(p *types.Point) ([]byte, error) {
	size := estimateSerializedSize(p)
	buf := make([]byte, 0, size)

	buf = append(buf, pointVersion, 0) // version + flags

	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(p.Timestamp))
	buf = append(buf, ts[:]...)

	var tc [2]byte
	binary.BigEndian.PutUint16(tc[:], uint16(len(p.Tags)))
	buf = append(buf, tc[:]...)

	for k, v := range p.Tags {
		buf = appendU16(buf, uint16(len(k)))
		buf = append(buf, k...)
		buf = appendU16(buf, uint16(len(v)))
		buf = append(buf, v...)
	}

	binary.BigEndian.PutUint16(tc[:], uint16(len(p.Fields)))
	buf = append(buf, tc[:]...)

	for k, v := range p.Fields {
		buf = appendU16(buf, uint16(len(k)))
		buf = append(buf, k...)

		switch val := v.GetValue().(type) {
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

// deserializePoint 从 bytes 反序列化为 Point。
func deserializePoint(data []byte) (*types.Point, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("point data too short: %d bytes", len(data))
	}

	version := data[0]
	if version != pointVersion {
		return nil, fmt.Errorf("unsupported point version: %d", version)
	}

	pos := 2
	ts := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8

	tagCount := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2

	tags := make(map[string]string, tagCount)
	for i := 0; i < tagCount; i++ {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("point data too short for tag key len")
		}
		kLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+kLen > len(data) {
			return nil, fmt.Errorf("point data too short for tag key")
		}
		key := string(data[pos : pos+kLen])
		pos += kLen

		if pos+2 > len(data) {
			return nil, fmt.Errorf("point data too short for tag val len")
		}
		vLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+vLen > len(data) {
			return nil, fmt.Errorf("point data too short for tag value")
		}
		value := string(data[pos : pos+vLen])
		pos += vLen

		tags[key] = value
	}

	if pos+2 > len(data) {
		return nil, fmt.Errorf("point data too short for field count")
	}
	fieldCount := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2

	fields := make(map[string]*types.FieldValue, fieldCount)
	for i := 0; i < fieldCount; i++ {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("point data too short for field key len")
		}
		kLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+kLen > len(data) {
			return nil, fmt.Errorf("point data too short for field key")
		}
		key := string(data[pos : pos+kLen])
		pos += kLen

		if pos+1 > len(data) {
			return nil, fmt.Errorf("point data too short for field type")
		}
		typ := data[pos]
		pos++

		switch typ {
		case 0:
			if pos+8 > len(data) {
				return nil, fmt.Errorf("point data too short for float64 value")
			}
			val := math.Float64frombits(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			fields[key] = types.NewFieldValue(val)
		case 1:
			if pos+8 > len(data) {
				return nil, fmt.Errorf("point data too short for int64 value")
			}
			val := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			fields[key] = types.NewFieldValue(val)
		case 2:
			if pos+2 > len(data) {
				return nil, fmt.Errorf("point data too short for string len")
			}
			vLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2
			if pos+vLen > len(data) {
				return nil, fmt.Errorf("point data too short for string value")
			}
			val := string(data[pos : pos+vLen])
			pos += vLen
			fields[key] = types.NewFieldValue(val)
		case 3:
			if pos+1 > len(data) {
				return nil, fmt.Errorf("point data too short for bool value")
			}
			val := data[pos] == 1
			pos++
			fields[key] = types.NewFieldValue(val)
		default:
			return nil, fmt.Errorf("unknown field type: %d", typ)
		}
	}

	return &types.Point{
		Timestamp: ts,
		Tags:      tags,
		Fields:    fields,
	}, nil
}

// estimateSerializedSize 估算序列化后的字节数。
func estimateSerializedSize(p *types.Point) int {
	size := 1 + 1 + 8 + 2 + 2 // version + flags + ts + tagCount + fieldCount

	for k, v := range p.Tags {
		size += 2 + len(k) + 2 + len(v)
	}

	for k, v := range p.Fields {
		size += 2 + len(k) + 1
		switch v.GetValue().(type) {
		case *types.FieldValue_FloatValue, *types.FieldValue_IntValue:
			size += 8
		case *types.FieldValue_StringValue:
			size += 2 + len(v.GetValue().(*types.FieldValue_StringValue).StringValue)
		case *types.FieldValue_BoolValue:
			size += 1
		}
	}

	return size
}
