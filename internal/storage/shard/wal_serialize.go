package shard

import (
	"encoding/binary"
	"fmt"
	"math"

	"codeberg.org/micro-ts/mts/types"
)

// Point 序列化格式:
// [8 bytes: timestamp][4 bytes: tag_len][N bytes: tags][4 bytes: field_count]
// field: [4 bytes: key_len][N bytes: key][1 byte: type][N bytes: value]

func estimateSerializedSize(p *types.Point) int {
	size := 8 + 4 + 4 // timestamp + tag_len + field_count

	for k, v := range p.Tags {
		size += len(k) + len(v) + 2
	}

	for k, v := range p.Fields {
		size += 4 + len(k) + 1
		switch v.GetValue().(type) {
		case *types.FieldValue_FloatValue, *types.FieldValue_IntValue:
			size += 8
		case *types.FieldValue_StringValue:
			size += 4 + len(v.GetValue().(*types.FieldValue_StringValue).StringValue)
		case *types.FieldValue_BoolValue:
			size += 1
		}
	}

	return size
}

func serializePoint(p *types.Point) ([]byte, error) {
	buf := make([]byte, 0, estimateSerializedSize(p))

	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(p.Timestamp))
	buf = append(buf, ts[:]...)

	tagLen := 0
	for k, v := range p.Tags {
		tagLen += len(k) + len(v) + 2
	}
	var tl [4]byte
	binary.BigEndian.PutUint32(tl[:], uint32(tagLen))
	buf = append(buf, tl[:]...)

	for k, v := range p.Tags {
		buf = append(buf, k...)
		buf = append(buf, 0)
		buf = append(buf, v...)
		buf = append(buf, 0)
	}

	var fc [4]byte
	binary.BigEndian.PutUint32(fc[:], uint32(len(p.Fields)))
	buf = append(buf, fc[:]...)

	for k, v := range p.Fields {
		var kl [4]byte
		binary.BigEndian.PutUint32(kl[:], uint32(len(k)))
		buf = append(buf, kl[:]...)
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
			var vl [4]byte
			binary.BigEndian.PutUint32(vl[:], uint32(len(val.StringValue)))
			buf = append(buf, vl[:]...)
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

func deserializePoint(data []byte) (*types.Point, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("data too short: %d bytes", len(data))
	}

	pos := 0

	ts := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8

	tagLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	estimatedTags := tagLen / 20
	if estimatedTags < 1 {
		estimatedTags = 1
	}
	tags := make(map[string]string, estimatedTags)

	if tagLen > 0 {
		if pos+tagLen > len(data) {
			return nil, fmt.Errorf("invalid tag length: %d", tagLen)
		}
		tagData := data[pos : pos+tagLen]
		pos += tagLen

		start := 0
		var key string
		for i := 0; i <= len(tagData); i++ {
			if i == len(tagData) || tagData[i] == 0 {
				if key == "" {
					key = string(tagData[start:i])
				} else {
					tags[key] = string(tagData[start:i])
					key = ""
				}
				start = i + 1
			}
		}
	}

	if pos+4 > len(data) {
		return nil, fmt.Errorf("data too short for field count")
	}
	fieldCount := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	fields := make(map[string]*types.FieldValue, fieldCount)

	for i := 0; i < fieldCount; i++ {
		if pos+4 > len(data) {
			return nil, fmt.Errorf("data too short for field key length")
		}
		keyLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4

		if pos+keyLen+1 > len(data) {
			return nil, fmt.Errorf("data too short for field key")
		}
		key := string(data[pos : pos+keyLen])
		pos += keyLen

		typ := data[pos]
		pos++

		switch typ {
		case 0:
			if pos+8 > len(data) {
				return nil, fmt.Errorf("data too short for float64 value")
			}
			val := math.Float64frombits(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			fields[key] = types.NewFieldValue(val)
		case 1:
			if pos+8 > len(data) {
				return nil, fmt.Errorf("data too short for int64 value")
			}
			val := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			fields[key] = types.NewFieldValue(val)
		case 2:
			if pos+4 > len(data) {
				return nil, fmt.Errorf("data too short for string length")
			}
			valLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if pos+valLen > len(data) {
				return nil, fmt.Errorf("data too short for string value")
			}
			val := string(data[pos : pos+valLen])
			pos += valLen
			fields[key] = types.NewFieldValue(val)
		case 3:
			if pos+1 > len(data) {
				return nil, fmt.Errorf("data too short for bool value")
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
