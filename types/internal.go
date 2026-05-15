package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
)

// InternalField 紧凑字段条目，避免每行分配 map。
type InternalField struct {
	Key   string
	Value *FieldValue
}

// InternalPoint 内部管线中的数据点，保留用于 compaction 和短暂需要解码字段的场景。
type InternalPoint struct {
	Timestamp int64
	Fields    []InternalField
	Sid       uint64
}

// MemPoint 是 MemTable 中存储的紧凑数据点。
// FieldData 使用 WAL v2 格式的字段部分（不含 version/ts/sid 头），
// 避免 []InternalField 切片分配。
//
// 内存布局：FieldData 是单块连续内存，对 GC 友好。
type MemPoint struct {
	Timestamp int64
	Sid       uint64
	FieldData []byte
}

// PointToInternal 将外部 Point 转换为 InternalPoint。
func PointToInternal(p *Point, sid uint64) InternalPoint {
	fields := make([]InternalField, 0, len(p.Fields))
	for k, v := range p.Fields {
		fields = append(fields, InternalField{Key: k, Value: v})
	}
	return InternalPoint{
		Timestamp: p.Timestamp,
		Fields:    fields,
		Sid:       sid,
	}
}

// InternalFieldsToFieldEntry 将 []InternalField 转换为 []*FieldEntry（用于构建 PointRow）。
func InternalFieldsToFieldEntry(fields []InternalField) []*FieldEntry {
	if len(fields) == 0 {
		return nil
	}
	out := make([]*FieldEntry, len(fields))
	for i, f := range fields {
		out[i] = &FieldEntry{Key: f.Key, Value: f.Value}
	}
	return out
}

// FieldEntryToInternalFields 将 []*FieldEntry 转换为 []InternalField（用于 Compaction 路径）。
func FieldEntryToInternalFields(fields []*FieldEntry) []InternalField {
	if len(fields) == 0 {
		return nil
	}
	out := make([]InternalField, len(fields))
	for i, f := range fields {
		out[i] = InternalField{Key: f.Key, Value: f.Value}
	}
	return out
}

// MapToInternalFields 将 map[string]*FieldValue 转换为 []InternalField（用于写路径）。
func MapToInternalFields(m map[string]*FieldValue) []InternalField {
	if len(m) == 0 {
		return nil
	}
	fields := make([]InternalField, 0, len(m))
	for k, v := range m {
		fields = append(fields, InternalField{Key: k, Value: v})
	}
	return fields
}

// InternalFieldsToMap 将 []InternalField 还原为 map[string]*FieldValue。
func InternalFieldsToMap(fields []InternalField) map[string]*FieldValue {
	if len(fields) == 0 {
		return nil
	}
	m := make(map[string]*FieldValue, len(fields))
	for _, f := range fields {
		m[f.Key] = f.Value
	}
	return m
}

// fieldSerialPool 串行化缓冲区池，复用 serializeFieldsFromMap 的内部缓冲区。
var fieldSerialPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 256)
		return &buf
	},
}

// serializeFieldsFromMap 直接将 map[string]*FieldValue 序列化为 FieldData 格式。
// 跳过 []InternalField 中间态，内部缓冲区池化减少 GC 压力。
//
// 格式: FieldCount(2B BE) + [KeyLen(2B BE) + Key + Type(1B) + Value]...
func serializeFieldsFromMap(fields map[string]*FieldValue) []byte {
	if len(fields) == 0 {
		return nil
	}
	size := 2 // fieldCount
	for k, v := range fields {
		size += 2 + len(k) + 1 // keyLen + key + type
		switch val := v.GetValue().(type) {
		case *FieldValue_FloatValue, *FieldValue_IntValue:
			size += 8
		case *FieldValue_StringValue:
			size += 2 + len(val.StringValue)
		case *FieldValue_BoolValue:
			size += 1
		}
	}

	bufPtr := fieldSerialPool.Get().(*[]byte)
	buf := *bufPtr
	buf = buf[:0]
	if cap(buf) < size {
		buf = make([]byte, 0, size)
	}

	buf = appendU16(buf, uint16(len(fields)))
	for k, v := range fields {
		buf = appendU16(buf, uint16(len(k)))
		buf = append(buf, k...)
		switch val := v.GetValue().(type) {
		case *FieldValue_FloatValue:
			buf = append(buf, 0)
			var vb [8]byte
			binary.BigEndian.PutUint64(vb[:], math.Float64bits(val.FloatValue))
			buf = append(buf, vb[:]...)
		case *FieldValue_IntValue:
			buf = append(buf, 1)
			var vb [8]byte
			binary.BigEndian.PutUint64(vb[:], uint64(val.IntValue))
			buf = append(buf, vb[:]...)
		case *FieldValue_StringValue:
			buf = append(buf, 2)
			buf = appendU16(buf, uint16(len(val.StringValue)))
			buf = append(buf, val.StringValue...)
		case *FieldValue_BoolValue:
			buf = append(buf, 3)
			if val.BoolValue {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		}
	}

	// 复制结果后归还缓冲区到池
	result := make([]byte, len(buf))
	copy(result, buf)
	*bufPtr = buf[:0]
	fieldSerialPool.Put(bufPtr)

	return result
}

// deserializeFieldData 从 FieldData 解码出 []InternalField。
func deserializeFieldData(data []byte) ([]InternalField, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("field data too short: %d bytes", len(data))
	}
	fieldCount := int(binary.BigEndian.Uint16(data[:2]))
	pos := 2
	fields := make([]InternalField, 0, fieldCount)
	for range fieldCount {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("truncated key len at pos %d", pos)
		}
		kLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+kLen > len(data) {
			return nil, fmt.Errorf("truncated key at pos %d (len=%d)", pos, kLen)
		}
		key := string(data[pos : pos+kLen])
		pos += kLen
		if pos+1 > len(data) {
			return nil, fmt.Errorf("truncated type at pos %d", pos)
		}
		typ := data[pos]
		pos++
		var fv *FieldValue
		switch typ {
		case 0: // float64
			if pos+8 > len(data) {
				return nil, fmt.Errorf("truncated float64 at pos %d", pos)
			}
			fv = NewFieldValue(math.Float64frombits(binary.BigEndian.Uint64(data[pos : pos+8])))
			pos += 8
		case 1: // int64
			if pos+8 > len(data) {
				return nil, fmt.Errorf("truncated int64 at pos %d", pos)
			}
			fv = NewFieldValue(int64(binary.BigEndian.Uint64(data[pos : pos+8])))
			pos += 8
		case 2: // string
			if pos+2 > len(data) {
				return nil, fmt.Errorf("truncated string len at pos %d", pos)
			}
			vLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2
			if pos+vLen > len(data) {
				return nil, fmt.Errorf("truncated string value at pos %d (len=%d)", pos, vLen)
			}
			fv = NewFieldValue(string(data[pos : pos+vLen]))
			pos += vLen
		case 3: // bool
			if pos+1 > len(data) {
				return nil, fmt.Errorf("truncated bool at pos %d", pos)
			}
			fv = NewFieldValue(data[pos] == 1)
			pos++
		default:
			return nil, fmt.Errorf("unknown field type: %d", typ)
		}
		fields = append(fields, InternalField{Key: key, Value: fv})
	}
	return fields, nil
}

// MemPointToInternal 将 MemPoint 解码为 InternalPoint（惰性解码）。
func MemPointToInternal(mp MemPoint) (InternalPoint, error) {
	if len(mp.FieldData) == 0 {
		return InternalPoint{
			Timestamp: mp.Timestamp,
			Sid:       mp.Sid,
			Fields:    nil,
		}, nil
	}
	fields, err := deserializeFieldData(mp.FieldData)
	if err != nil {
		return InternalPoint{}, err
	}
	return InternalPoint{
		Timestamp: mp.Timestamp,
		Sid:       mp.Sid,
		Fields:    fields,
	}, nil
}

// PointToMemPoint 将外部 Point 直接序列化为 MemPoint（写入路径入口）。
func PointToMemPoint(p *Point, sid uint64) MemPoint {
	return MemPoint{
		Timestamp: p.Timestamp,
		Sid:       sid,
		FieldData: serializeFieldsFromMap(p.Fields),
	}
}

func appendU16(buf []byte, v uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return append(buf, b[:]...)
}
