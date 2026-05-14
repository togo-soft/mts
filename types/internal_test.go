package types

import (
	"testing"
)

func TestSerializeFieldsFromMap_RoundTrip(t *testing.T) {
	fields := map[string]*FieldValue{
		"float_val": NewFieldValue(3.14),
		"int_val":   NewFieldValue(int64(42)),
		"str_val":   NewFieldValue("hello"),
		"bool_val":  NewFieldValue(true),
	}

	data := serializeFieldsFromMap(fields)
	if len(data) == 0 {
		t.Fatal("serialized data should not be empty")
	}

	decoded, err := deserializeFieldData(data)
	if err != nil {
		t.Fatalf("deserializeFieldData: %v", err)
	}
	if len(decoded) != len(fields) {
		t.Errorf("expected %d fields, got %d", len(fields), len(decoded))
	}

	result := InternalFieldsToMap(decoded)
	for k, v := range fields {
		got, ok := result[k]
		if !ok {
			t.Errorf("field %q missing", k)
			continue
		}
		if got.GetValue() == nil || v.GetValue() == nil {
			continue
		}
		switch ev := v.GetValue().(type) {
		case *FieldValue_FloatValue:
			if gv, ok := got.GetValue().(*FieldValue_FloatValue); ok {
				if gv.FloatValue != ev.FloatValue {
					t.Errorf("field %q: expected %f, got %f", k, ev.FloatValue, gv.FloatValue)
				}
			}
		case *FieldValue_IntValue:
			if gv, ok := got.GetValue().(*FieldValue_IntValue); ok {
				if gv.IntValue != ev.IntValue {
					t.Errorf("field %q: expected %d, got %d", k, ev.IntValue, gv.IntValue)
				}
			}
		case *FieldValue_StringValue:
			if gv, ok := got.GetValue().(*FieldValue_StringValue); ok {
				if gv.StringValue != ev.StringValue {
					t.Errorf("field %q: expected %q, got %q", k, ev.StringValue, gv.StringValue)
				}
			}
		case *FieldValue_BoolValue:
			if gv, ok := got.GetValue().(*FieldValue_BoolValue); ok {
				if gv.BoolValue != ev.BoolValue {
					t.Errorf("field %q: expected %v, got %v", k, ev.BoolValue, gv.BoolValue)
				}
			}
		}
	}
}

func TestSerializeFieldsFromMap_Empty(t *testing.T) {
	data := serializeFieldsFromMap(map[string]*FieldValue{})
	if data != nil {
		t.Errorf("expected nil for empty fields, got %d bytes", len(data))
	}

	decoded, err := deserializeFieldData(data)
	if err == nil {
		t.Error("expected error for empty field data")
	}
	_ = decoded
}

func TestSerializeFieldsFromMap_Nil(t *testing.T) {
	data := serializeFieldsFromMap(nil)
	if data != nil {
		t.Errorf("expected nil for nil fields, got %d bytes", len(data))
	}
}

func TestDeserializeFieldData_Invalid(t *testing.T) {
	// 过短数据
	_, err := deserializeFieldData([]byte{0})
	if err == nil {
		t.Error("expected error for single byte")
	}

	// fieldCount 指向不存在的区域
	_, err = deserializeFieldData([]byte{0, 5})
	if err == nil {
		t.Error("expected error for truncated field data")
	}

	// 截断的 key 长度
	_, err = deserializeFieldData([]byte{0, 1, 0})
	if err == nil {
		t.Error("expected error for truncated key len")
	}

	// 未知字段类型
	buf := []byte{0, 1, 0, 1, byte('k'), 99}
	_, err = deserializeFieldData(buf)
	if err == nil {
		t.Error("expected error for unknown field type")
	}
}

func TestPointToMemPoint_RoundTrip(t *testing.T) {
	p := &Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields: map[string]*FieldValue{
			"value": NewFieldValue(42.0),
		},
	}

	mp := PointToMemPoint(p, 7)
	if mp.Timestamp != p.Timestamp {
		t.Errorf("expected timestamp %d, got %d", p.Timestamp, mp.Timestamp)
	}
	if mp.Sid != 7 {
		t.Errorf("expected sid 7, got %d", mp.Sid)
	}
	if len(mp.FieldData) == 0 {
		t.Fatal("expected non-empty FieldData")
	}

	ip, err := MemPointToInternal(mp)
	if err != nil {
		t.Fatalf("MemPointToInternal: %v", err)
	}
	if ip.Timestamp != p.Timestamp {
		t.Errorf("expected timestamp %d, got %d", p.Timestamp, ip.Timestamp)
	}
	if ip.Sid != 7 {
		t.Errorf("expected sid 7, got %d", ip.Sid)
	}
	if len(ip.Fields) != 1 {
		t.Errorf("expected 1 field, got %d", len(ip.Fields))
	}
	if ip.Fields[0].Key != "value" {
		t.Errorf("expected key 'value', got %q", ip.Fields[0].Key)
	}
}

func TestPointToMemPoint_NoFields(t *testing.T) {
	p := &Point{
		Timestamp: 1000000000,
		Fields:    nil,
	}

	mp := PointToMemPoint(p, 0)
	if mp.FieldData != nil {
		t.Errorf("expected nil FieldData for point with no fields, got %d bytes", len(mp.FieldData))
	}

	ip, err := MemPointToInternal(mp)
	if err != nil {
		t.Fatalf("MemPointToInternal: %v", err)
	}
	if len(ip.Fields) != 0 {
		t.Errorf("expected 0 fields, got %d", len(ip.Fields))
	}
}

func TestMemPointToInternal_EmptyFieldData(t *testing.T) {
	mp := MemPoint{
		Timestamp: 100,
		Sid:       5,
		FieldData: nil,
	}
	ip, err := MemPointToInternal(mp)
	if err != nil {
		t.Fatalf("MemPointToInternal: %v", err)
	}
	if ip.Timestamp != 100 {
		t.Errorf("expected timestamp 100, got %d", ip.Timestamp)
	}
	if ip.Sid != 5 {
		t.Errorf("expected sid 5, got %d", ip.Sid)
	}
	if ip.Fields != nil {
		t.Errorf("expected nil Fields, got %v", ip.Fields)
	}
}

func TestDeserializeFieldData_AllTypes(t *testing.T) {
	fields := map[string]*FieldValue{
		"f": NewFieldValue(1.5),
		"i": NewFieldValue(int64(-42)),
		"s": NewFieldValue("test"),
		"b": NewFieldValue(false),
	}

	data := serializeFieldsFromMap(fields)
	decoded, err := deserializeFieldData(data)
	if err != nil {
		t.Fatalf("deserializeFieldData: %v", err)
	}

	m := InternalFieldsToMap(decoded)
	if fv := m["f"].GetFloatValue(); fv != 1.5 {
		t.Errorf("expected float 1.5, got %f", fv)
	}
	if iv := m["i"].GetIntValue(); iv != -42 {
		t.Errorf("expected int -42, got %d", iv)
	}
	if sv := m["s"].GetStringValue(); sv != "test" {
		t.Errorf("expected string 'test', got %q", sv)
	}
	if bv := m["b"].GetBoolValue(); bv {
		t.Errorf("expected bool false, got %v", bv)
	}
}
