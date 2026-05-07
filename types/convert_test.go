package types

import (
	"testing"
	"time"
)

func TestNewFieldValue_Int64(t *testing.T) {
	fv := NewFieldValue(int64(42))
	if fv == nil {
		t.Fatal("expected non-nil FieldValue")
	}
	if fv.GetIntValue() != 42 {
		t.Errorf("expected 42, got %v", fv.GetIntValue())
	}
}

func TestNewFieldValue_Int(t *testing.T) {
	fv := NewFieldValue(42)
	if fv == nil {
		t.Fatal("expected non-nil FieldValue")
	}
	if fv.GetIntValue() != 42 {
		t.Errorf("expected int64(42), got %v", fv.GetIntValue())
	}
}

func TestNewFieldValue_Float64(t *testing.T) {
	fv := NewFieldValue(3.14)
	if fv == nil {
		t.Fatal("expected non-nil FieldValue")
	}
	if fv.GetFloatValue() != 3.14 {
		t.Errorf("expected 3.14, got %v", fv.GetFloatValue())
	}
}

func TestNewFieldValue_Float32(t *testing.T) {
	fv := NewFieldValue(float32(3.14))
	if fv == nil {
		t.Fatal("expected non-nil FieldValue")
	}
	if fv.GetFloatValue() != float64(float32(3.14)) {
		t.Errorf("expected float64 value, got %v", fv.GetFloatValue())
	}
}

func TestNewFieldValue_String(t *testing.T) {
	fv := NewFieldValue("hello")
	if fv == nil {
		t.Fatal("expected non-nil FieldValue")
	}
	if fv.GetStringValue() != "hello" {
		t.Errorf("expected 'hello', got %v", fv.GetStringValue())
	}
}

func TestNewFieldValue_Bool(t *testing.T) {
	fv := NewFieldValue(true)
	if fv == nil {
		t.Fatal("expected non-nil FieldValue")
	}
	if !fv.GetBoolValue() {
		t.Error("expected true")
	}
}

func TestNewFieldValue_BoolFalse(t *testing.T) {
	fv := NewFieldValue(false)
	if fv == nil {
		t.Fatal("expected non-nil FieldValue")
	}
	if fv.GetBoolValue() {
		t.Error("expected false")
	}
}

func TestNewFieldValue_Unsupported(t *testing.T) {
	fv := NewFieldValue([]int{1, 2, 3})
	if fv != nil {
		t.Errorf("expected nil for unsupported type, got %v", fv)
	}
}

func TestDefaultMemTableConfig(t *testing.T) {
	cfg := DefaultMemTableConfig()
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.MaxSize != 64*1024*1024 {
		t.Errorf("expected MaxSize 64MB, got %d", cfg.MaxSize)
	}
	if cfg.MaxCount != 3000 {
		t.Errorf("expected MaxCount 3000, got %d", cfg.MaxCount)
	}
	if cfg.IdleDurationNanos != int64(time.Minute) {
		t.Errorf("expected IdleDuration 1min, got %d ns", cfg.IdleDurationNanos)
	}
}

func TestMemTableConfig_GetSetIdleDuration(t *testing.T) {
	cfg := &MemTableConfig{}
	cfg.SetIdleDuration(5 * time.Minute)
	if cfg.IdleDurationNanos != int64(5*time.Minute) {
		t.Errorf("expected 5min in nanos, got %d", cfg.IdleDurationNanos)
	}
	if cfg.GetIdleDuration() != 5*time.Minute {
		t.Errorf("expected 5min, got %v", cfg.GetIdleDuration())
	}
}

func TestConfig_GetSetShardDuration(t *testing.T) {
	cfg := &Config{}
	cfg.SetShardDuration(7 * 24 * time.Hour)
	if cfg.ShardDurationNanos != int64(7*24*time.Hour) {
		t.Errorf("expected 7 days in nanos, got %d", cfg.ShardDurationNanos)
	}
	if cfg.GetShardDuration() != 7*24*time.Hour {
		t.Errorf("expected 7 days, got %v", cfg.GetShardDuration())
	}
}

func TestPoint_SetField(t *testing.T) {
	p := &Point{}
	p.SetField("value", int64(42))
	if p.Fields == nil {
		t.Fatal("expected non-nil Fields")
	}
	if p.Fields["value"].GetIntValue() != 42 {
		t.Errorf("expected 42, got %v", p.Fields["value"].GetIntValue())
	}
}

func TestPoint_SetField_Overwrite(t *testing.T) {
	p := &Point{}
	p.SetField("value", float64(1.0))
	p.SetField("value", float64(2.0))
	if p.Fields["value"].GetFloatValue() != 2.0 {
		t.Errorf("expected 2.0, got %v", p.Fields["value"].GetFloatValue())
	}
}

func TestPoint_GetField(t *testing.T) {
	p := &Point{}
	p.SetField("value", int64(100))
	v := p.GetField("value")
	iv, ok := v.(*FieldValue_IntValue)
	if !ok {
		t.Fatalf("expected *FieldValue_IntValue, got %T", v)
	}
	if iv.IntValue != 100 {
		t.Errorf("expected 100, got %d", iv.IntValue)
	}
}

func TestPoint_GetField_Missing(t *testing.T) {
	p := &Point{}
	v := p.GetField("nonexistent")
	if v != nil {
		t.Errorf("expected nil for missing field, got %v", v)
	}
}

func TestPoint_GetField_NilFields(t *testing.T) {
	p := &Point{}
	v := p.GetField("any")
	if v != nil {
		t.Errorf("expected nil for nil Fields, got %v", v)
	}
}

func TestPoint_GetField_NilFieldValue(t *testing.T) {
	p := &Point{
		Fields: map[string]*FieldValue{
			"key": nil,
		},
	}
	v := p.GetField("key")
	if v != nil {
		t.Errorf("expected nil for nil field value, got %v", v)
	}
}

func TestPointRow_SetField(t *testing.T) {
	pr := &PointRow{}
	pr.SetField("temp", float64(23.5))
	if pr.Fields == nil {
		t.Fatal("expected non-nil Fields")
	}
	if pr.Fields["temp"].GetFloatValue() != 23.5 {
		t.Errorf("expected 23.5, got %v", pr.Fields["temp"].GetFloatValue())
	}
}

func TestPointRow_GetField(t *testing.T) {
	pr := &PointRow{}
	pr.SetField("status", "ok")
	v := pr.GetField("status")
	sv, ok := v.(*FieldValue_StringValue)
	if !ok {
		t.Fatalf("expected *FieldValue_StringValue, got %T", v)
	}
	if sv.StringValue != "ok" {
		t.Errorf("expected 'ok', got '%s'", sv.StringValue)
	}
}

func TestPointRow_GetField_Missing(t *testing.T) {
	pr := &PointRow{}
	v := pr.GetField("missing")
	if v != nil {
		t.Errorf("expected nil, got %v", v)
	}
}

func TestPointRow_GetField_NilFields(t *testing.T) {
	pr := &PointRow{}
	v := pr.GetField("any")
	if v != nil {
		t.Errorf("expected nil, got %v", v)
	}
}

func TestPointRow_ToPoint(t *testing.T) {
	pr := &PointRow{
		Timestamp: 1000,
		Tags:      map[string]string{"host": "s1"},
		Fields: map[string]*FieldValue{
			"value": NewFieldValue(int64(42)),
		},
	}
	p := pr.ToPoint("mydb", "cpu")
	if p == nil {
		t.Fatal("expected non-nil Point")
	}
	if p.Database != "mydb" {
		t.Errorf("expected Database 'mydb', got '%s'", p.Database)
	}
	if p.Measurement != "cpu" {
		t.Errorf("expected Measurement 'cpu', got '%s'", p.Measurement)
	}
	if p.Timestamp != 1000 {
		t.Errorf("expected Timestamp 1000, got %d", p.Timestamp)
	}
	if p.Tags["host"] != "s1" {
		t.Errorf("expected tag host=s1, got %v", p.Tags)
	}
	if p.Fields["value"].GetIntValue() != 42 {
		t.Errorf("expected field value=42, got %v", p.Fields["value"].GetIntValue())
	}
}

func TestPointRow_ToPoint_Nil(t *testing.T) {
	var pr *PointRow
	p := pr.ToPoint("db", "m")
	if p != nil {
		t.Errorf("expected nil for nil PointRow, got %v", p)
	}
}

func TestNewFieldValue_AllTypes(t *testing.T) {
	tests := []struct {
		name  string
		input any
		isNil bool
		check func(*testing.T, *FieldValue)
	}{
		{"int64", int64(1), false, func(t *testing.T, fv *FieldValue) {
			if fv.GetIntValue() != 1 {
				t.Errorf("expected 1, got %d", fv.GetIntValue())
			}
		}},
		{"int", 2, false, func(t *testing.T, fv *FieldValue) {
			if fv.GetIntValue() != 2 {
				t.Errorf("expected 2, got %d", fv.GetIntValue())
			}
		}},
		{"float64", 3.5, false, func(t *testing.T, fv *FieldValue) {
			if fv.GetFloatValue() != 3.5 {
				t.Errorf("expected 3.5, got %f", fv.GetFloatValue())
			}
		}},
		{"float32", float32(4.5), false, func(t *testing.T, fv *FieldValue) {
			if fv.GetFloatValue() != float64(float32(4.5)) {
				t.Errorf("expected float32(4.5), got %f", fv.GetFloatValue())
			}
		}},
		{"string", "test", false, func(t *testing.T, fv *FieldValue) {
			if fv.GetStringValue() != "test" {
				t.Errorf("expected 'test', got '%s'", fv.GetStringValue())
			}
		}},
		{"bool true", true, false, func(t *testing.T, fv *FieldValue) {
			if !fv.GetBoolValue() {
				t.Error("expected true")
			}
		}},
		{"bool false", false, false, func(t *testing.T, fv *FieldValue) {
			if fv.GetBoolValue() {
				t.Error("expected false")
			}
		}},
		{"nil", nil, true, nil},
		{"uint64", uint64(5), true, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fv := NewFieldValue(tt.input)
			if tt.isNil {
				if fv != nil {
					t.Errorf("expected nil, got %v", fv)
				}
				return
			}
			if fv == nil {
				t.Fatal("expected non-nil")
			}
			tt.check(t, fv)
		})
	}
}
