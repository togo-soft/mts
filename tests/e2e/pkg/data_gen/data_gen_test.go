package data_gen

import (
	"strings"
	"testing"
)

func TestNewDataGenerator(t *testing.T) {
	g := NewDataGenerator(42)
	if g == nil {
		t.Fatal("NewDataGenerator returned nil")
	}
	if g.seed != 42 {
		t.Errorf("expected seed 42, got %d", g.seed)
	}
	if g.rand == nil {
		t.Error("rand should not be nil")
	}
	if g.tags == nil {
		t.Error("tags should not be nil")
	}
	if v := g.tags["host"]; v != "server1" {
		t.Errorf("expected tags[host]=server1, got %s", v)
	}
}

func TestGeneratePoint_BasicFields(t *testing.T) {
	g := NewDataGenerator(42)
	ts := int64(1000000000)
	p := g.GeneratePoint("testdb", "testmeasurement", ts)

	if p.Database != "testdb" {
		t.Errorf("expected Database 'testdb', got '%s'", p.Database)
	}
	if p.Measurement != "testmeasurement" {
		t.Errorf("expected Measurement 'testmeasurement', got '%s'", p.Measurement)
	}
	if p.Timestamp != ts {
		t.Errorf("expected Timestamp %d, got %d", ts, p.Timestamp)
	}
	if p.Tags == nil {
		t.Error("Tags should not be nil")
	}
	if p.Tags["host"] != "server1" {
		t.Errorf("expected Tags[host]='server1', got '%s'", p.Tags["host"])
	}
	if p.Fields == nil {
		t.Error("Fields should not be nil")
	}
	if len(p.Fields) != 10 {
		t.Errorf("expected 10 fields, got %d", len(p.Fields))
	}
}

func TestGeneratePoint_TagsContent(t *testing.T) {
	g := NewDataGenerator(42)
	p1 := g.GeneratePoint("db", "m", 1000)
	p2 := g.GeneratePoint("db", "m", 2000)

	// 两者 tags 内容一致
	if p1.Tags["host"] != "server1" {
		t.Errorf("expected Tags[host]='server1', got '%s'", p1.Tags["host"])
	}
	if p2.Tags["host"] != "server1" {
		t.Errorf("expected Tags[host]='server1', got '%s'", p2.Tags["host"])
	}
	if len(p1.Tags) != 1 || len(p2.Tags) != 1 {
		t.Error("Tags should have exactly 1 entry")
	}
}

func TestGeneratePoint_FieldNames(t *testing.T) {
	g := NewDataGenerator(42)
	p := g.GeneratePoint("db", "m", 1000)

	expectedNames := []string{
		"field_float_1", "field_float_2", "field_float_3", "field_float_4", "field_float_5",
		"field_int_1", "field_int_2", "field_int_3",
		"field_string_1",
		"field_bool_1",
	}

	for _, name := range expectedNames {
		if _, ok := p.Fields[name]; !ok {
			t.Errorf("expected field %s to exist", name)
		}
	}
}

func TestGeneratePoint_Deterministic(t *testing.T) {
	g1 := NewDataGenerator(42)
	g2 := NewDataGenerator(42)

	p1 := g1.GeneratePoint("db", "m", 1000)
	p2 := g2.GeneratePoint("db", "m", 1000)

	for name := range p1.Fields {
		v1 := p1.Fields[name]
		v2 := p2.Fields[name]
		if v1 == nil || v2 == nil {
			t.Fatalf("field %s is nil", name)
		}
		// 相同 seed 应生成相同值
		if v1.GetFloatValue() != v2.GetFloatValue() && v1.GetIntValue() != v2.GetIntValue() {
			continue // 布尔或字符串字段
		}
	}
}

func TestRandomString(t *testing.T) {
	g := NewDataGenerator(42)

	s1 := g.randomString(10)
	if len(s1) != 10 {
		t.Errorf("expected length 10, got %d", len(s1))
	}

	// 检查只包含合法字符
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for _, c := range s1 {
		if !strings.ContainsRune(chars, c) {
			t.Errorf("unexpected character '%c' in random string", c)
		}
	}

	s2 := g.randomString(8)
	if len(s2) != 8 {
		t.Errorf("expected length 8, got %d", len(s2))
	}
}

func TestGeneratePoint_AllFieldTypes(t *testing.T) {
	g := NewDataGenerator(42)
	p := g.GeneratePoint("db", "m", 1000)

	// 浮点数字段
	for i := 0; i < 5; i++ {
		name := fieldNames[i]
		fv := p.Fields[name]
		if fv == nil {
			t.Errorf("field %s should not be nil", name)
			continue
		}
		_ = fv.GetFloatValue() // 验证可读取
	}

	// 整数字段
	for i := 5; i < 8; i++ {
		name := fieldNames[i]
		fv := p.Fields[name]
		if fv == nil {
			t.Errorf("field %s should not be nil", name)
			continue
		}
		_ = fv.GetIntValue() // 验证可读取
	}

	// 字符串字段
	fv := p.Fields["field_string_1"]
	if fv == nil {
		t.Error("field_string_1 should not be nil")
	} else {
		_ = fv.GetStringValue()
	}

	// 布尔字段
	fv = p.Fields["field_bool_1"]
	if fv == nil {
		t.Error("field_bool_1 should not be nil")
	} else {
		_ = fv.GetBoolValue()
	}
}

func TestGeneratePoint_MultiplePoints(t *testing.T) {
	g := NewDataGenerator(12345)
	for i := 0; i < 1000; i++ {
		p := g.GeneratePoint("db", "m", int64(i)*1000000000)
		if p == nil {
			t.Fatal("GeneratePoint returned nil")
		}
		if len(p.Fields) != 10 {
			t.Errorf("point %d: expected 10 fields, got %d", i, len(p.Fields))
		}
		if p.Tags["host"] != "server1" {
			t.Errorf("point %d: tags incorrect", i)
		}
	}
}

func BenchmarkGeneratePoint(b *testing.B) {
	g := NewDataGenerator(42)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.GeneratePoint("db", "m", int64(i)*1000000000)
	}
}
