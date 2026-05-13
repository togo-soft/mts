package data_gen

import (
	"math/rand"
	"strings"
	"testing"

	"codeberg.org/micro-ts/mts/types"
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
	if len(g.fieldPools) != fieldPoolSize {
		t.Errorf("expected fieldPools size %d, got %d", fieldPoolSize, len(g.fieldPools))
	}
}

func TestFieldPool_AllMapsValid(t *testing.T) {
	g := NewDataGenerator(42)
	for i, fm := range g.fieldPools {
		if len(fm) != 10 {
			t.Errorf("pool[%d]: expected 10 fields, got %d", i, len(fm))
		}
		for _, name := range fieldNames {
			if _, ok := fm[name]; !ok {
				t.Errorf("pool[%d]: missing field %s", i, name)
			}
		}
	}
}

func TestFieldPool_Deterministic(t *testing.T) {
	g1 := NewDataGenerator(42)
	g2 := NewDataGenerator(42)

	// 相同 seed 生成相同的 pool
	if len(g1.fieldPools) != len(g2.fieldPools) {
		t.Fatal("pool sizes differ")
	}
	for i := range g1.fieldPools {
		fv1 := g1.fieldPools[i]["field_float_1"].GetFloatValue()
		fv2 := g2.fieldPools[i]["field_float_1"].GetFloatValue()
		if fv1 != fv2 {
			t.Errorf("pool[%d]: float values differ: %f vs %f", i, fv1, fv2)
			return
		}
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

	for _, name := range fieldNames {
		if _, ok := p.Fields[name]; !ok {
			t.Errorf("expected field %s to exist", name)
		}
	}
}

func TestGeneratePoint_FieldsReused(t *testing.T) {
	g := NewDataGenerator(42)

	// 收集 pool 中所有 field_float_1 的 FieldValue 指针
	poolRefs := make(map[*types.FieldValue]bool)
	for _, fm := range g.fieldPools {
		poolRefs[fm["field_float_1"]] = true
	}

	// 多次调用 GeneratePoint，验证返回的 Fields 都来自预生成的 pool
	for i := 0; i < 100; i++ {
		p := g.GeneratePoint("db", "m", int64(i))
		fv := p.Fields["field_float_1"]
		if !poolRefs[fv] {
			t.Errorf("point %d: field_float_1 value not from pool (allocated outside pool)", i)
			return
		}
	}
}

func TestRandomString(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	s1 := randomString(rng, 10)
	if len(s1) != 10 {
		t.Errorf("expected length 10, got %d", len(s1))
	}

	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for _, c := range s1 {
		if !strings.ContainsRune(chars, c) {
			t.Errorf("unexpected character '%c' in random string", c)
		}
	}

	s2 := randomString(rng, 8)
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
		_ = fv.GetFloatValue()
	}

	// 整数字段
	for i := 5; i < 8; i++ {
		name := fieldNames[i]
		fv := p.Fields[name]
		if fv == nil {
			t.Errorf("field %s should not be nil", name)
			continue
		}
		_ = fv.GetIntValue()
	}

	// 字符串字段
	fv := p.Fields[fieldNames[8]]
	if fv == nil {
		t.Error("field_string_1 should not be nil")
	} else {
		_ = fv.GetStringValue()
	}

	// 布尔字段
	fv = p.Fields[fieldNames[9]]
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
