// tests/e2e/pkg/data_gen/data_gen.go
package data_gen

import (
	"math/rand"
	"time"

	"codeberg.org/micro-ts/mts/types"
)

// 预计算字段名，避免每次 GeneratePoint 重复分配字符串
var fieldNames = []string{
	"field_float_1", "field_float_2", "field_float_3", "field_float_4", "field_float_5",
	"field_int_1", "field_int_2", "field_int_3",
	"field_string_1",
	"field_bool_1",
}

// DataGenerator 数据生成器
type DataGenerator struct {
	seed     int64
	rand     *rand.Rand
	baseTime int64
	tags     map[string]string // 共享的只读 tags，避免每次分配
}

// NewDataGenerator 创建生成器
func NewDataGenerator(seed int64) *DataGenerator {
	return &DataGenerator{
		seed:     seed,
		rand:     rand.New(rand.NewSource(seed)),
		baseTime: time.Now().UnixNano(),
		tags:     map[string]string{"host": "server1"},
	}
}

// GeneratePoint 生成单个数据点
func (g *DataGenerator) GeneratePoint(db, measurement string, timestamp int64) *types.Point {
	return &types.Point{
		Database:    db,
		Measurement: measurement,
		Tags:        g.tags,
		Timestamp:   timestamp,
		Fields:      g.generateFields(),
	}
}

// generateFields 生成 10 个字段
func (g *DataGenerator) generateFields() map[string]*types.FieldValue {
	fields := make(map[string]*types.FieldValue, 10)

	// 5 个浮点数: field_float_1 ~ field_float_5
	for i := 0; i < 5; i++ {
		fields[fieldNames[i]] = types.NewFieldValue(g.rand.Float64() * 1000)
	}

	// 3 个整数: field_int_1 ~ field_int_3
	for i := 5; i < 8; i++ {
		fields[fieldNames[i]] = types.NewFieldValue(int64(g.rand.Intn(100000)))
	}

	// 1 个字符串
	fields[fieldNames[8]] = types.NewFieldValue(g.randomString(8 + g.rand.Intn(9)))

	// 1 个布尔
	fields[fieldNames[9]] = types.NewFieldValue(g.rand.Intn(2) == 1)

	return fields
}

// randomString 生成随机字符串
func (g *DataGenerator) randomString(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[g.rand.Intn(len(chars))]
	}
	return string(b)
}
