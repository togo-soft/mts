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

const fieldPoolSize = 1024

// DataGenerator 数据生成器
type DataGenerator struct {
	seed       int64
	rand       *rand.Rand
	baseTime   int64
	tags       map[string]string              // 共享的只读 tags，避免每次分配
	fieldPools []map[string]*types.FieldValue // 预生成的 field map 池，随机选取复用
}

// NewDataGenerator 创建生成器
func NewDataGenerator(seed int64) *DataGenerator {
	rng := rand.New(rand.NewSource(seed))
	pools := make([]map[string]*types.FieldValue, fieldPoolSize)
	for i := 0; i < fieldPoolSize; i++ {
		pools[i] = generateFieldMap(rng)
	}
	return &DataGenerator{
		seed:       seed,
		rand:       rng,
		baseTime:   time.Now().UnixNano(),
		tags:       map[string]string{"host": "server1"},
		fieldPools: pools,
	}
}

// GeneratePoint 生成单个数据点
func (g *DataGenerator) GeneratePoint(db, measurement string, timestamp int64) *types.Point {
	return &types.Point{
		Database:    db,
		Measurement: measurement,
		Tags:        g.tags,
		Timestamp:   timestamp,
		Fields:      g.fieldPools[g.rand.Intn(fieldPoolSize)],
	}
}

// generateFieldMap 生成一个包含 10 个字段的 map
func generateFieldMap(rng *rand.Rand) map[string]*types.FieldValue {
	fields := make(map[string]*types.FieldValue, 10)

	// 5 个浮点数: field_float_1 ~ field_float_5
	for i := 0; i < 5; i++ {
		fields[fieldNames[i]] = types.NewFieldValue(rng.Float64() * 1000)
	}

	// 3 个整数: field_int_1 ~ field_int_3
	for i := 5; i < 8; i++ {
		fields[fieldNames[i]] = types.NewFieldValue(int64(rng.Intn(100000)))
	}

	// 1 个字符串
	fields[fieldNames[8]] = types.NewFieldValue(randomString(rng, 8+rng.Intn(9)))

	// 1 个布尔
	fields[fieldNames[9]] = types.NewFieldValue(rng.Intn(2) == 1)

	return fields
}

// randomString 生成随机字符串
func randomString(rng *rand.Rand, length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[rng.Intn(len(chars))]
	}
	return string(b)
}
