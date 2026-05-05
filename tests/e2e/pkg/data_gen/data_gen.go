// tests/e2e/pkg/data_gen/data_gen.go
package data_gen

import (
	"math/rand"
	"time"

	"micro-ts/types"
)

// DataGenerator 数据生成器
type DataGenerator struct {
	seed     int64
	rand     *rand.Rand
	baseTime int64
}

// NewDataGenerator 创建生成器
func NewDataGenerator(seed int64) *DataGenerator {
	return &DataGenerator{
		seed:     seed,
		rand:     rand.New(rand.NewSource(seed)),
		baseTime: time.Now().UnixNano(),
	}
}

// GeneratePoint 生成单个数据点
func (g *DataGenerator) GeneratePoint(db, measurement string, timestamp int64) *types.Point {
	return &types.Point{
		Database:    db,
		Measurement: measurement,
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   timestamp,
		Fields:      g.generateFields(),
	}
}

// generateFields 生成 10 个字段
func (g *DataGenerator) generateFields() map[string]any {
	fields := make(map[string]any)

	// 5 个浮点数: field_float_1 ~ field_float_5
	for i := 1; i <= 5; i++ {
		fields["field_float_"+itoa(i)] = g.rand.Float64() * 1000
	}

	// 3 个整数: field_int_1 ~ field_int_3
	for i := 1; i <= 3; i++ {
		fields["field_int_"+itoa(i)] = int64(g.rand.Intn(100000))
	}

	// 1 个字符串
	fields["field_string_1"] = g.randomString(8 + g.rand.Intn(9))

	// 1 个布尔
	fields["field_bool_1"] = g.rand.Intn(2) == 1

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

// itoa converts int to string without importing strconv
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	idx := len(b)
	for i > 0 {
		idx--
		b[idx] = byte('0' + i%10)
		i /= 10
	}
	return string(b[idx:])
}
