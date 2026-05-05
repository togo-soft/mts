// internal/storage/shard/wal_bench_test.go
package shard

import (
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

// 创建典型的时间序列数据点
func createBenchPoint() *types.Point {
	return &types.Point{
		Database:    "testdb",
		Measurement: "cpu",
		Tags: map[string]string{
			"host":   "server1",
			"region": "us-west",
			"env":    "prod",
		},
		Timestamp: 1234567890000000000,
		Fields: map[string]any{
			"usage":       float64(85.5),
			"temperature": int64(65),
			"status":      "active",
			"flag":        true,
		},
	}
}

// BenchmarkEncodePoint 测试 WAL 序列化性能
// 这是写入路径的核心，1000 tps 意味着每秒需要序列化 1000 次
func BenchmarkEncodePoint(b *testing.B) {
	p := createBenchPoint()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := serializePoint(p)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSerializePointParallel 测试并发序列化性能
// 模拟 1000 tps 的并发写入场景
func BenchmarkSerializePointParallel(b *testing.B) {
	p := createBenchPoint()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := serializePoint(p)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkDeserializePoint 测试 WAL 反序列化性能
// 恢复时需要频繁调用
func BenchmarkDeserializePoint(b *testing.B) {
	p := createBenchPoint()
	data, err := serializePoint(p)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := deserializePoint(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
