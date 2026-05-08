// internal/storage/shard/memtable_bench_test.go
package shard

import (
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/types"
)

// createBenchPoint 创建典型的时间序列数据点。
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
		Fields: map[string]*types.FieldValue{
			"usage":       types.NewFieldValue(float64(85.5)),
			"temperature": types.NewFieldValue(int64(65)),
			"status":      types.NewFieldValue("active"),
			"flag":        types.NewFieldValue(true),
		},
	}
}

// BenchmarkMemTableWrite 测试 MemTable 写入性能
// 1000 tps 写入， MemTable 是热路径
func BenchmarkMemTableWrite(b *testing.B) {
	mt := NewMemTable(DefaultMemTableConfig())
	p := createBenchPoint()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 每次写入需要不同的时间戳以避免去重
		p.Timestamp = time.Now().UnixNano() + int64(i)
		err := mt.Write(p)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMemTableWriteParallel 测试并发写入性能
// 模拟 1000 tps 并发场景
func BenchmarkMemTableWriteParallel(b *testing.B) {
	mt := NewMemTable(DefaultMemTableConfig())
	p := createBenchPoint()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			p.Timestamp = time.Now().UnixNano() + i
			err := mt.Write(p)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkMemTableFlush 测试 Flush 性能
// Flush 时会复制所有 entries
func BenchmarkMemTableFlush(b *testing.B) {
	// 使用固定次数，避免 b.N 过大导致准备阶段耗时过长
	const numTables = 100
	mts := make([]*MemTable, numTables)
	for i := 0; i < numTables; i++ {
		mts[i] = NewMemTable(DefaultMemTableConfig())
		for j := 0; j < 1000; j++ {
			p := createBenchPoint()
			p.Timestamp = int64(1000000000000000000 + j*1e9)
			_ = mts[i].Write(p)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = mts[i%numTables].Flush()
	}
}

// BenchmarkMemTableShouldFlush 检查是否需要刷盘
func BenchmarkMemTableShouldFlush(b *testing.B) {
	mt := NewMemTable(DefaultMemTableConfig())

	// 预填充数据
	for i := 0; i < 1000; i++ {
		p := createBenchPoint()
		p.Timestamp = time.Now().UnixNano() + int64(i)*1e9
		_ = mt.Write(p)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = mt.ShouldFlush()
	}
}
