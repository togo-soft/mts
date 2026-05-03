// tests/bench_test.go
package tests

import (
	"context"
	"runtime"
	"testing"
	"time"

	"micro-ts"
	"micro-ts/internal/types"
)

func runGC() {
	runtime.GC()
}

// memStats 内存统计
type memStats struct {
	alloc uint64
}

func getMemStats() memStats {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return memStats{alloc: stats.Alloc}
}

func BenchmarkWrite_1K(b *testing.B) {
	benchmarkWrite(b, 1000)
}

func BenchmarkWrite_10K(b *testing.B) {
	benchmarkWrite(b, 10000)
}

func BenchmarkWrite_100K(b *testing.B) {
	benchmarkWrite(b, 100000)
}

func BenchmarkWrite_1M(b *testing.B) {
	benchmarkWrite(b, 1000000)
}

func benchmarkWrite(b *testing.B, count int) {
	runGC()
	memBefore := getMemStats()
	timeBefore := time.Now()

	db, err := microts.Open(microts.Config{
		DataDir:       b.TempDir(),
		ShardDuration: time.Hour,
	})
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	ctx := context.Background()
	gen := NewDataGenerator(42)
	baseTime := time.Now().UnixNano()

	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint("db1", "cpu", ts)
		if err := db.Write(ctx, p); err != nil {
			b.Fatalf("Write failed at %d: %v", i, err)
		}
	}

	timeAfter := time.Now()
	memAfter := getMemStats()
	runGC()

	elapsed := timeAfter.Sub(timeBefore)
	tps := float64(count) / elapsed.Seconds()
	memDelta := memAfter.alloc - memBefore.alloc

	b.ReportMetric(tps, "writes/sec")
	b.ReportMetric(float64(memDelta)/1024/1024, "MB_allocated")
	b.ReportAllocs()

	_ = db.Close()
}

func BenchmarkQuery_1K(b *testing.B) {
	benchmarkQuery(b, 1000)
}

func BenchmarkQuery_10K(b *testing.B) {
	benchmarkQuery(b, 10000)
}

func BenchmarkQuery_100K(b *testing.B) {
	benchmarkQuery(b, 100000)
}

func BenchmarkQuery_1M(b *testing.B) {
	benchmarkQuery(b, 1000000)
}

func benchmarkQuery(b *testing.B, count int) {
	// 先写入数据
	db, err := microts.Open(microts.Config{
		DataDir:       b.TempDir(),
		ShardDuration: time.Hour,
	})
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	ctx := context.Background()
	gen := NewDataGenerator(42)
	baseTime := time.Now().UnixNano()

	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint("db1", "cpu", ts)
		_ = db.Write(ctx, p)
	}

	b.ResetTimer()
	runGC()
	memBefore := getMemStats()

	// 执行查询
	resp, err := db.QueryRange(ctx, &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   baseTime,
		EndTime:     baseTime + int64(count)*int64(time.Second),
		Limit:       0,
	})
	if err != nil {
		b.Fatalf("Query failed: %v", err)
	}

	memAfter := getMemStats()
	runGC()

	memDelta := memAfter.alloc - memBefore.alloc

	b.ReportMetric(float64(len(resp.Rows))/b.Elapsed().Seconds(), "reads/sec")
	b.ReportMetric(float64(memDelta)/1024/1024, "MB_allocated")
	b.ReportAllocs()

	_ = db.Close()
}
