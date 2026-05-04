// tests/e2e/write_1m/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"micro-ts"
	"micro-ts/internal/storage/shard"
	"micro-ts/tests/e2e/pkg/data_gen"
	"micro-ts/tests/e2e/pkg/metrics"
)

func main() {
	tmpDir := filepath.Join(os.TempDir(), "microts_write_1m")
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	cfg := microts.Config{
		DataDir:        tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: shard.MemTableConfig{
			MaxSize:      64 * 1024 * 1024,
			MaxCount:     3000,
			IdleDuration: 10 * time.Second,
		},
	}

	db, err := microts.Open(cfg)
	if err != nil {
		fmt.Printf("Open failed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// GC 并记录初始内存
	metrics.GC()
	memBefore := metrics.ReadMemStats()
	fmt.Printf("Before write: %s\n", metrics.FormatMemStats(memBefore))

	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()
	const count = 1000000

	timer := metrics.NewTimer()
	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint("db1", "cpu", ts)
		if err := db.Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			os.Exit(1)
		}
	}
	elapsed := timer.Elapsed()

	// GC 后记录最终内存
	metrics.GC()
	memAfter := metrics.ReadMemStats()
	delta := metrics.CalcDelta(memBefore, memAfter)

	fmt.Printf("Write 1M: %d points in %v, TPS: %.2f\n", count, elapsed, metrics.TPS(count, elapsed))
	fmt.Printf("After write: %s\n", metrics.FormatMemStats(memAfter))
	fmt.Printf("Memory delta: %s\n", delta.Format())
}