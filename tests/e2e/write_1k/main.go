// tests/e2e/write_1k/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/data_gen"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
)

func main() {
	tmpDir := filepath.Join(os.TempDir(), "microts_write_1k")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           64 * 1024 * 1024,
			MaxCount:          3000,
			IdleDurationNanos: int64(10 * time.Second),
		},
	}

	db, err := microts.Open(cfg)
	if err != nil {
		fmt.Printf("Open failed: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	// GC 并记录初始内存
	metrics.GC()
	memBefore := metrics.ReadMemStats()
	fmt.Printf("Before write: %s\n", metrics.FormatMemStats(memBefore))

	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()
	const count = 1000

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

	fmt.Printf("Write 1K: %d points in %v, TPS: %.2f\n", count, elapsed, metrics.TPS(count, elapsed))
	fmt.Printf("After write: %s\n", metrics.FormatMemStats(memAfter))
	fmt.Printf("Memory delta: %s\n", delta.Format())

	// 等待数据落盘后统计存储
	fmt.Printf("\nWaiting for data flush...\n")
	time.Sleep(2 * time.Second)

	dataDir := filepath.Join(tmpDir, "db1", "cpu")
	fmt.Printf("\n%s\n", metrics.FormatStorageReport(dataDir, count, 80))
}
