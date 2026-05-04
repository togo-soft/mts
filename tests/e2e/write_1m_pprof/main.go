// tests/e2e/write_1m_pprof/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"micro-ts"
	"micro-ts/internal/storage/shard"
	"micro-ts/tests/e2e/pkg/data_gen"
	"micro-ts/tests/e2e/pkg/metrics"
)

func main() {
	// 开启 pprof
	f, err := os.Create(filepath.Join(os.TempDir(), "memprofile.prof"))
	if err != nil {
		fmt.Printf("Failed to create profile file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	tmpDir := filepath.Join(os.TempDir(), "microts_write_pprof")
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

	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()
	const count = 1000000

	metrics.GC()
	memBefore := metrics.ReadMemStats()
	fmt.Printf("Before write: %s\n", metrics.FormatMemStats(memBefore))

	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint("db1", "cpu", ts)
		if err := db.Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			os.Exit(1)
		}
		if i%100000 == 0 && i > 0 {
			fmt.Printf("Progress: %d/%d\n", i, count)
		}
	}

	metrics.GC()
	memAfter := metrics.ReadMemStats()
	fmt.Printf("\nAfter write: %s\n", metrics.FormatMemStats(memAfter))

	// 写入堆 profile
	runtime.GC() // 先 GC 获得更准确的 profile
	if err := pprof.WriteHeapProfile(f); err != nil {
		fmt.Printf("Failed to write heap profile: %v\n", err)
		os.Exit(1)
	}
	f.Close()

	fmt.Printf("\nHeap profile saved to: %s\n", filepath.Join(os.TempDir(), "memprofile.prof"))
	fmt.Printf("To analyze: go tool pprof %s\n", filepath.Join(os.TempDir(), "memprofile.prof"))

	// 打印内存统计
	var mstats runtime.MemStats
	runtime.ReadMemStats(&mstats)
	fmt.Printf("\nGo Runtime Stats:\n")
	fmt.Printf("  HeapAlloc: %d MB\n", mstats.HeapAlloc/1024/1024)
	fmt.Printf("  HeapSys: %d MB\n", mstats.HeapSys/1024/1024)
	fmt.Printf("  HeapIdle: %d MB\n", mstats.HeapIdle/1024/1024)
	fmt.Printf("  HeapInuse: %d MB\n", mstats.HeapInuse/1024/1024)
	fmt.Printf("  StackInuse: %d MB\n", mstats.StackInuse/1024/1024)
	fmt.Printf("  BuckHashSys: %d MB\n", mstats.BuckHashSys/1024/1024)
	fmt.Printf("  GCSys: %d MB\n", mstats.GCSys/1024/1024)
	fmt.Printf("  OtherSys: %d MB\n", mstats.OtherSys/1024/1024)
}