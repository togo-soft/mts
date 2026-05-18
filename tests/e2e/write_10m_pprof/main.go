// tests/e2e/write_10m_pprof/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/data_gen"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
)

func main() {
	// 开启 pprof
	f, err := os.Create(filepath.Join(os.TempDir(), "memprofile_10m.prof"))
	if err != nil {
		fmt.Printf("Failed to create profile file: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = f.Close() }()

	// 开启 CPU profile
	cpuF, err := os.Create(filepath.Join(os.TempDir(), "cpuprofile_10m.prof"))
	if err != nil {
		fmt.Printf("Failed to create cpu profile file: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = cpuF.Close() }()
	if err := pprof.StartCPUProfile(cpuF); err != nil {
		fmt.Printf("Failed to start CPU profile: %v\n", err)
		os.Exit(1)
	}
	defer pprof.StopCPUProfile()

	tmpDir := filepath.Join(os.TempDir(), "microts_write_10m_pprof")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cfg := microts.Config{
		DataDir:            tmpDir,
		ShardDurationNanos: int64(time.Hour),
		MemTableCfg: &microts.MemTableConfig{
			FlushMemorySize: 256 * 1024 * 1024,
			FlushPointCount: 50000,
			FlushIdleNanos:  int64(10 * time.Second),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSstableCount:    4,
			MaxCompactionBatch: 0,
			ShardSizeLimit:     1 * 1024 * 1024 * 1024,
			CheckIntervalNanos: int64(10 * time.Second),
			TimeoutNanos:       int64(30 * time.Second),
		},
	}

	db, err := microts.Open(&cfg)
	if err != nil {
		fmt.Printf("Open failed: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()
	const count = 10_000_000

	metrics.GC()
	memBefore := metrics.ReadMemStats()
	fmt.Printf("Before write: %s\n", metrics.FormatMemStats(memBefore))

	writeTimer := metrics.NewWriteSummary(count)
	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint("db1", "cpu", ts)
		if err := db.Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			os.Exit(1)
		}
		if i%1_000_000 == 0 && i > 0 {
			fmt.Printf("Progress: %d/%d\n", i, count)
		}
	}
	writeTimer.Finish()
	fmt.Printf("\n%s\n", writeTimer.Format())

	// 等待后台 flush 完成
	fmt.Println("\nWaiting for background flush to complete...")
	time.Sleep(5 * time.Second)

	metrics.GC()
	memAfter := metrics.ReadMemStats()
	fmt.Printf("\nAfter write: %s\n", metrics.FormatMemStats(memAfter))

	delta := metrics.CalcDelta(memBefore, memAfter)
	fmt.Printf("Memory delta: %s\n", delta.Format())

	// 写入堆 profile
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		fmt.Printf("Failed to write heap profile: %v\n", err)
		os.Exit(1)
	}
	_ = f.Close()

	pprof.StopCPUProfile()
	_ = cpuF.Close()

	fmt.Printf("\nHeap profile saved to: %s\n", filepath.Join(os.TempDir(), "memprofile_10m.prof"))
	fmt.Printf("CPU profile saved to: %s\n", filepath.Join(os.TempDir(), "cpuprofile_10m.prof"))
	fmt.Printf("To analyze heap: go tool pprof %s\n", filepath.Join(os.TempDir(), "memprofile_10m.prof"))
	fmt.Printf("To analyze cpu: go tool pprof %s\n", filepath.Join(os.TempDir(), "cpuprofile_10m.prof"))

	// 打印内存统计
	var mstats runtime.MemStats
	runtime.ReadMemStats(&mstats)
	fmt.Printf("\nGo Runtime Stats:\n")
	fmt.Printf("  HeapAlloc: %d MB\n", mstats.HeapAlloc/1024/1024)
	fmt.Printf("  HeapSys: %d MB\n", mstats.HeapSys/1024/1024)
	fmt.Printf("  HeapIdle: %d MB\n", mstats.HeapIdle/1024/1024)
	fmt.Printf("  HeapInuse: %d MB\n", mstats.HeapInuse/1024/1024)
	fmt.Printf("  StackInuse: %d MB\n", mstats.StackInuse/1024/1024)
	fmt.Printf("  TotalAlloc: %d MB\n", mstats.TotalAlloc/1024/1024)
	fmt.Printf("  NumGC: %d\n", mstats.NumGC)
	fmt.Printf("  PauseTotalNs: %d ms\n", mstats.PauseTotalNs/1_000_000)

	// 统计存储
	fmt.Printf("\n%s\n", metrics.FormatStorageReport(filepath.Join(tmpDir, "db1", "cpu"), count, 80))
}
