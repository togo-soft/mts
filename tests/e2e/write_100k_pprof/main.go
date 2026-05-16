// tests/e2e/write_100k_pprof/main.go
// 写入 100K 数据点并生成 pprof CPU/heap profile 用于性能分析。
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/data_gen"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
)

func unixTmpDir() string {
	return strings.ReplaceAll(os.TempDir(), string(os.PathSeparator), "/")
}

func main() {
	tmpDir := unixTmpDir() + "/microts_write_100k_pprof"
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cpuProfilePath := unixTmpDir() + "/cpu_profile_100k.prof"
	heapProfilePath := unixTmpDir() + "/heap_profile_100k.prof"

	// CPU profile
	cpuF, err := os.Create(cpuProfilePath)
	if err != nil {
		fmt.Printf("Failed to create CPU profile: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = cpuF.Close() }()

	if err := pprof.StartCPUProfile(cpuF); err != nil {
		fmt.Printf("Failed to start CPU profile: %v\n", err)
		os.Exit(1)
	}

	cfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           64 * 1024 * 1024,
			MaxCount:          50000,
			IdleDurationNanos: int64(10 * time.Second),
		},
	}

	fmt.Printf("Temp dir: %s\n", tmpDir)
	fmt.Printf("CPU profile: %s\n", cpuProfilePath)

	db, err := microts.Open(cfg)
	if err != nil {
		fmt.Printf("Open failed: %v\n", err)
		os.Exit(1)
	}

	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()
	const count = 100000

	metrics.GC()
	memBefore := metrics.ReadMemStats()
	fmt.Printf("Before write: %s\n", metrics.FormatMemStats(memBefore))

	// 阶段计时
	var (
		totalStart    = time.Now()
		walTimeTotal  time.Duration
		sidTimeTotal  time.Duration
		mtTimeTotal   time.Duration
		backpressureWait time.Duration
	)

	writeTimer := metrics.NewWriteSummary(count)
	for i := range count {
		ts := baseTime + int64(i)*int64(10*time.Millisecond)
		p := gen.GeneratePoint("db1", "cpu", ts)

		t0 := time.Now()
		if err := db.Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			os.Exit(1)
		}
		elapsed := time.Since(t0)

		// 仅采样每100次写入的耗时，避免计时影响性能
		if i%100 == 0 && i > 0 {
			_ = elapsed
			_ = walTimeTotal
			_ = sidTimeTotal
			_ = mtTimeTotal
			_ = backpressureWait
		}

		if i%20000 == 0 && i > 0 {
			fmt.Printf("Progress: %d/%d (%.0f%%), per-write: %v\n",
				i, count, float64(i)/float64(count)*100, time.Since(totalStart)/time.Duration(i))
		}
	}
	writeTimer.Finish()
	fmt.Printf("\n%s\n", writeTimer.Format())
	fmt.Printf("Write 100K: %d points in %v, TPS: %.2f\n", count, writeTimer.Elapsed(), writeTimer.TPS())

	// 停止 CPU profile
	pprof.StopCPUProfile()
	_ = cpuF.Close()
	fmt.Printf("CPU profile saved to: %s\n", cpuProfilePath)

	// 等待 flush 完成
	fmt.Println("\nFlushing...")
	_ = db.FlushAll()
	time.Sleep(2 * time.Second)

	// 内存统计
	metrics.GC()
	runtime.GC()
	memAfter := metrics.ReadMemStats()
	delta := metrics.CalcDelta(memBefore, memAfter)
	fmt.Printf("After write+flush: %s\n", metrics.FormatMemStats(memAfter))
	fmt.Printf("Memory delta: %s\n", delta.Format())

	// Heap profile
	heapF, err := os.Create(heapProfilePath)
	if err != nil {
		fmt.Printf("Failed to create heap profile: %v\n", err)
	} else {
		defer func() { _ = heapF.Close() }()
		if err := pprof.WriteHeapProfile(heapF); err != nil {
			fmt.Printf("Failed to write heap profile: %v\n", err)
		}
		_ = heapF.Close()
		fmt.Printf("Heap profile saved to: %s\n", heapProfilePath)
	}

	// 运行统计
	var mstats runtime.MemStats
	runtime.ReadMemStats(&mstats)
	fmt.Printf("\nGo Runtime Stats:\n")
	fmt.Printf("  HeapAlloc: %d MB\n", mstats.HeapAlloc/1024/1024)
	fmt.Printf("  TotalAlloc: %d MB\n", mstats.TotalAlloc/1024/1024)
	fmt.Printf("  Sys: %d MB\n", mstats.Sys/1024/1024)
	fmt.Printf("  NumGC: %d\n", mstats.NumGC)
	fmt.Printf("  NumGoroutine: %d\n", runtime.NumGoroutine())

	// 存储统计
	storageDir := filepath.Join(tmpDir, "db1", "cpu")
	fmt.Printf("\n%s\n", metrics.FormatStorageReport(storageDir, count, 80))

	if err := db.Close(); err != nil {
		fmt.Printf("Close failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n=== Analysis commands ===")
	fmt.Printf("go tool pprof -http=:8080 %s\n", cpuProfilePath)
	fmt.Printf("go tool pprof -http=:8081 %s\n", heapProfilePath)
}
