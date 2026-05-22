// tests/e2e/write_1m_pprof/main.go
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/data_gen"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
)

// countSSTables 递归统计 dataDir 下所有有效 SSTable 文件数（验证魔数过滤零填充文件）。
func countSSTables(dataDir string) int {
	count := 0
	_ = filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() || filepath.Ext(info.Name()) != ".bin" {
			return nil
		}
		if !isValidSSTable(path) {
			return nil
		}
		count++
		return nil
	})
	return count
}

// isValidSSTable 验证文件是否为有效的 SSTable（魔数检查）。
func isValidSSTable(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()

	var magic [8]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return false
	}
	return magic == sstable.Magic
}

// waitForCompaction 等待 compaction 完成，每个 shard 的有效 SSTable ≤ maxSSTPerShard 或总计数稳定。
func waitForCompaction(dataDir string) {
	const pollInterval = 2 * time.Second
	const stableRounds = 3
	const maxSSTPerShard = 4 // 与 CompactionConfig.MaxSstableCount 一致

	// 先等待所有后台 flush 完全结束
	time.Sleep(3 * time.Second)

	printShardBreakdown(dataDir)

	var prevCount int
	stableCount := 0

	for round := 0; round < 60; round++ {
		time.Sleep(pollInterval)
		current := countSSTables(dataDir)
		elapsed := time.Duration(round+1)*pollInterval + 3*time.Second
		allSettled := allShardsSettled(dataDir, maxSSTPerShard)

		if current == prevCount && allSettled {
			stableCount++
			fmt.Printf("  [%v] SSTables: %d (stable %d/%d, all shards settled)\n",
				elapsed.Round(100*time.Millisecond), current, stableCount, stableRounds)
			if stableCount >= stableRounds {
				fmt.Printf("Compaction settled: %d SSTables after %v\n", current, elapsed.Round(100*time.Millisecond))
				printShardBreakdown(dataDir)
				return
			}
		} else if current == prevCount {
			stableCount++
			fmt.Printf("  [%v] SSTables: %d (stable %d/%d)\n",
				elapsed.Round(100*time.Millisecond), current, stableCount, stableRounds)
			if stableCount >= stableRounds && allSettled {
				fmt.Printf("Compaction settled: %d SSTables after %v\n", current, elapsed.Round(100*time.Millisecond))
				printShardBreakdown(dataDir)
				return
			}
		} else {
			stableCount = 0
			delta := ""
			if prevCount > 0 {
				delta = fmt.Sprintf(" (was %d)", prevCount)
			}
			fmt.Printf("  [%v] SSTables: %d%s\n", elapsed.Round(100*time.Millisecond), current, delta)
			printShardBreakdown(dataDir)
		}
		prevCount = current
	}

	fmt.Println("Warning: compaction did not settle within timeout")
	printShardBreakdown(dataDir)
}

// allShardsSettled 检查所有 shard 的有效 SSTable 是否均不超过 maxFiles。
func allShardsSettled(dataDir string, maxFiles int) bool {
	entries, _ := os.ReadDir(dataDir)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		shardDir := filepath.Join(dataDir, e.Name())
		dataSubDir := filepath.Join(shardDir, "data")
		if info, err := os.Stat(dataSubDir); err != nil || !info.IsDir() {
			continue
		}
		if countSSTables(shardDir) > maxFiles {
			return false
		}
	}
	return true
}

// printShardBreakdown 输出每个 shard 的有效 SSTable 文件数。
func printShardBreakdown(dataDir string) {
	entries, _ := os.ReadDir(dataDir)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		shardDir := filepath.Join(dataDir, e.Name())
		dataSubDir := filepath.Join(shardDir, "data")
		if info, err := os.Stat(dataSubDir); err != nil || !info.IsDir() {
			continue
		}
		count := countSSTables(shardDir)
		total := countAllBin(shardDir)
		fmt.Printf("    shard %s: %d valid / %d total .bin files\n", e.Name(), count, total)
	}
}

// countAllBin 计数所有 .bin 文件（含无效文件）。
func countAllBin(dataDir string) int {
	count := 0
	_ = filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && filepath.Ext(info.Name()) == ".bin" {
			count++
		}
		return nil
	})
	return count
}

// unixTmpDir 返回跨平台安全的临时目录路径（始终使用正斜杠）。
func unixTmpDir() string {
	tmp := os.TempDir()
	// 替换反斜杠为正斜杠，确保跨平台一致性
	return strings.ReplaceAll(tmp, string(os.PathSeparator), "/")
}

func main() {
	// 开启 pprof，使用正斜杠路径避免 Windows 路径问题
	profilePath := unixTmpDir() + "/memprofile.prof"
	f, err := os.Create(profilePath)
	if err != nil {
		fmt.Printf("Failed to create profile file: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = f.Close() }()

	tmpDir := unixTmpDir() + "/microts_write_pprof"
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cfg := mts.Config{
		DataDir:            tmpDir,
		ShardDurationNanos: int64(time.Hour),
		MemTableCfg: &mts.MemTableConfig{
			FlushMemorySize: 64 * 1024 * 1024,
			FlushPointCount: 3000,
			FlushIdleNanos:  int64(10 * time.Second),
		},
		CompactionCfg: &mts.CompactionConfig{
			MaxSstableCount:    4,
			MaxCompactionBatch: 0,
			ShardSizeLimit:     1 * 1024 * 1024 * 1024,
			CheckIntervalNanos: int64(10 * time.Second),
			TimeoutNanos:       int64(30 * time.Second),
		},
	}

	fmt.Printf("Creating database at: %s\n", tmpDir)
	db, err := mts.Open(&cfg)
	if err != nil {
		fmt.Printf("Open failed: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()
	const count = 1000000

	metrics.GC()
	memBefore := metrics.ReadMemStats()
	fmt.Printf("Before write: %s\n", metrics.FormatMemStats(memBefore))

	writeTimer := metrics.NewWriteSummary(count)
	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(10*time.Millisecond)
		p := gen.GeneratePoint("db1", "cpu", ts)
		if err := db.Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			os.Exit(1)
		}
		if i%100000 == 0 && i > 0 {
			fmt.Printf("Progress: %d/%d\n", i, count)
		}
	}
	writeTimer.Finish()
	fmt.Printf("\n%s\n", writeTimer.Format())

	// 等待 compaction 完成
	fmt.Println("\nFlushing MemTable...")
	if err := db.FlushAll(); err != nil {
		fmt.Printf("FlushAll failed: %v\n", err)
	}
	fmt.Println("Waiting for compaction to settle...")
	waitForCompaction(tmpDir + "/db1/cpu")

	metrics.GC()
	memAfter := metrics.ReadMemStats()
	fmt.Printf("\nAfter write+compaction: %s\n", metrics.FormatMemStats(memAfter))

	// 写入堆 profile
	runtime.GC() // 先 GC 获得更准确的 profile
	if err := pprof.WriteHeapProfile(f); err != nil {
		fmt.Printf("Failed to write heap profile: %v\n", err)
		os.Exit(1)
	}
	_ = f.Close()

	fmt.Printf("\nHeap profile saved to: %s\n", profilePath)
	fmt.Printf("To analyze: go tool pprof %s\n", profilePath)

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

	// 统计存储
	fmt.Printf("\n%s\n", metrics.FormatStorageReport(tmpDir+"/db1/cpu", count, 80))
}
