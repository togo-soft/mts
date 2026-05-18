// tests/e2e/diff_compact_disk_usage/main.go
//
// 磁盘空间压缩对比测试：对比 1M 数据点在 none/snappy/lz4 压缩时的磁盘占用和写入性能。
//
// 运行方式：
//
//	cd tests/e2e/diff_compact_disk_usage && go build && ./diff_compact_disk_usage
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/data_gen"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
)

const totalPoints = 1_000_000

type result struct {
	name  string
	size  int64
	files int
	tps   float64
}

// runTest 执行一轮测试，写入数据、等待落盘和 compaction 完成后统计磁盘占用。
func runTest(dataDir string, compression sstable.CompressionAlgorithm, label string) (int64, int, float64, error) {
	_ = os.RemoveAll(dataDir)

	cfg := microts.Config{
		DataDir:       dataDir,
		ShardDurationNanos: int64(time.Hour),
		MemTableCfg: &microts.MemTableConfig{
			FlushMemorySize:       64 * 1024 * 1024,
			FlushPointCount:    50000,
			FlushIdleNanos: int64(30 * time.Second),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSstableCount:    4,
			MaxCompactionBatch: 0,
			ShardSizeLimit:     1 * 1024 * 1024 * 1024,
			CheckIntervalNanos: int64(5 * time.Second),
			TimeoutNanos:       int64(30 * time.Second),
		},
		CompressionAlgorithm: compression,
	}

	db, err := microts.Open(cfg)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("open db: %w", err)
	}

	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()

	timer := metrics.NewWriteSummary(totalPoints)
	for i := 0; i < totalPoints; i++ {
		ts := baseTime + int64(i)*int64(time.Millisecond)
		p := gen.GeneratePoint("db1", "cpu", ts)
		if err := db.Write(context.Background(), p); err != nil {
			_ = db.Close()
			return 0, 0, 0, fmt.Errorf("write point %d: %w", i, err)
		}
	}
	timer.Finish()

	fmt.Printf("  %s: %s\n", label, timer.Format())

	// 等待 idle flush + compaction 完成（compaction 检查间隔 5s，等待 3 个周期）
	fmt.Printf("  等待落盘和 compaction...\n")
	time.Sleep(20 * time.Second)

	if err := db.Close(); err != nil {
		return 0, 0, 0, fmt.Errorf("close db: %w", err)
	}

	dataDirPath := filepath.Join(dataDir, "db1", "cpu")
	stats, err := metrics.CalcDirSize(dataDirPath)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("calc dir size: %w", err)
	}

	fmt.Printf("  磁盘占用: %s (%d 文件, %.1f B/点)\n",
		metrics.FormatBytes(uint64(stats.TotalSize)), stats.FileCount, stats.BytesPerPoint(totalPoints))

	return stats.TotalSize, stats.FileCount, timer.TPS(), nil
}

func main() {
	fmt.Println("================================================")
	fmt.Println("磁盘空间压缩对比测试 (1M 数据点)")
	fmt.Println("================================================")

	tmpBase := filepath.Join(os.TempDir(), "microts_diff_compact_test")
	defer func() { _ = os.RemoveAll(tmpBase) }()

	compressions := []struct {
		name string
		dir  string
		algo sstable.CompressionAlgorithm
	}{
		{"不压缩 (none)", "none", sstable.CompressionNone},
		{"Snappy", "snappy", sstable.CompressionSnappy},
		{"LZ4", "lz4", sstable.CompressionLZ4},
	}

	results := make([]result, len(compressions))

	for i, c := range compressions {
		fmt.Printf("\n>>> 第 %d 轮: %s <<<\n", i+1, c.name)
		size, files, tps, err := runTest(filepath.Join(tmpBase, c.dir), c.algo, "写入")
		if err != nil {
			fmt.Printf("❌ %s 测试失败: %v\n", c.name, err)
			os.Exit(1)
		}
		results[i] = result{name: c.name, size: size, files: files, tps: tps}
	}

	// 原始数据估算（每点 10 字段 + tags + timestamp）
	const rawBytesPerPoint = 99
	rawSizeEst := int64(totalPoints * rawBytesPerPoint)
	rawMB := float64(rawSizeEst) / (1024 * 1024)

	fmt.Println("\n================================================")
	fmt.Println("对比结果")
	fmt.Println("================================================")

	fmt.Printf("原始数据预估:     %.1f MB (%.0f B/点)\n\n", rawMB, float64(rawBytesPerPoint))

	for _, r := range results {
		fmt.Printf("  %s:  %s (%.2f MB, %.1f B/点, %d 文件)\n",
			r.name, metrics.FormatBytes(uint64(r.size)),
			float64(r.size)/(1024*1024), float64(r.size)/float64(totalPoints), r.files)
	}

	fmt.Println()
	baseline := results[0] // none
	for i := 1; i < len(results); i++ {
		r := results[i]
		savings := float64(baseline.size-r.size) / float64(baseline.size) * 100
		ratio := float64(baseline.size) / float64(r.size)
		fmt.Printf("  块压缩比 (none→%s): %.2fx (节省 %.1f%%)\n", r.name, ratio, savings)
	}

	encodingRatio := float64(rawSizeEst) / float64(baseline.size)
	fmt.Printf("\n  编码压缩比 (原始→none): %.2fx\n", encodingRatio)
	for i := 1; i < len(results); i++ {
		overallRatio := float64(rawSizeEst) / float64(results[i].size)
		fmt.Printf("  总压缩比 (原始→%s): %.2fx\n", results[i].name, overallRatio)
	}

	fmt.Printf("\n写入性能:\n")
	for _, r := range results {
		fmt.Printf("  %s TPS: %.0f\n", r.name, r.tps)
	}

	baselineTPS := results[0].tps
	for i := 1; i < len(results); i++ {
		if baselineTPS > 0 {
			fmt.Printf("  %s 性能影响: %.1f%%\n", results[i].name, (1-results[i].tps/baselineTPS)*100)
		}
	}

	fmt.Println("\n测试完成！")
}
