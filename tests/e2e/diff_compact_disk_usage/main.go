// tests/e2e/diff_compact_disk_usage/main.go
//
// 磁盘空间压缩对比测试：对比 1M 数据点在开启/关闭压缩时的磁盘占用，计算压缩比。
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

// runTest 执行一轮测试，写入数据、等待落盘和 compaction 完成后统计磁盘占用。
func runTest(dataDir string, compression sstable.CompressionAlgorithm, label string) (diskBytes int64, tps float64, err error) {
	_ = os.RemoveAll(dataDir)

	cfg := microts.Config{
		DataDir:       dataDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           64 * 1024 * 1024,
			MaxCount:          50000,
			IdleDurationNanos: int64(30 * time.Second),
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
		return 0, 0, fmt.Errorf("open db: %w", err)
	}

	gen := data_gen.NewDataGenerator(42) // 固定种子，确保两次写入数据一致
	baseTime := time.Now().UnixNano()

	timer := metrics.NewWriteSummary(totalPoints)
	for i := 0; i < totalPoints; i++ {
		ts := baseTime + int64(i)*int64(time.Millisecond)
		p := gen.GeneratePoint("db1", "cpu", ts)
		if err := db.Write(context.Background(), p); err != nil {
			_ = db.Close()
			return 0, 0, fmt.Errorf("write point %d: %w", i, err)
		}
	}
	timer.Finish()
	writeTPS := timer.TPS()

	fmt.Printf("  %s: %s\n", label, timer.Format())

	// 等待 idle flush + compaction 完成（compaction 检查间隔 5s，等待 3 个周期确保完成）
	fmt.Printf("  等待落盘和 compaction...\n")
	time.Sleep(20 * time.Second)

	if err := db.Close(); err != nil {
		return 0, 0, fmt.Errorf("close db: %w", err)
	}

	// 统计 data 目录的磁盘占用
	dataDirPath := filepath.Join(dataDir, "db1", "cpu")
	stats, err := metrics.CalcDirSize(dataDirPath)
	if err != nil {
		return 0, 0, fmt.Errorf("calc dir size: %w", err)
	}

	fmt.Printf("  磁盘占用: %s (%d 文件)\n", metrics.FormatBytes(uint64(stats.TotalSize)), stats.FileCount)
	fmt.Printf("  每点字节: %.2f\n", stats.BytesPerPoint(totalPoints))

	return stats.TotalSize, writeTPS, nil
}

func main() {
	fmt.Println("================================================")
	fmt.Println("磁盘空间压缩对比测试 (1M 数据点)")
	fmt.Println("================================================")

	tmpBase := filepath.Join(os.TempDir(), "microts_diff_compact_test")
	defer func() { _ = os.RemoveAll(tmpBase) }()

	// 第一轮：不压缩
	fmt.Println("\n>>> 第 1 轮: 不压缩 (none) <<<")
	dirNone := filepath.Join(tmpBase, "none")
	sizeNone, tpsNone, err := runTest(dirNone, sstable.CompressionNone, "写入")
	if err != nil {
		fmt.Printf("❌ 不压缩测试失败: %v\n", err)
		os.Exit(1)
	}

	// 第二轮：snappy 压缩
	fmt.Println("\n>>> 第 2 轮: Snappy 压缩 <<<")
	dirSnappy := filepath.Join(tmpBase, "snappy")
	sizeSnappy, tpsSnappy, err := runTest(dirSnappy, sstable.CompressionSnappy, "写入")
	if err != nil {
		fmt.Printf("❌ Snappy 压缩测试失败: %v\n", err)
		os.Exit(1)
	}

	// 原始数据估算（每点 10 字段 + tags + timestamp）
	// 5 float64 (8B) + 3 int64 (8B) + 1 string (~13B) + 1 bool (1B) + tags (~13B) + ts (8B) ≈ 99B
	const rawBytesPerPoint = 99
	rawSizeEst := int64(totalPoints * rawBytesPerPoint)
	rawMB := float64(rawSizeEst) / (1024 * 1024)

	// 对比结果
	fmt.Println("\n================================================")
	fmt.Println("对比结果")
	fmt.Println("================================================")

	noneMB := float64(sizeNone) / (1024 * 1024)
	snappyMB := float64(sizeSnappy) / (1024 * 1024)

	fmt.Printf("原始数据预估:     %.1f MB (%.0f B/点)\n", rawMB, float64(rawBytesPerPoint))
	fmt.Printf("不压缩 (none):   %s (%.2f MB, %.1f B/点)\n", metrics.FormatBytes(uint64(sizeNone)), noneMB, float64(sizeNone)/float64(totalPoints))
	fmt.Printf("Snappy 压缩:     %s (%.2f MB, %.1f B/点)\n", metrics.FormatBytes(uint64(sizeSnappy)), snappyMB, float64(sizeSnappy)/float64(totalPoints))

	encodingRatio := float64(rawSizeEst) / float64(sizeNone)
	fmt.Printf("\n编码压缩比 (原始→none):   %.2fx\n", encodingRatio)

	if sizeNone > 0 {
		savings := float64(sizeNone-sizeSnappy) / float64(sizeNone) * 100
		blockRatio := float64(sizeNone) / float64(sizeSnappy)
		fmt.Printf("块压缩比   (none→snappy): %.2fx (节省 %.1f%%)\n", blockRatio, savings)

		overallRatio := float64(rawSizeEst) / float64(sizeSnappy)
		fmt.Printf("总压缩比   (原始→snappy): %.2fx\n", overallRatio)
	}

	fmt.Printf("\n写入性能:\n")
	fmt.Printf("  不压缩 TPS:  %.0f\n", tpsNone)
	fmt.Printf("  Snappy TPS:  %.0f\n", tpsSnappy)
	if tpsNone > 0 {
		fmt.Printf("  性能影响:    %.1f%%\n", (1-tpsSnappy/tpsNone)*100)
	}

	fmt.Println("\n测试完成！")
}
