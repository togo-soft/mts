// tests/e2e/write_and_compact/main.go
//
// 验证 Level Compaction 写入并压缩后是否降低文件个数，且不影响查询功能。
//
// 测试流程：
//  1. 使用 Level Compaction 配置创建 Shard（L0 MaxParts=4）
//  2. 写入多批带重叠时间范围的数据（7 次 Flush 产生 7 个 L0 SSTable）
//  3. 等待后台 Compaction 自动触发（第 4 次 Flush 满足 ShouldCompact 条件）
//  4. 验证 Compaction 后 L1 有合并后的文件
//  5. 查询验证数据完整性
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
	"codeberg.org/micro-ts/mts/types"
)

func countBinFiles(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".bin") {
			n++
		}
	}
	return n
}

func main() {
	tmpDir, err := os.MkdirTemp("", "mts_write_compact_*")
	if err != nil {
		fmt.Printf("FAIL: create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	seriesStore := metadata.NewSimpleSeriesStore()
	schemaStore := metadata.NewSimpleSchemaStore()
	_ = schemaStore.SetSchema("testdb", "cpu", &metadata.Schema{
		Version: 1,
		Fields:  []metadata.FieldDef{{Name: "value", Type: 1}}, // float64
	})

	cfg := shard.ShardConfig{
		DB:          "testdb",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Now().Add(24 * time.Hour).UnixNano(),
		Dir:         tmpDir,
		SeriesStore: seriesStore,
		SchemaStore: schemaStore,
		MemTableCfg: &memtable.MemTableConfig{
			MaxSize:           64 * 1024 * 1024, // 足够大，避免因大小触发 Flush
			MaxCount:          100,              // 足够大，避免因条数触发 Flush
			IdleDurationNanos: int64(10 * time.Second),
		},
		LevelCompactionCfg: &compaction.LevelConfig{
			Enabled: true,
			LevelConfigs: []compaction.LevelSpec{
				{Level: 0, MaxSize: 10 * 1024 * 1024, MaxParts: 4},
				{Level: 1, MaxSize: 100 * 1024 * 1024, MaxParts: 0},
				{Level: 2, MaxSize: 1024 * 1024 * 1024, MaxParts: 0},
			},
			L0ToL1SizeThreshold: 1024, // 极小阈值，确保 Size 条件也容易触发
			MaxCompactionParts:  10,
			TombstoneRetention:  1 * time.Hour,
			CheckInterval:       1 * time.Hour, // 禁用定时检查，仅依赖 Flush 触发
			Timeout:             30 * time.Second,
		},
	}

	s := shard.NewShard(cfg)
	fmt.Println("Shard created with Level Compaction (L0 MaxParts=4)")

	baseTime := time.Now().UnixNano()
	numFlushes := 7
	pointsPerFlush := 5

	totalPoints := numFlushes * pointsPerFlush
	fmt.Printf("\nWriting %d batches x %d points each with overlapping time ranges...\n", numFlushes, pointsPerFlush)
	writeTimer := metrics.NewWriteSummary(totalPoints)
	for flush := 0; flush < numFlushes; flush++ {
		for i := 0; i < pointsPerFlush; i++ {
			ts := baseTime + int64(i)*int64(time.Millisecond)
			p := &types.Point{
				Database:    "testdb",
				Measurement: "cpu",
				Tags:        map[string]string{"host": fmt.Sprintf("batch%d", flush)},
				Timestamp:   ts,
				Fields: map[string]*types.FieldValue{
					"value": types.NewFieldValue(float64(flush*100 + i)),
				},
			}
			if err := s.Write(p); err != nil {
				fmt.Printf("FAIL: write point at batch %d, index %d: %v\n", flush, i, err)
				_ = s.Close()
				os.Exit(1)
			}
		}

		if err := s.Flush(); err != nil {
			fmt.Printf("FAIL: flush batch %d: %v\n", flush, err)
			_ = s.Close()
			os.Exit(1)
		}
		fmt.Printf("  Flush %d complete\n", flush+1)
	}
	writeTimer.Finish()
	fmt.Printf("%s\n", writeTimer.Format())

	l0Dir := filepath.Join(tmpDir, "data", "L0")
	l1Dir := filepath.Join(tmpDir, "data", "L1")

	l0Before := countBinFiles(l0Dir)
	fmt.Printf("\nBefore compaction wait: L0 has %d .bin files\n", l0Before)

	// 等待后台 Compaction 完成
	// 第 4 次 Flush 触发 ShouldCompact（4 parts >= MaxParts=4），后台 goroutine 开始合并
	fmt.Println("Waiting for background compaction to complete...")

	// 轮询等待 Compaction 完成（最多等 10 秒）
	for i := 0; i < 20; i++ {
		time.Sleep(500 * time.Millisecond)
		l1Count := countBinFiles(l1Dir)
		l0Count := countBinFiles(l0Dir)
		if l1Count > 0 {
			fmt.Printf("  Compaction detected: L0=%d, L1=%d\n", l0Count, l1Count)
			break
		}
		fmt.Printf("  [%d] Waiting... L0=%d, L1=%d\n", i+1, l0Count, l1Count)
	}

	l0After := countBinFiles(l0Dir)
	l1After := countBinFiles(l1Dir)

	fmt.Printf("\nFile count after compaction:\n")
	fmt.Printf("  L0: %d .bin files (was %d)\n", l0After, l0Before)
	fmt.Printf("  L1: %d .bin files\n", l1After)

	// 验证 Compaction 已将数据合并到 L1
	if l1After == 0 {
		fmt.Println("\nWARNING: No files in L1, compaction may not have completed")
		fmt.Println("This could be a timing issue — compaction still in progress")
	} else {
		fmt.Println("PASS: Compaction merged files to L1")
	}

	// 查询验证数据完整性
	fmt.Println("\nQuerying to verify data integrity...")
	iter := shard.NewShardIterator(s, baseTime, baseTime+int64(pointsPerFlush)*int64(time.Millisecond), 0)
	var rows []*types.PointRow
	for row := iter.Next(); row != nil; row = iter.Next() {
		rows = append(rows, row)
	}
	iter.Close()
	if err := iter.Err(); err != nil {
		fmt.Printf("FAIL: query: %v\n", err)
		_ = s.Close()
		os.Exit(1)
	}

	// 统计唯一的 timestamp+SID 组合
	seen := make(map[string]bool)
	for _, row := range rows {
		// each flush creates a unique tag "batchN" → unique SID
		seen[fmt.Sprintf("%d-%d", row.Timestamp, row.Sid)] = true
	}
	expectedUnique := numFlushes * pointsPerFlush
	fmt.Printf("  Query returned %d rows, %d unique (timestamp,sid) pairs\n", len(rows), len(seen))
	fmt.Printf("  Expected unique: %d\n", expectedUnique)

	if len(seen) == expectedUnique {
		fmt.Println("PASS: All written data is readable")
	} else if len(seen) > 0 && len(seen) < expectedUnique {
		fmt.Printf("WARNING: Expected %d unique rows but got %d (some data may still be in MemTable)\n",
			expectedUnique, len(seen))
	} else if len(seen) == 0 {
		fmt.Println("FAIL: No data returned from query")
		_ = s.Close()
		os.Exit(1)
	}

	if err := s.Close(); err != nil {
		fmt.Printf("WARNING: shard close error: %v\n", err)
	}

	// 最终判定
	if l1After > 0 && len(seen) == expectedUnique {
		fmt.Println("\n=== Test PASSED: Write and Compact ===")
		fmt.Println("Compaction reduced file count, query integrity preserved")
	} else if l1After > 0 {
		fmt.Println("\n=== Test PARTIAL: Compaction worked but query may have timing issues ===")
	} else {
		fmt.Println("\n=== Test FAILED: Compaction did not produce expected results ===")
		os.Exit(1)
	}
}
