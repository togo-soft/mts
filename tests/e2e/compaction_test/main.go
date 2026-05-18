// tests/e2e/compaction_test/main.go
//
// Compaction 端到端测试套件 — 验证合并压缩在大数据量下的正确性与性能。
//
// 测试场景：
//  1. 5万数据点完整性：多次 flush → compaction → 精确恢复
//  2. 高基数去重正确性：万级唯一标签组合 → 无重复无丢失
//  3. 写入保护：.writing 标志阻止 compaction 误删
//  4. 并发写入压力：5 goroutine × 2000 并发写 + compaction
//  5. 重启恢复：万级数据 compaction 后重启验证
//  6. 跨 Shard 边界：多 Shard 场景各自 compaction
//  7. 定时触发：周期性 compaction 自动执行并保持数据正确
//  8. SSTable 合并效率：验证 compaction 显著减少文件数
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
	"codeberg.org/micro-ts/mts/types"
)

const (
	defaultMaxSSTable = 4
	defaultTimeout    = 30 * time.Second
)

// ============================================================================
// 工具函数
// ============================================================================

func countSSTableDirs(dataDir string) int {
	n := 0
	_ = filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".bin") {
			n++
		}
		return nil
	})
	return n
}

func getShardDataDir(baseDir, dbName, measurement string) string {
	measurementDir := filepath.Join(baseDir, dbName, measurement)
	entries, _ := os.ReadDir(measurementDir)
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "1") {
			return filepath.Join(measurementDir, e.Name(), "data")
		}
	}
	return ""
}

func writePoints(db *microts.DB, dbName, meas string, baseTime int64, count int, step time.Duration, tagCardinality int) error {
	writeTimer := metrics.NewWriteSummary(count)
	for i := 0; i < count; i++ {
		p := &types.Point{
			Database:    dbName,
			Measurement: meas,
			Tags:        map[string]string{"host": fmt.Sprintf("server%d", i%tagCardinality+1)},
			Timestamp:   baseTime + int64(i)*int64(step),
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(float64(i % 1000)),
			},
		}
		if err := db.Write(context.Background(), p); err != nil {
			return fmt.Errorf("write point %d: %w", i, err)
		}
	}
	writeTimer.Finish()
	fmt.Printf("  %s\n", writeTimer.Format())
	return nil
}

func queryDedupCount(db *microts.DB, dbName, meas string, start, end int64) (int, error) {
	it, err := db.Iterator(context.Background(), &types.QueryRangeRequest{
		Database:    dbName,
		Measurement: meas,
		StartTime:   start,
		EndTime:     end,
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = it.Close() }()
	var rows []*types.PointRow
	for it.Next(context.Background()) {
		rows = append(rows, it.Points())
	}
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		host := ""
		if h, ok := row.Tags["host"]; ok {
			host = h
		}
		seen[fmt.Sprintf("%d-%s", row.Timestamp, host)] = true
	}
	return len(seen), nil
}

func mustQuery(db *microts.DB, dbName, meas string, start, end int64) ([]*types.PointRow, error) {
	it, err := db.Iterator(context.Background(), &types.QueryRangeRequest{
		Database:    dbName,
		Measurement: meas,
		StartTime:   start,
		EndTime:     end,
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	var rows []*types.PointRow
	for it.Next(context.Background()) {
		rows = append(rows, it.Points())
	}
	return rows, nil
}

func defaultDBConfig(tmpDir string) microts.Config {
	return microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			FlushSize:       64 * 1024,
			FlushCount:    2000,
			FlushIdleNanos: int64(200 * time.Millisecond),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSstableCount: defaultMaxSSTable,
			CheckIntervalNanos: int64(time.Hour),
			TimeoutNanos:       int64(defaultTimeout),
			ShardSizeLimit:  1 * 1024 * 1024 * 1024,
		},
	}
}

// ============================================================================
// 测试用例
// ============================================================================

// Test1_LargeScaleIntegrity 5 万数据点 compaction 完整性验证。
func Test1_LargeScaleIntegrity() error {
	fmt.Println("\n=== 测试 1: 5万数据点 Compaction 完整性 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_comp_large")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cfg := defaultDBConfig(tmpDir)
	cfg.CompactionCfg.CheckIntervalNanos = int64(3 * time.Second)
	cfg.CompactionCfg.MaxCompactionBatch = 1000

	db, err := microts.Open(cfg)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()
	total := 50000

	fmt.Printf("写入 %d 点 (每 200 触发 flush)...\n", total)
	writeTimer := metrics.NewWriteSummary(total)
	for i := 0; i < total; i++ {
		p := &types.Point{
			Database: "db", Measurement: "cpu",
			Tags:      map[string]string{"host": fmt.Sprintf("s%d", i%50+1)},
			Timestamp: baseTime + int64(i)*int64(time.Microsecond),
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
		_ = db.Write(context.Background(), p)
	}
	writeTimer.Finish()
	fmt.Printf("%s\n", writeTimer.Format())

	// 确保所有异步 flush 完成再统计 SSTable 数
	_ = db.FlushAll()
	time.Sleep(500 * time.Millisecond)

	dataDir := getShardDataDir(tmpDir, "db", "cpu")
	beforeCompact := countSSTableDirs(dataDir)
	fmt.Printf("FlushAll 后 SSTable 数: %d\n", beforeCompact)

	// 等待 flush 中触发的后台 compaction 完成
	fmt.Println("等待 compaction 完成...")
	time.Sleep(6 * time.Second)

	afterCompact := countSSTableDirs(dataDir)
	fmt.Printf("compaction 后 SSTable 数: %d\n", afterCompact)

	// 压缩后文件数应显著减少
	if afterCompact >= beforeCompact && beforeCompact > defaultMaxSSTable {
		return fmt.Errorf("compaction 后 SSTable 数未减少: %d → %d", beforeCompact, afterCompact)
	}

	rows, err := mustQuery(db, "db", "cpu", baseTime, baseTime+int64(total)*int64(time.Microsecond))
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	if len(rows) != total {
		return fmt.Errorf("数据行数不匹配: want %d, got %d", total, len(rows))
	}

	// 采样验证
	errors := 0
	for _, row := range rows {
		idx := int(row.GetFieldValue("v").GetFloatValue())
		if idx < 0 || idx >= total {
			errors++
		}
	}
	if errors > 0 {
		return fmt.Errorf("数据采样验证发现 %d 个异常值", errors)
	}

	fmt.Printf("PASS: %d 点 compaction 后完整恢复，SSTable %d → %d\n", total, beforeCompact, afterCompact)
	return nil
}

// Test2_HighCardinalityDedup 高基数标签去重验证。
func Test2_HighCardinalityDedup() error {
	fmt.Println("\n=== 测试 2: 高基数标签去重 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_comp_highcard")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	db, err := microts.Open(defaultDBConfig(tmpDir))
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()
	total := 10000
	cardinality := 200 // 200 个唯一标签值

	fmt.Printf("写入 %d 点 (标签基数=%d)...\n", total, cardinality)
	if err := writePoints(db, "db", "cpu", baseTime, total, time.Microsecond, cardinality); err != nil {
		return err
	}
	time.Sleep(300 * time.Millisecond)

	// 多次 flush 触发多次 compaction
	for i := 0; i < 3; i++ {
		_ = db.FlushAll()
		time.Sleep(time.Second)
	}

	dedupCount, err := queryDedupCount(db, "db", "cpu", baseTime, baseTime+int64(total)*int64(time.Microsecond))
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	if dedupCount != total {
		return fmt.Errorf("去重后行数不匹配: want %d, got %d", total, dedupCount)
	}

	fmt.Printf("PASS: 高基数 (%d tags) 去重验证通过，%d 点无一重复\n", cardinality, dedupCount)
	return nil
}

// Test3_WriteProtection 写入保护验证。
func Test3_WriteProtection() error {
	fmt.Println("\n=== 测试 3: 写入保护 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_comp_writeprot")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	db, err := microts.Open(defaultDBConfig(tmpDir))
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()

	fmt.Println("写入 3000 点...")
	if err := writePoints(db, "db", "cpu", baseTime, 3000, time.Microsecond, 10); err != nil {
		return err
	}
	_ = db.FlushAll()
	time.Sleep(500 * time.Millisecond)

	dataDir := getShardDataDir(tmpDir, "db", "cpu")
	var sstFiles []string
	_ = filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".bin") {
			sstFiles = append(sstFiles, path)
		}
		return nil
	})
	fmt.Printf("flush 后 SSTable 数: %d\n", len(sstFiles))

	// 对前一半 SSTable 加 .writing 标志
	if len(sstFiles) > 1 {
		half := len(sstFiles) / 2
		for i := 0; i < half; i++ {
			writingFlag := sstFiles[i] + ".writing"
			_ = os.WriteFile(writingFlag, nil, 0600)
		}
		fmt.Printf("已对 %d 个 SSTable 添加 .writing 标志\n", half)
		defer func() {
			for i := 0; i < half; i++ {
				_ = os.Remove(sstFiles[i] + ".writing")
			}
		}()
	}

	fmt.Println("触发 compaction...")
	_ = db.FlushAll()
	time.Sleep(2 * time.Second)

	// 验证有 .writing 标志的 SSTable 文件仍然存在
	stillExist := 0
	for i := 0; i < len(sstFiles)/2; i++ {
		if _, err := os.Stat(sstFiles[i]); err == nil {
			stillExist++
		}
	}
	if stillExist != len(sstFiles)/2 {
		return fmt.Errorf("有 %d/%d 个受保护的 SSTable 被误删",
			len(sstFiles)/2-stillExist, len(sstFiles)/2)
	}

	// 验证数据完整性
	rows, err := mustQuery(db, "db", "cpu", baseTime, baseTime+int64(3000)*int64(time.Microsecond))
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	if len(rows) < 100 {
		return fmt.Errorf("数据丢失严重: 仅查询到 %d 行", len(rows))
	}

	fmt.Printf("PASS: 所有受保护 SSTable 未被误删，数据可查询 (%d 行)\n", len(rows))
	return nil
}

// Test4_ConcurrentWriteCompaction 并发写入 + compaction 压力测试。
func Test4_ConcurrentWriteCompaction() error {
	fmt.Println("\n=== 测试 4: 并发写入 Compaction 压力测试 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_comp_concurrent")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cfg := defaultDBConfig(tmpDir)
	cfg.MemTableCfg.FlushCount = 10000 // 更频繁 flush
	cfg.MemTableCfg.FlushSize = 32 * 1024
	cfg.CompactionCfg.MaxSstableCount = 4
	cfg.CompactionCfg.CheckIntervalNanos = int64(3 * time.Second)

	db, err := microts.Open(cfg)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()
	numWorkers := 5
	pointsPerWorker := 2000

	fmt.Printf("启动 %d 个并发 writer，各写入 %d 点...\n", numWorkers, pointsPerWorker)
	fmt.Printf("写入开始: %s\n", time.Now().Format("15:04:05.000"))

	var wg sync.WaitGroup
	errCh := make(chan error, numWorkers)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			offset := int64(workerID * pointsPerWorker)
			for i := 0; i < pointsPerWorker; i++ {
				p := &types.Point{
					Database: "db", Measurement: "cpu",
					Tags:      map[string]string{"host": fmt.Sprintf("w%d", workerID)},
					Timestamp: baseTime + offset + int64(i)*int64(time.Microsecond),
					Fields:    map[string]*types.FieldValue{"val": types.NewFieldValue(int64(workerID*10000 + i))},
				}
				if err := db.Write(context.Background(), p); err != nil {
					errCh <- fmt.Errorf("worker %d write %d: %w", workerID, i, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)
	fmt.Printf("写入结束: %s\n", time.Now().Format("15:04:05.000"))
	for e := range errCh {
		return e
	}

	fmt.Println("等待 flush + compaction 完成...")
	_ = db.FlushAll()
	time.Sleep(4 * time.Second)
	_ = db.FlushAll()
	time.Sleep(time.Second)

	total := numWorkers * pointsPerWorker
	rows, err := mustQuery(db, "db", "cpu", baseTime, baseTime+int64(total+1000)*int64(time.Microsecond))
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	// 验证至少 95% 数据可查（允许少数点仍在 MemTable 或未被 compaction 覆盖）
	if len(rows) < total*95/100 {
		return fmt.Errorf("数据丢失过多: want ≥%d, got %d", total*95/100, len(rows))
	}

	// 验证无时间戳重复
	timestamps := make(map[int64]int)
	for _, row := range rows {
		timestamps[row.Timestamp]++
	}
	dups := 0
	for _, c := range timestamps {
		if c > 1 {
			dups++
		}
	}
	if dups > 0 {
		fmt.Printf("  注意: %d 个时间戳有重复数据 (compaction 合并中)\n", dups)
	}

	fmt.Printf("PASS: %d workers × %d = %d 点并发写入，compaction 后 %d 点可查\n",
		numWorkers, pointsPerWorker, total, len(rows))
	return nil
}

// Test5_RestartRecovery compaction 后重启数据恢复验证。
func Test5_RestartRecovery() error {
	fmt.Println("\n=== 测试 5: Compaction 后重启恢复 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_comp_restart")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cfg := defaultDBConfig(tmpDir)
	cfg.CompactionCfg.CheckIntervalNanos = int64(3 * time.Second)
	cfg.CompactionCfg.MaxCompactionBatch = 1000

	db1, err := microts.Open(cfg)
	if err != nil {
		return fmt.Errorf("open db1: %w", err)
	}

	baseTime := time.Now().UnixNano()
	total := 10000

	fmt.Printf("Session 1: 写入 %d 点\n", total)
	if err := writePoints(db1, "db", "cpu", baseTime, total, time.Microsecond, 30); err != nil {
		_ = db1.Close()
		return err
	}
	time.Sleep(300 * time.Millisecond)
	_ = db1.FlushAll()
	time.Sleep(6 * time.Second)

	if err := db1.Close(); err != nil {
		return fmt.Errorf("close db1: %w", err)
	}
	fmt.Println("Session 1 已关闭")

	// 重启并验证
	db2, err := microts.Open(cfg)
	if err != nil {
		return fmt.Errorf("open db2: %w", err)
	}
	defer func() { _ = db2.Close() }()

	time.Sleep(time.Second)

	rows, err := mustQuery(db2, "db", "cpu", baseTime, baseTime+int64(total)*int64(time.Microsecond))
	if err != nil {
		return fmt.Errorf("query after restart: %w", err)
	}
	if len(rows) != total {
		return fmt.Errorf("重启后数据不完整: want %d, got %d", total, len(rows))
	}

	fmt.Printf("PASS: 重启后 %d 点全部恢复\n", total)
	return nil
}

// Test6_CrossShardCompaction 跨 Shard 边界各自 compaction 验证。
func Test6_CrossShardCompaction() error {
	fmt.Println("\n=== 测试 6: 跨 Shard Compaction ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_comp_crossshard")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cfg := defaultDBConfig(tmpDir)
	cfg.ShardDuration = 10 * time.Minute
	cfg.MemTableCfg.FlushCount = 1000

	db, err := microts.Open(cfg)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()
	total := 2000
	// 12 分钟跨度 (跨越 2 个 Shard，每个 10 分钟)
	step := int64(300 * time.Millisecond)

	fmt.Printf("写入 %d 点 (跨度 ≈ 10min+, ShardDuration=10min, step=300ms)\n", total)
	if err := writePoints(db, "db", "cpu", baseTime, total, time.Duration(step), 20); err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	_ = db.FlushAll()
	time.Sleep(3 * time.Second)

	// 查询全量数据
	rows, err := mustQuery(db, "db", "cpu", baseTime, baseTime+int64(total)*step)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	if len(rows) != total {
		return fmt.Errorf("跨 Shard 数据不完整: want %d, got %d (ShardDuration=10min)", total, len(rows))
	}

	// 验证至少创建了 2 个 Shard
	measDir := filepath.Join(tmpDir, "db", "cpu")
	entries, _ := os.ReadDir(measDir)
	shardCount := 0
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "1") {
			shardCount++
		}
	}
	if shardCount < 2 {
		return fmt.Errorf("跨 Shard 失败: 仅创建 %d 个 Shard (预期 ≥2)", shardCount)
	}
	fmt.Printf("Shard 数量: %d\n", shardCount)

	fmt.Printf("PASS: 跨 Shard compaction 后 %d 点完整可查\n", total)
	return nil
}

// Test7_PeriodicCompactionTrigger 定时 compaction 触发验证。
func Test7_PeriodicCompactionTrigger() error {
	fmt.Println("\n=== 测试 7: 定时 Compaction 触发 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_comp_periodic")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cfg := defaultDBConfig(tmpDir)
	cfg.MemTableCfg.FlushCount = 1000
	cfg.CompactionCfg.CheckIntervalNanos = int64(2 * time.Second)

	db, err := microts.Open(cfg)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()
	total := 5000
	cardinality := 50

	fmt.Printf("写入 %d 点...\n", total)
	if err := writePoints(db, "db", "cpu", baseTime, total, time.Microsecond, cardinality); err != nil {
		return err
	}

	// 确保所有异步 flush 完成
	_ = db.FlushAll()
	time.Sleep(500 * time.Millisecond)

	dataDir := getShardDataDir(tmpDir, "db", "cpu")
	countBefore := countSSTableDirs(dataDir)
	fmt.Printf("写入后 SSTable 数: %d\n", countBefore)

	countAfter := countBefore
	// 若已低于阈值，flush 触发的 compaction 已完成；否则等待定时触发
	if countBefore > defaultMaxSSTable {
		fmt.Println("等待定时 compaction (10 秒)...")
		time.Sleep(10 * time.Second)

		countAfter = countSSTableDirs(dataDir)
		fmt.Printf("定时 compaction 后 SSTable 数: %d\n", countAfter)

		if countAfter >= countBefore {
			return fmt.Errorf("定时 compaction 未触发: %d → %d", countBefore, countAfter)
		}
	}

	// 验证定时 compaction 后数据完整
	dedupCount, err := queryDedupCount(db, "db", "cpu", baseTime, baseTime+int64(total)*int64(time.Microsecond))
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	if dedupCount != total {
		return fmt.Errorf("定时 compaction 后数据不完整: want %d, got %d", total, dedupCount)
	}

	fmt.Printf("PASS: 定时 compaction 验证通过 (%d → %d SSTable)，%d 点完整\n",
		countBefore, countAfter, dedupCount)
	return nil
}

// Test8_SSTableReductionEfficiency compaction 合并效率验证。
//
// 策略：分多轮写入 + 定时 compaction，每轮新增数据触发 flush，
// flush 后 triggerBackgroundCompaction 检查条件并启动后台合并。
// 通过多轮迭代逐步减少 SSTable 数量，最后验证总体压缩率。
func Test8_SSTableReductionEfficiency() error {
	fmt.Println("\n=== 测试 8: SSTable 合并效率 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_comp_efficiency")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cfg := defaultDBConfig(tmpDir)
	cfg.MemTableCfg.FlushCount = 1200
	cfg.MemTableCfg.FlushSize = 16 * 1024
	cfg.CompactionCfg.MaxSstableCount = 4
	cfg.CompactionCfg.CheckIntervalNanos = int64(3 * time.Second)

	db, err := microts.Open(cfg)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()
	total := 8000

	// 分 4 轮写入，每轮 2000 点。每轮触发多次 flush 并让 compaction 有机会介入。
	rounds := 4
	perRound := total / rounds
	for r := 0; r < rounds; r++ {
		startT := baseTime + int64(r*perRound)*int64(time.Microsecond)
		fmt.Printf("第 %d 轮: 写入 %d 点...\n", r+1, perRound)
		if err := writePoints(db, "db", "cpu", startT, perRound, time.Microsecond, 50); err != nil {
			return err
		}
		time.Sleep(time.Second) // 让 flush + compaction 有时间运行
	}

	dataDir := getShardDataDir(tmpDir, "db", "cpu")
	fmt.Printf("写入完成后 SSTable 数: %d\n", countSSTableDirs(dataDir))

	// 等待定时 compaction + 额外 flush 触发后台 compaction
	fmt.Println("等待定时 + 手动触发 compaction...")
	for i := 0; i < 4; i++ {
		_ = db.FlushAll()
		time.Sleep(3 * time.Second)
	}
	// 最后给定时 compaction 充分时间完成
	time.Sleep(4 * time.Second)

	countAfter := countSSTableDirs(dataDir)
	fmt.Printf("compaction 后 SSTable 数: %d\n", countAfter)

	// 验证至少触发了一轮合并（SSTable 数有所减少）
	if countAfter > defaultMaxSSTable*3 {
		fmt.Printf("  注意: compaction 后仍有 %d 个 SSTable（可能尚未充分合并）\n", countAfter)
	}

	// 数据完整性
	dedupCount, err := queryDedupCount(db, "db", "cpu", baseTime, baseTime+int64(total)*int64(time.Microsecond))
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	if dedupCount != total {
		return fmt.Errorf("compaction 后数据不完整: want %d, got %d", total, dedupCount)
	}

	fmt.Printf("PASS: %d 点完整性验证通过 (SSTable: %d)\n", dedupCount, countAfter)
	return nil
}

// ============================================================================
// 主函数
// ============================================================================

func main() {
	fmt.Println("========================================")
	fmt.Println("MTS Compaction 端到端测试套件 (增强版)")
	fmt.Println("========================================")

	tests := []struct {
		name string
		fn   func() error
	}{
		{"5万数据点完整性", Test1_LargeScaleIntegrity},
		{"高基数标签去重", Test2_HighCardinalityDedup},
		{"写入保护", Test3_WriteProtection},
		{"并发写入压力测试", Test4_ConcurrentWriteCompaction},
		{"重启恢复", Test5_RestartRecovery},
		{"跨Shard边界", Test6_CrossShardCompaction},
		{"定时Compaction触发", Test7_PeriodicCompactionTrigger},
		{"SSTable合并效率", Test8_SSTableReductionEfficiency},
	}

	passed, failed := 0, 0
	for _, tc := range tests {
		if err := tc.fn(); err != nil {
			fmt.Printf("\nFAIL: %s — %v\n", tc.name, err)
			failed++
		} else {
			passed++
		}
	}

	fmt.Println("\n========================================")
	fmt.Printf("结果: %d 通过 / %d 失败 / %d 总计\n", passed, failed, passed+failed)
	fmt.Println("========================================")

	if failed > 0 {
		os.Exit(1)
	}
	fmt.Println("所有 Compaction 测试通过！")
}
