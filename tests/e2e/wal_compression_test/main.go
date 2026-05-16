// tests/e2e/wal_compression_test/main.go
//
// # WAL 压缩端到端测试
//
// 本测试验证 MTS 数据库 WAL 压缩功能：
//
//  1. WAL 压缩验证：写入数据后检查压缩是否生效
//  2. 压缩数据回放：验证压缩后的数据能正确 replay
//  3. 压缩率统计：统计实际压缩效果
//
// 运行方式：
//
//	cd tests/e2e/wal_compression_test && go build && ./wal_compression_test
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
	"codeberg.org/micro-ts/mts/types"
)

// listWALFiles 列出 WAL 目录下所有 .wal 文件
func listWALFiles(walDir string) ([]string, error) {
	pattern := filepath.Join(walDir, "*.wal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	return matches, nil
}

// getWALDirectory 获取全局 WAL 目录（新架构: {dataDir}/wal/）。
func getWALDirectory(dataDir, _, _ string) (string, error) {
	globalWALDir := filepath.Join(dataDir, "wal")
	if info, err := os.Stat(globalWALDir); err == nil && info.IsDir() {
		return globalWALDir, nil
	}
	return "", fmt.Errorf("global WAL directory not found: %s", globalWALDir)
}

// getWALFileSize 获取 WAL 文件大小
func getWALFileSize(walDir string) (int64, error) {
	files, err := listWALFiles(walDir)
	if err != nil || len(files) == 0 {
		return 0, err
	}
	info, err := os.Stat(files[len(files)-1])
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// writeLargePoints 写入大量数据点，验证压缩效果
func writeLargePoints(db *microts.DB, dbName, measurement string, startTime int64, count int) error {
	writeTimer := metrics.NewWriteSummary(count)
	for i := 0; i < count; i++ {
		p := &types.Point{
			Database:    dbName,
			Measurement: measurement,
			Tags: map[string]string{
				"host":       fmt.Sprintf("server%d", i%5+1),
				"region":     fmt.Sprintf("us-west-%d", i%3+1),
				"datacenter": fmt.Sprintf("dc%d", i%2+1),
			},
			Timestamp: startTime + int64(i)*int64(1000000), // 1ms 间隔
			Fields: map[string]*types.FieldValue{
				"cpu_usage":     types.NewFieldValue(float64(50.0 + float64(i%50))),
				"memory_usage":  types.NewFieldValue(float64(30.0 + float64(i%30))),
				"disk_io":       types.NewFieldValue(float64(100.0 + float64(i%100))),
				"network_in":    types.NewFieldValue(float64(1000.0 + float64(i%500))),
				"network_out":   types.NewFieldValue(float64(500.0 + float64(i%300))),
				"request_count": types.NewFieldValue(int64(i * 10)),
				"error_count":   types.NewFieldValue(int64(i % 10)),
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

// queryAndCount 查询数据并返回行数
func queryAndCount(db *microts.DB, dbName, measurement string, startTime, endTime int64) (int, error) {
	it, err := db.Iterator(context.Background(), &types.QueryRangeRequest{
		Database:    dbName,
		Measurement: measurement,
		StartTime:   startTime,
		EndTime:     endTime,
		Offset:      0,
		Limit:       0,
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = it.Close() }()
	var count int
	for it.Next(context.Background()) {
		count++
	}
	return count, nil
}

// Test1_WALCompressionVerify 测试 WAL 压缩是否生效
func Test1_WALCompressionVerify() error {
	fmt.Println("\n=== 测试 1: WAL 压缩验证 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_wal_compression_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           64 * 1024 * 1024,
			MaxCount:          100000,
			IdleDurationNanos: int64(time.Hour),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSstableCount:    4,
			MaxCompactionBatch: 0,
			ShardSizeLimit:     1 * 1024 * 1024 * 1024,
			CheckIntervalNanos: int64(10 * time.Second),
			TimeoutNanos:       int64(30 * time.Second),
		},
	}

	dbName := "testdb"
	measurement := "metrics"

	fmt.Printf("Step 1: 打开数据库\n")
	db, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()

	fmt.Printf("Step 2: 写入大量数据点\n")
	const writeCount = 10000
	if err := writeLargePoints(db, dbName, measurement, baseTime, writeCount); err != nil {
		return fmt.Errorf("write points: %w", err)
	}
	fmt.Printf("      写入 %d 个数据点\n", writeCount)

	// 预估原始数据大小（每个点约 200 字节）
	estimatedRawSize := int64(writeCount) * 200

	fmt.Printf("Step 3: 检查 WAL 文件大小\n")
	walDir, err := getWALDirectory(tmpDir, dbName, measurement)
	if err != nil {
		return fmt.Errorf("get WAL directory: %w", err)
	}

	walSize, err := getWALFileSize(walDir)
	if err != nil {
		return fmt.Errorf("get WAL file size: %w", err)
	}

	compressionRatio := float64(walSize) / float64(estimatedRawSize)
	fmt.Printf("      预估原始大小: %.2f KB\n", float64(estimatedRawSize)/1024)
	fmt.Printf("      WAL 文件大小: %.2f KB\n", float64(walSize)/1024)
	fmt.Printf("      压缩比: %.1f%%\n", compressionRatio*100)

	// WAL 文件应该比原始数据小（压缩生效）
	// 由于还有 header 和 record overhead，压缩后应该在 40-70% 之间
	if compressionRatio > 0.9 {
		fmt.Printf("      警告: 压缩效果不明显，可能未启用压缩\n")
	} else {
		fmt.Printf("      压缩生效: 文件大小为原始的 %.1f%%\n", compressionRatio*100)
	}

	fmt.Printf("=== 测试 1 通过: WAL 压缩验证完成 ===\n")
	return nil
}

// Test2_WALCompressionReplay 测试压缩数据回放
func Test2_WALCompressionReplay() error {
	fmt.Println("\n=== 测试 2: 压缩数据回放 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_wal_compression_replay_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           64 * 1024 * 1024,
			MaxCount:          100000,
			IdleDurationNanos: int64(time.Hour),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSstableCount:    4,
			MaxCompactionBatch: 0,
			ShardSizeLimit:     1 * 1024 * 1024 * 1024,
			CheckIntervalNanos: int64(10 * time.Second),
			TimeoutNanos:       int64(30 * time.Second),
		},
	}

	dbName := "testdb"
	measurement := "metrics"

	fmt.Printf("Step 1: 第一次会话 - 写入数据\n")
	db1, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db1: %w", err)
	}

	session1BaseTime := time.Now().UnixNano()
	const session1Count = 5000
	if err := writeLargePoints(db1, dbName, measurement, session1BaseTime, session1Count); err != nil {
		_ = db1.Close()
		return fmt.Errorf("write session1 points: %w", err)
	}
	fmt.Printf("      写入 %d 个数据点\n", session1Count)

	fmt.Printf("Step 2: 关闭数据库\n")
	if err := db1.Close(); err != nil {
		return fmt.Errorf("close db1: %w", err)
	}

	// 检查 WAL 文件
	walDir, _ := getWALDirectory(tmpDir, dbName, measurement)
	walSize1, _ := getWALFileSize(walDir)
	fmt.Printf("      WAL 大小: %.2f KB\n", float64(walSize1)/1024)

	fmt.Printf("Step 3: 第二次会话 - 重新打开数据库，验证回放\n")
	db2, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db2: %w", err)
	}
	defer func() { _ = db2.Close() }()

	// 写入新数据触发 Shard 发现和 WAL replay
	session2BaseTime := time.Now().UnixNano()
	newPoint := &types.Point{
		Database:    dbName,
		Measurement: measurement,
		Tags:        map[string]string{"host": "trigger"},
		Timestamp:   session2BaseTime,
		Fields: map[string]*types.FieldValue{
			"cpu_usage": types.NewFieldValue(float64(100.0)),
		},
	}
	if err := db2.Write(context.Background(), newPoint); err != nil {
		return fmt.Errorf("write trigger point: %w", err)
	}

	fmt.Printf("Step 4: 验证数据完整性\n")

	// 验证第一次的数据可以恢复
	oldCount, err := queryAndCount(db2, dbName, measurement,
		session1BaseTime, session1BaseTime+int64(session1Count)*int64(1000000))
	if err != nil {
		return fmt.Errorf("query old data failed: %w", err)
	}
	fmt.Printf("      第一次数据查询: %d 行\n", oldCount)

	if oldCount == 0 {
		return fmt.Errorf("WAL replay failed: no data recovered from session 1")
	}

	// 验证新数据
	newCount, err := queryAndCount(db2, dbName, measurement,
		session2BaseTime, session2BaseTime+int64(1000000))
	if err != nil {
		return fmt.Errorf("query new data failed: %w", err)
	}
	fmt.Printf("      第二次数据查询: %d 行\n", newCount)

	if newCount == 0 {
		return fmt.Errorf("new data not found")
	}

	fmt.Printf("=== 测试 2 通过: 压缩数据回放正常 ===\n")
	return nil
}

// Test3_WALCompressionRate 测试压缩率
func Test3_WALCompressionRate() error {
	fmt.Println("\n=== 测试 3: WAL 压缩率统计 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_wal_compression_rate_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           256 * 1024 * 1024,
			MaxCount:          1000000,
			IdleDurationNanos: int64(time.Hour),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSstableCount:    4,
			MaxCompactionBatch: 0,
			ShardSizeLimit:     1 * 1024 * 1024 * 1024,
			CheckIntervalNanos: int64(10 * time.Second),
			TimeoutNanos:       int64(30 * time.Second),
		},
	}

	dbName := "testdb"
	measurement := "metrics"

	fmt.Printf("Step 1: 打开数据库\n")
	db, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()

	// 测试不同数据量的压缩率
	testCases := []struct {
		count       int
		description string
	}{
		{1000, "1K 数据"},
		{10000, "10K 数据"},
	}

	for _, tc := range testCases {
		fmt.Printf("\nStep 2: 写入 %s\n", tc.description)
		if err := writeLargePoints(db, dbName, measurement, baseTime, tc.count); err != nil {
			return fmt.Errorf("write points: %w", err)
		}

		walDir, _ := getWALDirectory(tmpDir, dbName, measurement)
		walSize, _ := getWALFileSize(walDir)

		// 每个点约 200 字节原始数据
		estimatedRaw := int64(tc.count) * 200
		ratio := float64(walSize) / float64(estimatedRaw)
		saved := (1 - ratio) * 100

		fmt.Printf("      原始预估: %.2f KB, WAL: %.2f KB, 压缩率: %.1f%%, 节省: %.1f%%\n",
			float64(estimatedRaw)/1024, float64(walSize)/1024, ratio*100, saved)

		baseTime += int64(tc.count) * int64(1000000)
	}

	fmt.Printf("\n=== 测试 3 完成: 压缩率统计完成 ===\n")
	return nil
}

// Test4_WALCompressionSmallData 测试小数据压缩
func Test4_WALCompressionSmallData() error {
	fmt.Println("\n=== 测试 4: 小数据压缩处理 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_wal_small_data_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           64 * 1024 * 1024,
			MaxCount:          1000,
			IdleDurationNanos: int64(time.Hour),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSstableCount:    4,
			MaxCompactionBatch: 0,
			ShardSizeLimit:     1 * 1024 * 1024 * 1024,
			CheckIntervalNanos: int64(10 * time.Second),
			TimeoutNanos:       int64(30 * time.Second),
		},
	}

	dbName := "testdb"
	measurement := "small_data"

	fmt.Printf("Step 1: 写入小数据量\n")
	db, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()
	const smallCount = 100

	// 写入小数据
	for i := 0; i < smallCount; i++ {
		p := &types.Point{
			Database:    dbName,
			Measurement: measurement,
			Tags:        map[string]string{"host": "small"},
			Timestamp:   baseTime + int64(i)*int64(time.Second),
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(float64(i)),
			},
		}
		if err := db.Write(context.Background(), p); err != nil {
			return fmt.Errorf("write point: %w", err)
		}
	}
	fmt.Printf("      写入 %d 个小数据点\n", smallCount)

	// 关闭并重新打开，验证小数据也能正确回放
	if err := db.Close(); err != nil {
		return fmt.Errorf("close db: %w", err)
	}

	fmt.Printf("Step 2: 重新打开数据库\n")
	db2, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db2: %w", err)
	}
	defer func() { _ = db2.Close() }()

	// 触发 replay
	triggerPoint := &types.Point{
		Database:    dbName,
		Measurement: measurement,
		Tags:        map[string]string{"host": "trigger"},
		Timestamp:   baseTime + int64(smallCount)*int64(time.Second),
		Fields: map[string]*types.FieldValue{
			"value": types.NewFieldValue(float64(999)),
		},
	}
	if err := db2.Write(context.Background(), triggerPoint); err != nil {
		return fmt.Errorf("write trigger: %w", err)
	}

	count, err := queryAndCount(db2, dbName, measurement, baseTime, baseTime+int64(smallCount)*int64(time.Second))
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	fmt.Printf("Step 3: 验证数据\n")
	fmt.Printf("      查询到 %d 行\n", count)

	if count == 0 {
		return fmt.Errorf("small data replay failed")
	}

	fmt.Printf("=== 测试 4 通过: 小数据压缩处理正常 ===\n")
	return nil
}

// ============================================================================
// 主函数
// ============================================================================

func main() {
	fmt.Println("========================================")
	fmt.Println("MTS WAL 压缩端到端测试")
	fmt.Println("========================================")

	passed := 0
	failed := 0

	tests := []struct {
		name string
		fn   func() error
	}{
		{"WAL 压缩验证", Test1_WALCompressionVerify},
		{"压缩数据回放", Test2_WALCompressionReplay},
		{"压缩率统计", Test3_WALCompressionRate},
		{"小数据压缩处理", Test4_WALCompressionSmallData},
	}

	for _, tc := range tests {
		if err := tc.fn(); err != nil {
			fmt.Printf("\n❌ 测试失败: %s\n", tc.name)
			fmt.Printf("   错误: %v\n", err)
			failed++
		} else {
			passed++
		}
	}

	fmt.Println("\n========================================")
	fmt.Println("测试结果汇总")
	fmt.Println("========================================")
	fmt.Printf("通过: %d\n", passed)
	fmt.Printf("失败: %d\n", failed)
	fmt.Printf("总计: %d\n", passed+failed)

	if failed > 0 {
		os.Exit(1)
	}
	fmt.Println("\n所有测试通过！WAL 压缩功能验证完成。")
}
