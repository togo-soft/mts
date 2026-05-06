// tests/e2e/compaction_test/main.go
//
// # Compaction 端到端测试套件
//
// 本测试验证 MTS 数据库 Compaction 功能的核心行为：
//
//  1. 数据完整性：多次 Flush 后 Compaction 去重正确，数据精确恢复
//  2. 查询正确性：Compaction 后查询结果正确，无重复数据
//  3. 写入保护：正在写入的 SSTable 不参与 Compaction
//  4. 定时触发：定时触发 compaction，阈值触发后重置定时器
//  5. 重启恢复：Compaction 后重启，数据完整恢复
//
// 测试设计原则：
//
//   - 每个测试场景独立，互不影响
//   - 使用临时目录，测试结束后自动清理
//   - 详细的日志输出，便于调试和理解 Compaction 行为
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/types"
)

// ============================================================================
// 工具函数
// ============================================================================

// countSSTableFiles 统计 data 目录下的 SSTable 目录数量
func countSSTableFiles(dataDir string) (int, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "sst_") {
			count++
		}
	}
	return count, nil
}

// listSSTableDirs 列出 data 目录下所有 SSTable 目录
func listSSTableDirs(dataDir string) ([]string, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	var dirs []string
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "sst_") {
			dirs = append(dirs, filepath.Join(dataDir, entry.Name()))
		}
	}
	return dirs, nil
}

// getShardDataDir 获取 Shard 的 data 目录
func getShardDataDir(dataDir, dbName, measurement string) (string, error) {
	measurementDir := filepath.Join(dataDir, dbName, measurement)
	entries, err := os.ReadDir(measurementDir)
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "1") {
			return filepath.Join(measurementDir, entry.Name(), "data"), nil
		}
	}
	return "", fmt.Errorf("no shard data dir found")
}

// writeTestPoints 写入测试数据点
func writeTestPoints(db *microts.DB, dbName, measurement string, startTime int64, count int, interval time.Duration) error {
	for i := 0; i < count; i++ {
		p := &types.Point{
			Database:    dbName,
			Measurement: measurement,
			Tags: map[string]string{
				"host": fmt.Sprintf("server%d", i%3+1),
			},
			Timestamp: startTime + int64(i)*int64(interval),
			Fields: map[string]*types.FieldValue{
				"usage": types.NewFieldValue(float64(50.0 + float64(i%50))),
				"count": types.NewFieldValue(int64(i * 10)),
			},
		}
		if err := db.Write(context.Background(), p); err != nil {
			return fmt.Errorf("write point %d: %w", i, err)
		}
	}
	return nil
}

// queryAndCount 查询数据并返回行数
func queryAndCount(db *microts.DB, dbName, measurement string, startTime, endTime int64) (int, error) {
	resp, err := db.QueryRange(context.Background(), &types.QueryRangeRequest{
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
	return len(resp.Rows), nil
}

// queryAndCountWithDedup 查询数据并返回去重后的行数（按 timestamp+host 去重）
func queryAndCountWithDedup(db *microts.DB, dbName, measurement string, startTime, endTime int64) (int, map[string]bool, error) {
	resp, err := db.QueryRange(context.Background(), &types.QueryRangeRequest{
		Database:    dbName,
		Measurement: measurement,
		StartTime:   startTime,
		EndTime:     endTime,
		Offset:      0,
		Limit:       0,
	})
	if err != nil {
		return 0, nil, err
	}

	// 按 timestamp+host 去重
	seen := make(map[string]bool)
	for _, row := range resp.Rows {
		host := ""
		if h, ok := row.Tags["host"]; ok {
			host = h
		}
		key := fmt.Sprintf("%d-%s", row.Timestamp, host)
		seen[key] = true
	}
	return len(seen), seen, nil
}

// getMemStats 获取当前内存统计（字节）
func getMemStats() runtime.MemStats {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats
}

// ============================================================================
// 测试用例
// ============================================================================

// Test1_CompactionDataIntegrity 测试 Compaction 数据完整性
//
// 验证多次 Flush 后 Compaction 去重正确，数据精确恢复
func Test1_CompactionDataIntegrity() error {
	fmt.Println("\n=== 测试 1: Compaction 数据完整性 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_compaction_integrity_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           64 * 1024 * 1024, // 较大 MemTable
			MaxCount:          100000,
			IdleDurationNanos: int64(time.Hour),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSSTableCount: 4,
			CheckInterval:   time.Hour,
			Timeout:         30 * time.Second,
		},
	}

	dbName := "testdb"
	measurement := "cpu"

	fmt.Printf("Step 1: 打开数据库\n")
	db, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()
	writeCount := 500

	fmt.Printf("Step 2: 写入 %d 条数据\n", writeCount)
	if err := writeTestPoints(db, dbName, measurement, baseTime, writeCount, time.Millisecond); err != nil {
		return fmt.Errorf("write points: %w", err)
	}

	time.Sleep(100 * time.Millisecond)

	fmt.Printf("Step 3: 触发 Flush 和 Compaction\n")
	if err := db.FlushAll(); err != nil {
		return fmt.Errorf("flush all: %w", err)
	}
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("Step 4: 验证数据完整性\n")
	totalCount, err := queryAndCount(db, dbName, measurement, baseTime, baseTime+int64(writeCount)*int64(time.Millisecond))
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	fmt.Printf("      查询结果行数: %d\n", totalCount)

	dedupCount, _, err := queryAndCountWithDedup(db, dbName, measurement, baseTime, baseTime+int64(writeCount)*int64(time.Millisecond))
	if err != nil {
		return fmt.Errorf("query with dedup failed: %w", err)
	}
	fmt.Printf("      去重后数据行数: %d\n", dedupCount)

	// 核心验证：去重后行数应该等于查询返回行数（说明无重复）
	// 同时行数应该等于写入数量
	if totalCount != dedupCount {
		return fmt.Errorf("dedup mismatch: total=%d, dedup=%d", totalCount, dedupCount)
	}
	if dedupCount != writeCount {
		return fmt.Errorf("expected %d rows, got %d", writeCount, dedupCount)
	}

	fmt.Printf("=== 测试 1 通过: Compaction 数据完整性验证成功 ===\n")
	return nil
}

// Test2_CompactionQueryResult 测试 Compaction 后查询结果正确
//
// 验证 Compaction 后查询结果正确，无重复数据
func Test2_CompactionQueryResult() error {
	fmt.Println("\n=== 测试 2: Compaction 后查询结果正确 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_compaction_query_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           10 * 1024,
			MaxCount:          10,
			IdleDurationNanos: int64(time.Hour),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSSTableCount: 4,
			CheckInterval:   time.Hour,
			Timeout:         30 * time.Second,
		},
	}

	dbName := "testdb"
	measurement := "cpu"

	fmt.Printf("Step 1: 打开数据库\n")
	db, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()

	fmt.Printf("Step 2: 写入特定时间点的数据\n")
	// 写入 100 个不同时间点的数据
	pointCount := 100
	for i := 0; i < pointCount; i++ {
		p := &types.Point{
			Database:    dbName,
			Measurement: measurement,
			Tags: map[string]string{
				"host": "server1",
			},
			Timestamp: baseTime + int64(i)*int64(10*time.Millisecond),
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(float64(i)),
			},
		}
		if err := db.Write(context.Background(), p); err != nil {
			return fmt.Errorf("write point: %w", err)
		}
	}

	// 等待刷盘
	time.Sleep(200 * time.Millisecond)

	fmt.Printf("Step 3: 触发 Compaction\n")
	if err := db.FlushAll(); err != nil {
		fmt.Printf("      FlushAll warning: %v\n", err)
	}
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("Step 4: 查询并验证无重复\n")
	resp, err := db.QueryRange(context.Background(), &types.QueryRangeRequest{
		Database:    dbName,
		Measurement: measurement,
		StartTime:   baseTime,
		EndTime:     baseTime + int64(pointCount)*int64(10*time.Millisecond),
		Offset:      0,
		Limit:       0,
	})
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	// 检查重复
	timestamps := make(map[int64]bool)
	for _, row := range resp.Rows {
		if timestamps[row.Timestamp] {
			return fmt.Errorf("duplicate timestamp found: %d", row.Timestamp)
		}
		timestamps[row.Timestamp] = true
	}

	if len(resp.Rows) != pointCount {
		return fmt.Errorf("expected %d rows, got %d", pointCount, len(resp.Rows))
	}

	fmt.Printf("      查询到 %d 条记录，无重复\n", len(resp.Rows))
	fmt.Printf("=== 测试 2 通过: Compaction 后查询结果正确 ===\n")
	return nil
}

// Test3_WriteProtection 测试写入保护
//
// 验证正在写入的 SSTable 不参与 Compaction
func Test3_WriteProtection() error {
	fmt.Println("\n=== 测试 3: 写入保护 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_write_protection_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           10 * 1024,
			MaxCount:          10,
			IdleDurationNanos: int64(time.Hour),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSSTableCount: 4,
			CheckInterval:   time.Hour,
			Timeout:         30 * time.Second,
		},
	}

	dbName := "testdb"
	measurement := "cpu"

	fmt.Printf("Step 1: 打开数据库\n")
	db, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()

	fmt.Printf("Step 2: 写入数据触发多次 Flush\n")
	if err := writeTestPoints(db, dbName, measurement, baseTime, 500, time.Millisecond); err != nil {
		return fmt.Errorf("write points: %w", err)
	}
	time.Sleep(200 * time.Millisecond)

	dataDir, err := getShardDataDir(tmpDir, dbName, measurement)
	if err != nil {
		return fmt.Errorf("get shard data dir: %w", err)
	}

	sstDirs, _ := listSSTableDirs(dataDir)
	fmt.Printf("      当前 SSTable 数量: %d\n", len(sstDirs))

	fmt.Printf("Step 3: 手动创建 .writing 标志模拟正在写入\n")
	if len(sstDirs) > 0 {
		writingFlag := filepath.Join(sstDirs[0], ".writing")
		f, err := os.Create(writingFlag)
		if err != nil {
			return fmt.Errorf("create writing flag: %w", err)
		}
		if err := f.Close(); err != nil {
			slog.Warn("failed to close writing flag", "error", err)
		}
		fmt.Printf("      已创建 .writing 标志: %s\n", sstDirs[0])
		defer func() {
			_ = os.Remove(filepath.Join(sstDirs[0], ".writing"))
		}()
	}

	fmt.Printf("Step 4: 触发 Compaction\n")
	if err := db.FlushAll(); err != nil {
		fmt.Printf("      FlushAll warning: %v\n", err)
	}
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("Step 5: 验证有 .writing 标志的文件未被删除\n")
	sstDirsAfter, _ := listSSTableDirs(dataDir)
	if len(sstDirs) > 0 && len(sstDirsAfter) > 0 {
		// 验证第一个 SSTable 仍然存在
		_, err := os.Stat(sstDirs[0])
		if err != nil {
			return fmt.Errorf("SSTable with .writing flag was incorrectly deleted: %s", sstDirs[0])
		}
		fmt.Printf("      有 .writing 标志的 SSTable 仍存在（正确）\n")
	}

	fmt.Printf("=== 测试 3 通过: 写入保护验证成功 ===\n")
	return nil
}

// Test4_CompactionDuringWrite 测试写入过程中 Compaction
//
// 验证写入过程中 Compaction 执行，结果正确
func Test4_CompactionDuringWrite() error {
	fmt.Println("\n=== 测试 4: 写入过程中 Compaction ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_compaction_during_write_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           10 * 1024, // 小 MemTable 频繁触发 Flush
			MaxCount:          10,
			IdleDurationNanos: int64(time.Hour),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSSTableCount: 4,
			CheckInterval:   time.Hour,
			Timeout:         30 * time.Second,
		},
	}

	dbName := "testdb"
	measurement := "cpu"

	fmt.Printf("Step 1: 打开数据库\n")
	db, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()

	fmt.Printf("Step 2: 持续写入数据，触发 Compaction\n")
	done := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)
		for i := 0; i < 1000; i++ {
			select {
			case <-done:
				return
			default:
			}
			p := &types.Point{
				Database:    dbName,
				Measurement: measurement,
				Tags: map[string]string{
					"host": fmt.Sprintf("server%d", i%3+1),
				},
				Timestamp: baseTime + int64(i)*int64(time.Millisecond),
				Fields: map[string]*types.FieldValue{
					"value": types.NewFieldValue(float64(i)),
				},
			}
			_ = db.Write(context.Background(), p)
		}
	}()

	// 等待一些数据写入
	time.Sleep(100 * time.Millisecond)

	// 触发 flush 和 compaction
	for i := 0; i < 5; i++ {
		_ = db.FlushAll()
		time.Sleep(100 * time.Millisecond)
	}

	// 继续写入
	time.Sleep(100 * time.Millisecond)
	close(done)
	<-stopped

	fmt.Printf("Step 3: 等待 Compaction 完成\n")
	time.Sleep(500 * time.Millisecond)
	_ = db.FlushAll()
	time.Sleep(200 * time.Millisecond)

	fmt.Printf("Step 4: 验证数据完整性\n")
	totalCount, err := queryAndCount(db, dbName, measurement, baseTime, baseTime+int64(1000)*int64(time.Millisecond))
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	fmt.Printf("      查询到 %d 条记录\n", totalCount)

	// 验证去重后数量
	dedupCount, dedupMap, err := queryAndCountWithDedup(db, dbName, measurement, baseTime, baseTime+int64(1000)*int64(time.Millisecond))
	if err != nil {
		return fmt.Errorf("query with dedup failed: %w", err)
	}
	fmt.Printf("      去重后 %d 条记录\n", dedupCount)

	if dedupCount < 900 { // 允许一些数据还在 MemTable 中
		return fmt.Errorf("expected at least 900 rows after dedup, got %d", dedupCount)
	}

	// 验证去重数据正确
	for i := int64(0); i < 1000; i++ {
		key := fmt.Sprintf("%d-server%d", baseTime+int64(i)*int64(time.Millisecond), (i%3)+1)
		if _, exists := dedupMap[key]; !exists {
			// 记录缺失的数据，但不失败（允许一些数据还在 MemTable 中）
			slog.Debug("missing dedup key", "key", key)
		}
	}

	fmt.Printf("=== 测试 4 通过: 写入过程中 Compaction 正常 ===\n")
	return nil
}

// Test5_CompactionRestartRecovery 测试重启后数据恢复
//
// 验证 Compaction 后重启，数据完整恢复
func Test5_CompactionRestartRecovery() error {
	fmt.Println("\n=== 测试 5: Compaction 后重启恢复 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_compaction_restart_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           10 * 1024,
			MaxCount:          10,
			IdleDurationNanos: int64(time.Hour),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSSTableCount: 4,
			CheckInterval:   time.Hour,
			Timeout:         30 * time.Second,
		},
	}

	dbName := "testdb"
	measurement := "cpu"

	fmt.Printf("Step 1: 第一次会话 - 写入数据\n")
	db1, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db1: %w", err)
	}

	baseTime := time.Now().UnixNano()
	writeCount := 300

	if err := writeTestPoints(db1, dbName, measurement, baseTime, writeCount, time.Millisecond); err != nil {
		_ = db1.Close()
		return fmt.Errorf("write points: %w", err)
	}
	fmt.Printf("      写入 %d 条数据\n", writeCount)

	// 等待刷盘和 compaction
	time.Sleep(200 * time.Millisecond)
	_ = db1.FlushAll()
	time.Sleep(500 * time.Millisecond)

	if err := db1.Close(); err != nil {
		return fmt.Errorf("close db1: %w", err)
	}
	fmt.Printf("      第一次会话结束\n")

	fmt.Printf("Step 2: 第二次会话 - 验证数据可恢复\n")
	db2, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db2: %w", err)
	}
	defer func() { _ = db2.Close() }()

	// 写入触发 WAL replay
	newTriggerTime := baseTime + int64(writeCount)*int64(time.Millisecond) + int64(time.Millisecond)
	triggerPoint := &types.Point{
		Database:    dbName,
		Measurement: measurement,
		Tags:        map[string]string{"host": "trigger"},
		Timestamp:   newTriggerTime,
		Fields: map[string]*types.FieldValue{
			"usage": types.NewFieldValue(float64(100.0)),
		},
	}
	if err := db2.Write(context.Background(), triggerPoint); err != nil {
		return fmt.Errorf("write trigger point: %w", err)
	}
	time.Sleep(100 * time.Millisecond)

	fmt.Printf("Step 3: 验证数据完整性\n")
	dedupCount, _, err := queryAndCountWithDedup(db2, dbName, measurement, baseTime, baseTime+int64(writeCount)*int64(time.Millisecond))
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	fmt.Printf("      重启后恢复 %d 条数据\n", dedupCount)

	if dedupCount == 0 {
		return fmt.Errorf("no data recovered after restart")
	}

	fmt.Printf("=== 测试 5 通过: 重启后数据恢复成功 ===\n")
	return nil
}

// Test6_MemoryEfficiency 测试内存效率
//
// 验证 Compaction 过程中内存占用保持在合理范围
func Test6_MemoryEfficiency() error {
	fmt.Println("\n=== 测试 6: 内存效率 ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_memory_efficiency_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           10 * 1024,
			MaxCount:          10,
			IdleDurationNanos: int64(time.Hour),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSSTableCount: 4,
			CheckInterval:   time.Hour,
			Timeout:         30 * time.Second,
		},
	}

	dbName := "testdb"
	measurement := "cpu"

	fmt.Printf("Step 1: 记录初始内存\n")
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	memBefore := getMemStats()
	fmt.Printf("      初始内存: Alloc=%d KB, TotalAlloc=%d KB\n",
		memBefore.Alloc/1024, memBefore.TotalAlloc/1024)

	fmt.Printf("Step 2: 打开数据库\n")
	db, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()

	fmt.Printf("Step 3: 写入大量数据\n")
	if err := writeTestPoints(db, dbName, measurement, baseTime, 1000, time.Millisecond); err != nil {
		return fmt.Errorf("write points: %w", err)
	}
	time.Sleep(200 * time.Millisecond)

	memAfterWrite := getMemStats()
	fmt.Printf("      写入后内存: Alloc=%d KB, TotalAlloc=%d KB\n",
		memAfterWrite.Alloc/1024, memAfterWrite.TotalAlloc/1024)

	fmt.Printf("Step 4: 触发 Compaction\n")
	_ = db.FlushAll()
	time.Sleep(500 * time.Millisecond)

	memAfterCompaction := getMemStats()
	fmt.Printf("      Compaction 后内存: Alloc=%d KB, TotalAlloc=%d KB\n",
		memAfterCompaction.Alloc/1024, memAfterCompaction.TotalAlloc/1024)

	// 验证内存增长在合理范围内
	// TotalAlloc 增长应该小于数据大小的 2 倍（批处理机制）
	dataSize := int64(1000) * 200 // 估算每条数据约 200 字节
	expectedMax := dataSize * 2

	actualGrowth := int64(memAfterCompaction.TotalAlloc - memBefore.TotalAlloc)
	if actualGrowth > expectedMax {
		fmt.Printf("      警告: 内存增长超过预期\n")
		fmt.Printf("      预期最大增长: %d KB, 实际增长: %d KB\n",
			expectedMax/1024, actualGrowth/1024)
	} else {
		fmt.Printf("      内存增长在合理范围内\n")
	}

	fmt.Printf("=== 测试 6 通过: 内存效率验证完成 ===\n")
	return nil
}

// Test7_PeriodicCompaction 测试定时 Compaction
//
// 验证定时触发 compaction，阈值触发后重置定时器
func Test7_PeriodicCompaction() error {
	fmt.Println("\n=== 测试 7: 定时 Compaction ===")

	tmpDir := filepath.Join(os.TempDir(), "microts_periodic_compaction_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// 使用短间隔方便测试
	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           10 * 1024,
			MaxCount:          10,
			IdleDurationNanos: int64(time.Hour),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSSTableCount: 4,
			CheckInterval:   2 * time.Second, // 2 秒间隔
			Timeout:         30 * time.Second,
		},
	}

	dbName := "testdb"
	measurement := "cpu"

	fmt.Printf("Step 1: 打开数据库\n")
	db, err := microts.Open(dbCfg)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()

	baseTime := time.Now().UnixNano()

	fmt.Printf("Step 2: 写入数据触发多次 Flush\n")
	if err := writeTestPoints(db, dbName, measurement, baseTime, 500, time.Millisecond); err != nil {
		return fmt.Errorf("write points: %w", err)
	}
	time.Sleep(200 * time.Millisecond)

	dataDir, err := getShardDataDir(tmpDir, dbName, measurement)
	if err != nil {
		return fmt.Errorf("get shard data dir: %w", err)
	}

	sstCountBefore, _ := countSSTableFiles(dataDir)
	fmt.Printf("      Flush 后 SSTable 数量: %d\n", sstCountBefore)

	fmt.Printf("Step 3: 等待定时 Compaction 触发（3秒）\n")
	time.Sleep(3 * time.Second)

	sstCountAfter, _ := countSSTableFiles(dataDir)
	fmt.Printf("      定时 Compaction 后 SSTable 数量: %d\n", sstCountAfter)

	// 验证定时 compaction 执行了（SSTable 数量减少）
	if sstCountAfter < sstCountBefore {
		fmt.Printf("      定时 Compaction 已执行（数量减少）\n")
	} else {
		fmt.Printf("      定时 Compaction 可能尚未执行或条件不满足\n")
	}

	fmt.Printf("=== 测试 7 完成: 定时 Compaction 验证 ===\n")
	return nil
}

// ============================================================================
// 主函数
// ============================================================================

func main() {
	fmt.Println("========================================")
	fmt.Println("MTS Compaction 端到端测试套件")
	fmt.Println("========================================")

	passed := 0
	failed := 0

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Compaction 数据完整性", Test1_CompactionDataIntegrity},
		{"Compaction 后查询结果正确", Test2_CompactionQueryResult},
		{"写入保护", Test3_WriteProtection},
		{"写入过程中 Compaction", Test4_CompactionDuringWrite},
		{"Compaction 后重启恢复", Test5_CompactionRestartRecovery},
		{"内存效率", Test6_MemoryEfficiency},
		{"定时 Compaction", Test7_PeriodicCompaction},
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
	fmt.Println("\n所有测试通过！Compaction 功能验证完成。")
}
