// tests/e2e/downsample_test/main.go
//
// 降采样端到端测试套件 — 验证降采样数据生成、聚合函数正确性及查询路径。
//
// 测试场景：
//  1. 基础降采样：写入数据 → flush → 触发降采样 → 验证文件生成
//  2. 聚合函数正确性：验证 avg/max/min/sum/count/first/last 计算正确
//  3. 多窗口降采样：同一数据降采样到多个窗口大小
//  4. 降采样查询：通过 downsample_window_nanos 查询降采样数据
//  5. 速率函数：验证 rate/irate/derivative/diff 计算正确
//  6. 幂等性：重复触发降采样不产生重复数据
//  7. 重启恢复：重启后降采样数据可查询
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/types"
)

const (
	testDB          = "testdb"
	testMeasurement = "cpu"
)

// downsampleConfig 创建测试用降采样配置。
func downsampleConfig(windowSeconds int64, functions []string) *types.DownsampleConfig {
	if functions == nil {
		functions = []string{"avg", "max", "min", "sum", "count", "first", "last"}
	}
	return &types.DownsampleConfig{
		Enabled: true,
		Rules: []*types.DownsampleRule{
			{
				WindowNanos: windowSeconds * 1e9,
				Functions:   functions,
			},
		},
	}
}

// setupDB 创建测试数据库并返回 DB 实例和临时目录。
func setupDB(name string, shardDuration, retention time.Duration) (*microts.DB, string, error) {
	tmpDir := filepath.Join(os.TempDir(), "microts_ds_"+name)
	_ = os.RemoveAll(tmpDir)

	db, err := microts.Open(&microts.Config{
		DataDir:              tmpDir,
		ShardDurationNanos:   int64(shardDuration),
		MemTableCfg:          microts.DefaultMemTableConfig(),
		CompactionCfg:        microts.DefaultCompactionConfig(),
		RetentionPeriodNanos: int64(retention),
	})
	if err != nil {
		return nil, tmpDir, fmt.Errorf("open: %w", err)
	}
	return db, tmpDir, nil
}

// writeOldPoints 写入带有旧时间戳的数据点（使其处于保留期之前）。
func writeOldPoints(db *microts.DB, count int, ageSeconds int64, interval time.Duration) error {
	baseTime := time.Now().UnixNano() - ageSeconds*1e9
	for i := 0; i < count; i++ {
		p := &types.Point{
			Database:    testDB,
			Measurement: testMeasurement,
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   baseTime + int64(i)*int64(interval),
			Fields: map[string]*types.FieldValue{
				"cpu_usage":  types.NewFieldValue(float64(i) * 1.5),
				"cpu_count":  types.NewFieldValue(int64(i * 10)),
				"cpu_active": types.NewFieldValue(float64(i%100) * 0.1),
			},
		}
		if err := db.Write(context.Background(), p); err != nil {
			return fmt.Errorf("write point %d: %w", i, err)
		}
	}
	return nil
}

// writeCounterPoints 写入计数器类型的数据（用于 rate/irate 验证）。
func writeCounterPoints(db *microts.DB, count int, ageSeconds int64, interval time.Duration) error {
	baseTime := time.Now().UnixNano() - ageSeconds*1e9
	for i := 0; i < count; i++ {
		p := &types.Point{
			Database:    testDB,
			Measurement: testMeasurement,
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   baseTime + int64(i)*int64(interval),
			Fields: map[string]*types.FieldValue{
				"counter": types.NewFieldValue(float64(100 + i*10)),
			},
		}
		if err := db.Write(context.Background(), p); err != nil {
			return fmt.Errorf("write point %d: %w", i, err)
		}
	}
	return nil
}

// waitForCompaction 等待 UnorderedCompactor 将数据从 unordered/ 移到 shard data/。
func waitForCompaction(dataRoot string) {
	// UnorderedCompactor 每 500ms 运行一次，等待 2 秒确保其完成
	time.Sleep(2 * time.Second)
}

// countDownsampleFiles 统计降采样目录中的文件数。
func countDownsampleFiles(dataRoot string) int {
	n := 0
	_ = filepath.Walk(dataRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".bin") &&
			strings.Contains(path, "downsampled") {
			n++
		}
		return nil
	})
	return n
}

// hasDownsampleDone 检查降采样完成标记。
func hasDownsampleDone(dataRoot string) bool {
	found := false
	_ = filepath.Walk(dataRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.Name() == "_downsample_done" {
			found = true
		}
		return nil
	})
	return found
}

// ============================================================================
// 测试用例
// ============================================================================

// testBasicDownsample 基础降采样：写数据 → flush → 等待 compaction → 降采样 → 验证文件。
func testBasicDownsample() error {
	shardDuration := 10 * time.Second
	retention := 5 * time.Second

	db, tmpDir, err := setupDB("basic", shardDuration, retention)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dsCfg := downsampleConfig(5, nil)
	if err := db.CreateDatabase(context.Background(), testDB, retention, dsCfg); err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	fmt.Printf("写入 20 个点 (时间戳: 30s 前)...\n")
	if err := writeOldPoints(db, 20, 30, 100*time.Millisecond); err != nil {
		return err
	}

	// Flush 到 unordered → 等待 Compactor 将其移至 shard data/
	if err := db.FlushAll(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	waitForCompaction(tmpDir)

	// 触发降采样
	fmt.Println("触发降采样...")
	db.ForceDownsample()
	time.Sleep(300 * time.Millisecond)

	// 验证降采样文件存在
	n := countDownsampleFiles(tmpDir)
	fmt.Printf("降采样 .bin 文件数: %d\n", n)
	if n == 0 {
		return fmt.Errorf("未生成降采样文件")
	}

	// 验证 _downsample_done 标记
	if !hasDownsampleDone(tmpDir) {
		return fmt.Errorf("缺少 _downsample_done 标记")
	}

	fmt.Printf("PASS: 降采样文件已生成 (%d 个), _downsample_done 标记存在\n", n)
	return nil
}

// testAggregationCorrectness 验证聚合函数正确性。
func testAggregationCorrectness() error {
	shardDuration := 5 * time.Second
	retention := 2 * time.Second

	db, tmpDir, err := setupDB("agg", shardDuration, retention)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dsCfg := downsampleConfig(3, []string{"avg", "max", "min", "sum", "count", "first", "last"})
	if err := db.CreateDatabase(context.Background(), testDB, retention, dsCfg); err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	// 写入 6 个点，值依次递增: 0, 1.5, 3.0, 4.5, 6.0, 7.5
	fmt.Printf("写入 6 个点 (值: 0 到 7.5)...\n")
	if err := writeOldPoints(db, 6, 30, 50*time.Millisecond); err != nil {
		return err
	}

	if err := db.FlushAll(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	waitForCompaction(tmpDir)

	db.ForceDownsample()
	time.Sleep(300 * time.Millisecond)

	// 查询降采样数据
	now := time.Now().UnixNano()
	req := &types.QueryRangeRequest{
		Database:              testDB,
		Measurement:           testMeasurement,
		StartTime:             0,
		EndTime:               now,
		DownsampleWindowNanos: 3 * 1e9,
	}

	rows, err := queryRows(db, req)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	if len(rows) == 0 {
		return fmt.Errorf("降采样查询返回 0 行")
	}
	fmt.Printf("降采样查询返回 %d 行\n", len(rows))

	row := rows[0]

	// 验证聚合字段存在且合理
	requiredKeys := []string{"avg_cpu_usage", "max_cpu_usage", "min_cpu_usage", "sum_cpu_usage", "count_cpu_usage", "first_cpu_usage", "last_cpu_usage"}
	for _, key := range requiredKeys {
		fv := row.GetFieldValue(key)
		if fv == nil {
			return fmt.Errorf("缺少字段: %s", key)
		}
	}

	// cpu_usage: 0, 1.5, 3.0, 4.5, 6.0, 7.5
	// avg = (0+1.5+3+4.5+6+7.5)/6 = 3.75
	avgVal := row.GetFieldValue("avg_cpu_usage").GetFloatValue()
	if avgVal != 3.75 {
		return fmt.Errorf("avg 不正确: want 3.75, got %v", avgVal)
	}
	fmt.Printf("  avg_cpu_usage = %v ✓\n", avgVal)

	// max = 7.5
	maxVal := row.GetFieldValue("max_cpu_usage").GetFloatValue()
	if maxVal != 7.5 {
		return fmt.Errorf("max 不正确: want 7.5, got %v", maxVal)
	}
	fmt.Printf("  max_cpu_usage = %v ✓\n", maxVal)

	// min = 0
	minVal := row.GetFieldValue("min_cpu_usage").GetFloatValue()
	if minVal != 0.0 {
		return fmt.Errorf("min 不正确: want 0.0, got %v", minVal)
	}
	fmt.Printf("  min_cpu_usage = %v ✓\n", minVal)

	// count = 6
	countVal := row.GetFieldValue("count_cpu_usage").GetIntValue()
	if countVal != 6 {
		return fmt.Errorf("count 不正确: want 6, got %v", countVal)
	}
	fmt.Printf("  count_cpu_usage = %v ✓\n", countVal)

	// first = 0
	firstVal := row.GetFieldValue("first_cpu_usage").GetFloatValue()
	if firstVal != 0.0 {
		return fmt.Errorf("first 不正确: want 0.0, got %v", firstVal)
	}
	fmt.Printf("  first_cpu_usage = %v ✓\n", firstVal)

	// last = 7.5
	lastVal := row.GetFieldValue("last_cpu_usage").GetFloatValue()
	if lastVal != 7.5 {
		return fmt.Errorf("last 不正确: want 7.5, got %v", lastVal)
	}
	fmt.Printf("  last_cpu_usage = %v ✓\n", lastVal)

	fmt.Println("PASS: 所有聚合函数值正确")
	return nil
}

// testMultipleWindows 验证同一数据降采样到多个窗口大小。
func testMultipleWindows() error {
	shardDuration := 10 * time.Second
	retention := 5 * time.Second

	db, tmpDir, err := setupDB("multiwindow", shardDuration, retention)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dsCfg := &types.DownsampleConfig{
		Enabled: true,
		Rules: []*types.DownsampleRule{
			{WindowNanos: 2 * 1e9, Functions: []string{"avg", "count"}},
			{WindowNanos: 5 * 1e9, Functions: []string{"avg", "count"}},
		},
	}
	if err := db.CreateDatabase(context.Background(), testDB, retention, dsCfg); err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	fmt.Printf("写入 10 个点...\n")
	if err := writeOldPoints(db, 10, 30, 100*time.Millisecond); err != nil {
		return err
	}

	if err := db.FlushAll(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	waitForCompaction(tmpDir)

	db.ForceDownsample()
	time.Sleep(300 * time.Millisecond)

	// 验证两个窗口的降采样文件都存在
	n := countDownsampleFiles(tmpDir)
	fmt.Printf("降采样文件总数: %d\n", n)
	if n < 2 {
		return fmt.Errorf("多窗口降采样文件数不足: 预期 ≥2, 实际 %d", n)
	}

	// 查询 2s 窗口
	now := time.Now().UnixNano()
	req2s := &types.QueryRangeRequest{
		Database:              testDB,
		Measurement:           testMeasurement,
		StartTime:             0,
		EndTime:               now,
		DownsampleWindowNanos: 2 * 1e9,
	}
	rows2s, err := queryRows(db, req2s)
	if err != nil {
		return err
	}
	fmt.Printf("2s 窗口查询返回 %d 行\n", len(rows2s))

	// 查询 5s 窗口
	req5s := &types.QueryRangeRequest{
		Database:              testDB,
		Measurement:           testMeasurement,
		StartTime:             0,
		EndTime:               now,
		DownsampleWindowNanos: 5 * 1e9,
	}
	rows5s, err := queryRows(db, req5s)
	if err != nil {
		return err
	}
	fmt.Printf("5s 窗口查询返回 %d 行\n", len(rows5s))

	if len(rows2s) == 0 || len(rows5s) == 0 {
		return fmt.Errorf("查询降采样数据返回空结果")
	}

	fmt.Println("PASS: 多窗口降采样正确")
	return nil
}

// testDownsampleQuery 验证 downsample_window_nanos=0 和 >0 的区别。
func testDownsampleQuery() error {
	shardDuration := 10 * time.Second
	retention := 5 * time.Second

	db, tmpDir, err := setupDB("query", shardDuration, retention)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dsCfg := downsampleConfig(3, []string{"avg", "max", "min", "sum", "count"})
	if err := db.CreateDatabase(context.Background(), testDB, retention, dsCfg); err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	fmt.Printf("写入 15 个点...\n")
	if err := writeOldPoints(db, 15, 30, 50*time.Millisecond); err != nil {
		return err
	}

	if err := db.FlushAll(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	waitForCompaction(tmpDir)

	db.ForceDownsample()
	time.Sleep(300 * time.Millisecond)

	now := time.Now().UnixNano()

	// 查询原始数据（不设置 downsample_window_nanos）
	reqRaw := &types.QueryRangeRequest{
		Database:    testDB,
		Measurement: testMeasurement,
		StartTime:   0,
		EndTime:     now,
	}
	rawRows, err := queryRows(db, reqRaw)
	if err != nil {
		return err
	}
	fmt.Printf("原始数据查询返回 %d 行\n", len(rawRows))

	// 查询降采样数据
	reqDS := &types.QueryRangeRequest{
		Database:              testDB,
		Measurement:           testMeasurement,
		StartTime:             0,
		EndTime:               now,
		DownsampleWindowNanos: 3 * 1e9,
	}
	dsRows, err := queryRows(db, reqDS)
	if err != nil {
		return err
	}
	fmt.Printf("降采样查询返回 %d 行\n", len(dsRows))

	if len(dsRows) == 0 {
		return fmt.Errorf("降采样查询返回 0 行")
	}

	// 降采样行应包含 avg_/max_/min_ 前缀字段
	dsRow := dsRows[0]
	if dsRow.GetFieldValue("avg_cpu_usage") == nil {
		return fmt.Errorf("降采样查询结果中缺少 avg_cpu_usage 字段")
	}
	if dsRow.GetFieldValue("max_cpu_usage") == nil {
		return fmt.Errorf("降采样查询结果中缺少 max_cpu_usage 字段")
	}

	fmt.Println("PASS: 降采样查询路径正确")
	return nil
}

// testRateFunctions 验证 rate/irate/derivative/diff 计算。
func testRateFunctions() error {
	shardDuration := 5 * time.Second
	retention := 3 * time.Second

	db, tmpDir, err := setupDB("rate", shardDuration, retention)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dsCfg := downsampleConfig(60, []string{"diff", "rate", "irate", "derivative"})
	if err := db.CreateDatabase(context.Background(), testDB, retention, dsCfg); err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	// 写入计数器数据（60 秒前，确保超出 shardDuration + retention）：100, 110, 120, 130, 140
	fmt.Printf("写入 5 个计数器点 (100 → 140)...\n")
	if err := writeCounterPoints(db, 5, 60, 200*time.Millisecond); err != nil {
		return err
	}

	if err := db.FlushAll(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	waitForCompaction(tmpDir)

	db.ForceDownsample()
	time.Sleep(300 * time.Millisecond)

	now := time.Now().UnixNano()
	req := &types.QueryRangeRequest{
		Database:              testDB,
		Measurement:           testMeasurement,
		StartTime:             0,
		EndTime:               now,
		DownsampleWindowNanos: 60 * 1e9,
	}

	rows, err := queryRows(db, req)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return fmt.Errorf("查询返回 0 行")
	}
	row := rows[0]

	// diff: last - first = 140 - 100 = 40
	diffVal := row.GetFieldValue("diff_counter")
	if diffVal == nil {
		return fmt.Errorf("缺少 diff_counter 字段")
	}
	expectedDiff := 40.0
	if diffVal.GetFloatValue() != expectedDiff {
		return fmt.Errorf("diff 不正确: want %.1f, got %.1f", expectedDiff, diffVal.GetFloatValue())
	}
	fmt.Printf("  diff_counter = %.1f ✓\n", diffVal.GetFloatValue())

	// rate: (last - first) / windowSeconds = 40 / 60 ≈ 0.666...
	rateVal := row.GetFieldValue("rate_counter")
	if rateVal == nil {
		return fmt.Errorf("缺少 rate_counter 字段")
	}
	expectedRate := 40.0 / 60.0
	gotRate := rateVal.GetFloatValue()
	if gotRate-expectedRate > 0.001 || expectedRate-gotRate > 0.001 {
		return fmt.Errorf("rate 不正确: want %.4f, got %.4f", expectedRate, gotRate)
	}
	fmt.Printf("  rate_counter = %.4f ✓\n", gotRate)

	// irate: (140 - 130) / 0.2s = 50/s (基于最后两个点)
	irateVal := row.GetFieldValue("irate_counter")
	if irateVal == nil {
		return fmt.Errorf("缺少 irate_counter 字段")
	}
	expectedIrate := 10.0 / 0.2 // 50/sec
	gotIrate := irateVal.GetFloatValue()
	if gotIrate-expectedIrate > 0.001 || expectedIrate-gotIrate > 0.001 {
		return fmt.Errorf("irate 不正确: want %.1f, got %.1f", expectedIrate, gotIrate)
	}
	fmt.Printf("  irate_counter = %.1f ✓\n", gotIrate)

	// derivative: (last - first) / windowSeconds = 40/60
	derivVal := row.GetFieldValue("derivative_counter")
	if derivVal == nil {
		return fmt.Errorf("缺少 derivative_counter 字段")
	}
	expectedDeriv := 40.0 / 60.0
	gotDeriv := derivVal.GetFloatValue()
	if gotDeriv-expectedDeriv > 0.001 || expectedDeriv-gotDeriv > 0.001 {
		return fmt.Errorf("derivative 不正确: want %.4f, got %.4f", expectedDeriv, gotDeriv)
	}
	fmt.Printf("  derivative_counter = %.4f ✓\n", gotDeriv)

	fmt.Println("PASS: 速率函数计算正确")
	return nil
}

// testIdempotency 验证降采样幂等性：重复触发不产生重复文件。
func testIdempotency() error {
	shardDuration := 10 * time.Second
	retention := 5 * time.Second

	db, tmpDir, err := setupDB("idem", shardDuration, retention)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dsCfg := downsampleConfig(5, []string{"avg", "count"})
	if err := db.CreateDatabase(context.Background(), testDB, retention, dsCfg); err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	fmt.Printf("写入 10 个点...\n")
	if err := writeOldPoints(db, 10, 30, 100*time.Millisecond); err != nil {
		return err
	}

	if err := db.FlushAll(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	waitForCompaction(tmpDir)

	// 第一次降采样
	db.ForceDownsample()
	time.Sleep(300 * time.Millisecond)
	count1 := countDownsampleFiles(tmpDir)
	fmt.Printf("第一次降采样后文件数: %d\n", count1)

	// 第二次降采样（应被幂等性阻止）
	db.ForceDownsample()
	time.Sleep(300 * time.Millisecond)
	count2 := countDownsampleFiles(tmpDir)
	fmt.Printf("第二次降采样后文件数: %d\n", count2)

	if count2 != count1 {
		return fmt.Errorf("降采样不幂等: 文件数从 %d 变为 %d", count1, count2)
	}

	fmt.Println("PASS: 降采样幂等性验证通过")
	return nil
}

// testRestartRecovery 验证重启后降采样数据可查询。
func testRestartRecovery() error {
	shardDuration := 10 * time.Second
	retention := 5 * time.Second

	tmpDir := filepath.Join(os.TempDir(), "microts_ds_restart")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	db1, err := microts.Open(&microts.Config{
		DataDir:              tmpDir,
		ShardDurationNanos:   int64(shardDuration),
		MemTableCfg:          microts.DefaultMemTableConfig(),
		CompactionCfg:        microts.DefaultCompactionConfig(),
		RetentionPeriodNanos: int64(retention),
	})
	if err != nil {
		return fmt.Errorf("open db1: %w", err)
	}

	dsCfg := downsampleConfig(5, []string{"avg", "max", "min", "sum", "count"})
	if err := db1.CreateDatabase(context.Background(), testDB, retention, dsCfg); err != nil {
		_ = db1.Close()
		return fmt.Errorf("create database: %w", err)
	}

	fmt.Printf("Session 1: 写入 10 个点...\n")
	if err := writeOldPoints(db1, 10, 30, 100*time.Millisecond); err != nil {
		_ = db1.Close()
		return err
	}

	if err := db1.FlushAll(); err != nil {
		_ = db1.Close()
		return fmt.Errorf("flush: %w", err)
	}
	waitForCompaction(tmpDir)

	db1.ForceDownsample()
	time.Sleep(300 * time.Millisecond)

	count1 := countDownsampleFiles(tmpDir)
	fmt.Printf("Session 1 降采样文件数: %d\n", count1)

	if err := db1.Close(); err != nil {
		return fmt.Errorf("close db1: %w", err)
	}
	fmt.Println("Session 1 已关闭")

	// 重启数据库
	db2, err := microts.Open(&microts.Config{
		DataDir:              tmpDir,
		ShardDurationNanos:   int64(shardDuration),
		MemTableCfg:          microts.DefaultMemTableConfig(),
		CompactionCfg:        microts.DefaultCompactionConfig(),
		RetentionPeriodNanos: int64(retention),
	})
	if err != nil {
		return fmt.Errorf("open db2: %w", err)
	}
	defer func() { _ = db2.Close() }()

	time.Sleep(300 * time.Millisecond)

	// 重启后验证降采样数据可查询
	now := time.Now().UnixNano()
	req := &types.QueryRangeRequest{
		Database:              testDB,
		Measurement:           testMeasurement,
		StartTime:             0,
		EndTime:               now,
		DownsampleWindowNanos: 5 * 1e9,
	}
	rows, err := queryRows(db2, req)
	if err != nil {
		return err
	}
	fmt.Printf("Session 2 降采样查询返回 %d 行\n", len(rows))

	if len(rows) == 0 {
		return fmt.Errorf("重启后降采样查询返回 0 行")
	}

	// 验证字段存在
	row := rows[0]
	if row.GetFieldValue("avg_cpu_usage") == nil ||
		row.GetFieldValue("max_cpu_usage") == nil ||
		row.GetFieldValue("count_cpu_usage") == nil {
		return fmt.Errorf("重启后降采样数据字段不完整")
	}

	fmt.Println("PASS: 重启后降采样数据可查询")
	return nil
}

// ============================================================================
// 辅助函数
// ============================================================================

// queryRows 执行查询并返回所有行。
func queryRows(db *microts.DB, req *types.QueryRangeRequest) ([]*types.PointRow, error) {
	it, err := db.Iterator(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = it.Close() }()

	var rows []*types.PointRow
	for it.Next(context.Background()) {
		row := it.Points()
		if row != nil {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// ============================================================================
// 主函数
// ============================================================================

func main() {
	fmt.Println("========================================")
	fmt.Println("MTS 降采样端到端测试套件")
	fmt.Println("========================================")

	tests := []struct {
		name string
		fn   func() error
	}{
		{"基础降采样", testBasicDownsample},
		{"聚合函数正确性", testAggregationCorrectness},
		{"多窗口降采样", testMultipleWindows},
		{"降采样查询路径", testDownsampleQuery},
		{"速率函数 (diff/rate/irate/derivative)", testRateFunctions},
		{"幂等性", testIdempotency},
		{"重启恢复", testRestartRecovery},
	}

	passed, failed := 0, 0
	for _, tc := range tests {
		fmt.Printf("\n=== 测试: %s ===\n", tc.name)
		if err := tc.fn(); err != nil {
			fmt.Printf("FAIL: %s — %v\n", tc.name, err)
			failed++
		} else {
			fmt.Printf("PASS: %s\n", tc.name)
			passed++
		}
	}

	fmt.Println("\n========================================")
	fmt.Printf("结果: %d 通过 / %d 失败 / %d 总计\n", passed, failed, passed+failed)
	fmt.Println("========================================")

	if failed > 0 {
		os.Exit(1)
	}
	fmt.Println("所有降采样测试通过！")
}
