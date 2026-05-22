// tests/e2e/restart_recovery/main.go
//
// # 重启恢复后数据累积测试
//
// 测试场景：
//   - 连续 10 次创建 DB → 写入 100 条数据 → 关闭 DB
//   - 第 11 次创建 DB 后查询，验证累积数据达到 1000 条
//
// 验证目标：
//   - 每次重启后 SSTable 序列号正确恢复，不会覆盖已有数据
//   - Wal Purge 后数据已在 SSTable 中安全持久化
//   - 边界条件：MaxCount=100，写入 100 条刚好触发刷盘
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
	"codeberg.org/micro-ts/mts/types"
)

const (
	cycles         = 10
	pointsPerCycle = 100
	expectedTotal  = cycles * pointsPerCycle
)

func main() {
	fmt.Println("========================================")
	fmt.Println("MTS 重启恢复数据累积测试")
	fmt.Println("========================================")
	fmt.Printf("循环次数: %d, 每次写入: %d 条, 期望总计: %d 条\n\n", cycles, pointsPerCycle, expectedTotal)

	tmpDir := filepath.Join(os.TempDir(), "microts_restart_recovery_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbName := "testdb"
	measurement := "cpu"

	dbCfg := mts.Config{
		DataDir:            tmpDir,
		ShardDurationNanos: int64(time.Hour),
		MemTableCfg: &mts.MemTableConfig{
			FlushMemorySize: 64 * 1024 * 1024,
			FlushPointCount: pointsPerCycle, // 边界：等于写入数量，每次都会触发刷盘
			FlushIdleNanos:  int64(5 * time.Second),
		},
		CompactionCfg: &mts.CompactionConfig{
			MaxSstableCount:    4,
			MaxCompactionBatch: 0,
			ShardSizeLimit:     1 * 1024 * 1024 * 1024,
			CheckIntervalNanos: int64(10 * time.Second),
			TimeoutNanos:       int64(30 * time.Second),
		},
	}

	var baseTime int64

	for cycle := 1; cycle <= cycles; cycle++ {
		fmt.Printf("第 %d 次: 打开 → 写入 %d 条 → 关闭\n", cycle, pointsPerCycle)

		db, err := mts.Open(&dbCfg)
		if err != nil {
			fmt.Printf("FATAL: 第 %d 次打开失败: %v\n", cycle, err)
			os.Exit(1)
		}

		if cycle == 1 {
			baseTime = time.Now().UnixNano()
		}

		startTs := baseTime + int64((cycle-1)*pointsPerCycle)*int64(time.Millisecond)
		writeTimer := metrics.NewWriteSummary(pointsPerCycle)
		for i := 0; i < pointsPerCycle; i++ {
			p := &types.Point{
				Database:    dbName,
				Measurement: measurement,
				Tags: map[string]string{
					"cycle": fmt.Sprintf("%d", cycle),
				},
				Timestamp: startTs + int64(i)*int64(time.Millisecond),
				Fields: map[string]*types.FieldValue{
					"value": types.NewFieldValue(float64(cycle*pointsPerCycle + i)),
				},
			}
			if err := db.Write(context.Background(), p); err != nil {
				fmt.Printf("FATAL: 第 %d 次写入失败 (i=%d): %v\n", cycle, i, err)
				_ = db.Close()
				os.Exit(1)
			}
		}
		writeTimer.Finish()
		fmt.Printf("  %s\n", writeTimer.Format())

		if err := db.Close(); err != nil {
			fmt.Printf("FATAL: 第 %d 次关闭失败: %v\n", cycle, err)
			os.Exit(1)
		}
	}

	fmt.Printf("\n验证: 第 %d 次打开 → 仅查询\n", cycles+1)
	db, err := mts.Open(&dbCfg)
	if err != nil {
		fmt.Printf("FATAL: 验证打开失败: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	time.Sleep(500 * time.Millisecond)

	it, err := db.Iterator(context.Background(), &types.QueryRangeRequest{
		Database:    dbName,
		Measurement: measurement,
		StartTime:   baseTime,
		EndTime:     baseTime + int64(cycles*pointsPerCycle)*int64(time.Millisecond) + int64(time.Hour),
		Offset:      0,
		Limit:       0,
	})
	if err != nil {
		fmt.Printf("FATAL: 查询失败: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = it.Close() }()
	var rows []*types.PointRow
	for it.Next(context.Background()) {
		rows = append(rows, it.Points())
	}

	got := len(rows)
	if got == expectedTotal {
		fmt.Printf("\n✅ 通过: 累计 %d 条数据，完整无误\n", got)
	} else {
		fmt.Printf("\n❌ 失败: 期望 %d 条，实际 %d 条（差异 %d 条）\n", expectedTotal, got, expectedTotal-got)
		os.Exit(1)
	}

	fmt.Println("\n所有测试通过！数据累积验证完成。")
}
