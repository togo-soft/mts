// examples/simple/main.go
//
// # Simple Write and Query Example
//
// 本示例展示如何使用 microts 写入和查询数据。
//
// 配置说明：
//
//   - MemTableCfg.MaxCount = 100：MemTable 最多存储 100 条数据，超出后触发刷盘
//   - MemTableCfg.IdleDurationNanos = 5 秒：空闲 5 秒后触发刷盘
//
// 数据流程：
//
//	写入 → WAL → MemTable → (触发刷盘) → SSTable
//
// 运行方式：
//
//	cd examples/simple && go run main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/types"
)

func main() {
	// 创建临时数据目录
	tmpDir := filepath.Join(os.TempDir(), "microts_simple_example")
	// 使用 CLEANUP=1 环境变量清除旧数据（只在启动时清除）
	if os.Getenv("CLEANUP") == "1" {
		_ = os.RemoveAll(tmpDir)
	}

	// 数据库配置
	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           64 * 1024 * 1024,
			MaxCount:          100,
			IdleDurationNanos: int64(5 * time.Second),
		},
	}

	db, err := microts.Open(dbCfg)
	if err != nil {
		log.Fatalf("打开数据库失败: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("关闭数据库失败: %v", err)
		}
	}()

	fmt.Println("=== MicroTS 简单读写示例 ===")
	fmt.Printf("数据目录: %s\n\n", tmpDir)

	// ============ 写入数据 ============
	fmt.Println("Step 1: 写入 100 条数据")
	dbName := "testdb"
	measurement := "cpu"
	baseTime := time.Now().UnixNano()

	for i := 0; i < 100; i++ {
		p := &types.Point{
			Database:    dbName,
			Measurement: measurement,
			Tags: map[string]string{
				"host": fmt.Sprintf("server%d", i%3+1),
			},
			Timestamp: baseTime + int64(i)*int64(time.Millisecond),
			Fields: map[string]*types.FieldValue{
				"usage": types.NewFieldValue(float64(50.0 + float64(i%50))),
				"count": types.NewFieldValue(int64(i * 10)),
			},
		}
		if err := db.Write(context.Background(), p); err != nil {
			log.Fatalf("写入数据点 %d 失败: %v", i, err)
		}
	}
	fmt.Printf("写入完成，当前会话时间范围: [%d, %d]\n", baseTime, baseTime+100*int64(time.Millisecond))

	// ============ 查询数据 ============
	// 查询时间范围：所有累积数据（从 0 到未来一个月）
	oneMonthLater := time.Now().Add(30 * 24 * time.Hour).UnixNano()
	fmt.Println("\nStep 2: 查询所有累积数据（从 0 到未来一个月）")
	it, err := db.Iterator(context.Background(), &types.QueryRangeRequest{
		Database:    dbName,
		Measurement: measurement,
		StartTime:   0,
		EndTime:     oneMonthLater,
		Offset:      0,
		Limit:       0,
	})
	if err != nil {
		log.Fatalf("查询失败: %v", err)
	}
	defer func() { _ = it.Close() }()

	var rows []*types.PointRow
	for it.Next(context.Background()) {
		rows = append(rows, it.Points())
	}

	fmt.Printf("查询结果: %d 条数据（时间范围 [0, %d]）\n\n", len(rows), oneMonthLater)

	// 打印前几条数据
	fmt.Println("前 5 条数据:")
	for i := 0; i < 5 && i < len(rows); i++ {
		row := rows[i]
		fmt.Printf("  [%d] host=%s usage=%.1f\n", row.Timestamp, row.Tags["host"], row.GetFieldValue("usage").GetFloatValue())
	}

	fmt.Println("\n=== 示例完成 ===")
}
