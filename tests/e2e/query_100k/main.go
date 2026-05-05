// tests/e2e/query_100k/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/data_gen"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
	"codeberg.org/micro-ts/mts/types"
)

func main() {
	tmpDir := filepath.Join(os.TempDir(), "microts_query_100k")
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	cfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: microts.MemTableConfig{
			MaxSize:           64 * 1024 * 1024,
			MaxCount:          3000,
			IdleDurationNanos: int64(10 * time.Second),
		},
	}

	db, err := microts.Open(cfg)
	if err != nil {
		fmt.Printf("Open failed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// 先写入 100K 数据
	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()
	const count = 100000

	metrics.GC()
	memBeforeWrite := metrics.ReadMemStats()

	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint("db1", "cpu", ts)
		if err := db.Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			os.Exit(1)
		}
	}

	metrics.GC()
	memAfterWrite := metrics.ReadMemStats()
	writeDelta := metrics.CalcDelta(memBeforeWrite, memAfterWrite)

	fmt.Printf("Write 100K: %d points\n", count)
	fmt.Printf("After write: %s, Δ: %s\n\n", metrics.FormatMemStats(memAfterWrite), writeDelta.Format())

	// 等待 15 秒确保 idle flush 触发
	fmt.Printf("Waiting 15s for idle flush to trigger...\n")
	time.Sleep(15 * time.Second)

	// 查询（使用分页策略：每次查询 2000 条）
	metrics.GC()
	memBeforeQuery := metrics.ReadMemStats()
	fmt.Printf("Before query: %s\n", metrics.FormatMemStats(memBeforeQuery))

	const queryLimit = 2000
	timer := metrics.NewTimer()
	resp, err := db.QueryRange(context.Background(), &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   baseTime,
		EndTime:     baseTime + int64(count)*int64(time.Second),
		Offset:      0,
		Limit:       queryLimit,
	})
	elapsed := timer.Elapsed()

	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		os.Exit(1)
	}

	metrics.GC()
	memAfterQuery := metrics.ReadMemStats()
	queryDelta := metrics.CalcDelta(memBeforeQuery, memAfterQuery)

	fmt.Printf("Query 100K (paginated): %d rows in %v, TPS: %.2f (limit=%d)\n", len(resp.Rows), elapsed, metrics.TPS(len(resp.Rows), elapsed), queryLimit)
	fmt.Printf("After query: %s\n", metrics.FormatMemStats(memAfterQuery))
	fmt.Printf("Query memory delta: %s\n", queryDelta.Format())
}
