// tests/e2e/query_1k/main.go
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
	tmpDir := filepath.Join(os.TempDir(), "microts_query_1k")
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	cfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
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

	// 先写入 1K 数据
	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()
	const count = 1000

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

	fmt.Printf("Write 1K: %d points\n", count)
	fmt.Printf("After write: %s, Δ: %s\n\n", metrics.FormatMemStats(memAfterWrite), writeDelta.Format())

	// 等待 15 秒确保 idle flush 触发
	fmt.Printf("Waiting 15s for idle flush to trigger...\n")
	time.Sleep(15 * time.Second)

	// 查询
	metrics.GC()
	memBeforeQuery := metrics.ReadMemStats()
	fmt.Printf("Before query: %s\n", metrics.FormatMemStats(memBeforeQuery))

	timer := metrics.NewTimer()
	resp, err := db.QueryRange(context.Background(), &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   baseTime,
		EndTime:     baseTime + int64(count)*int64(time.Second),
	})
	elapsed := timer.Elapsed()

	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		os.Exit(1)
	}

	metrics.GC()
	memAfterQuery := metrics.ReadMemStats()
	queryDelta := metrics.CalcDelta(memBeforeQuery, memAfterQuery)

	fmt.Printf("Query 1K: %d rows in %v, TPS: %.2f\n", len(resp.Rows), elapsed, metrics.TPS(len(resp.Rows), elapsed))
	fmt.Printf("After query: %s\n", metrics.FormatMemStats(memAfterQuery))
	fmt.Printf("Query memory delta: %s\n", queryDelta.Format())
}
