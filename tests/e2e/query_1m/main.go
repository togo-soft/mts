// tests/e2e/query_1m/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"micro-ts"
	"micro-ts/internal/types"
	"micro-ts/tests/e2e/pkg/data_gen"
)

func main() {
	tmpDir := filepath.Join(os.TempDir(), "microts_query_1m")
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	cfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
	}

	db, err := microts.Open(cfg)
	if err != nil {
		fmt.Printf("Open failed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// 先写入 1M 数据
	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()
	const count = 1000000

	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint("db1", "cpu", ts)
		if err := db.Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			os.Exit(1)
		}
	}

	// 查询
	start := time.Now()
	resp, err := db.QueryRange(context.Background(), &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   baseTime,
		EndTime:     baseTime + int64(count)*int64(time.Second),
	})
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		os.Exit(1)
	}
	elapsed := time.Since(start)

	fmt.Printf("Query 1M: %d rows in %v\n", len(resp.Rows), elapsed)
}
