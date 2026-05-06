// tests/e2e/simple_integrity/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/types"
)

func main() {
	tmpDir := filepath.Join(os.TempDir(), "microts_simple_test")
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	cfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			MaxSize:           64 * 1024 * 1024,
			MaxCount:          3000,
			IdleDurationNanos: int64(5 * time.Second),
		},
	}

	db, err := microts.Open(cfg)
	if err != nil {
		fmt.Printf("Open failed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	baseTime := time.Now().UnixNano()
	count := 100

	fmt.Printf("Writing %d points...\n", count)
	for i := 0; i < count; i++ {
		p := &types.Point{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   baseTime + int64(i)*int64(time.Second),
			Fields: map[string]*types.FieldValue{
				"usage": types.NewFieldValue(float64(i) * 1.5),
				"count": types.NewFieldValue(int64(i * 10)),
			},
		}
		if err := db.Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			os.Exit(1)
		}
	}

	fmt.Printf("Waiting for idle flush...\n")
	time.Sleep(6 * time.Second)

	fmt.Printf("Querying (same session)...\n")
	resp, err := db.QueryRange(context.Background(), &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   baseTime,
		EndTime:     baseTime + int64(count)*int64(time.Second),
		Offset:      0,
		Limit:       0,
	})
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Got %d rows, expected %d\n", len(resp.Rows), count)

	// 验证数据
	errors := 0
	for i, row := range resp.Rows {
		expectedUsage := float64(i) * 1.5
		expectedCount := int64(i * 10)
		usage := row.Fields["usage"]
		countVal := row.Fields["count"]
		if usage == nil || countVal == nil {
			fmt.Printf("Row %d: nil fields!\n", i)
			errors++
			continue
		}
		if usage.GetFloatValue() != expectedUsage {
			fmt.Printf("Row %d: usage mismatch: expected %v, got %v\n", i, expectedUsage, usage.GetFloatValue())
			errors++
		}
		if countVal.GetIntValue() != expectedCount {
			fmt.Printf("Row %d: count mismatch: expected %v, got %v\n", i, expectedCount, countVal.GetIntValue())
			errors++
		}
	}
	if errors == 0 {
		fmt.Printf("SUCCESS: All data verified!\n")
	} else {
		fmt.Printf("FAIL: %d errors\n", errors)
	}
}
