// tests/e2e/integrity/main.go
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
	tmpDir := filepath.Join(os.TempDir(), "microts_integrity")
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	cfg := microts.Config{
		DataDir:        tmpDir,
		ShardDuration: time.Hour,
	}

	db, err := microts.Open(cfg)
	if err != nil {
		fmt.Printf("Open failed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()
	const count = 100000
	expectedPoints := make([]*types.Point, count)

	fmt.Printf("Generating and writing %d points...\n", count)
	writeStart := time.Now()

	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint("db1", "cpu", ts)
		expectedPoints[i] = p
		if err := db.Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			os.Exit(1)
		}
	}

	fmt.Printf("Write completed in %v\n", time.Since(writeStart))

	fmt.Printf("Reading back data...\n")
	readStart := time.Now()
	resp, err := db.QueryRange(context.Background(), &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   baseTime,
		EndTime:     baseTime + int64(count)*int64(time.Second),
		Limit:       0,
	})
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Read completed in %v\n", time.Since(readStart))

	// 验证数量
	if int(resp.TotalCount) != count {
		fmt.Printf("FAIL: expected %d points, got %d\n", count, resp.TotalCount)
		os.Exit(1)
	}

	// 逐字段验证
	fmt.Printf("Verifying data integrity...\n")
	errorCount := 0
	for i, row := range resp.Rows {
		expected := expectedPoints[i]

		// 验证 timestamp
		if row.Timestamp != expected.Timestamp {
			fmt.Printf("row %d: timestamp mismatch\n", i)
			errorCount++
			if errorCount > 10 {
				break
			}
		}

		// 验证 fields
		for name, expectedVal := range expected.Fields {
			actualVal, ok := row.Fields[name]
			if !ok {
				fmt.Printf("row %d: missing field %s\n", i, name)
				errorCount++
				if errorCount > 10 {
					break
				}
				continue
			}
			if actualVal != expectedVal {
				fmt.Printf("row %d: field %s mismatch\n", i, name)
				errorCount++
				if errorCount > 10 {
					break
				}
			}
		}
		if errorCount > 10 {
			break
		}
	}

	if errorCount > 0 {
		fmt.Printf("FAIL: %d errors found\n", errorCount)
		os.Exit(1)
	}

	fmt.Printf("PASS: Data integrity verified successfully (%d points)\n", count)
}