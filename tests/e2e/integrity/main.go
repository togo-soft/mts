// tests/e2e/integrity/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"micro-ts"
	"micro-ts/types"
	"micro-ts/tests/e2e/pkg/data_gen"
	"micro-ts/tests/e2e/pkg/metrics"
)

func main() {
	tmpDir := filepath.Join(os.TempDir(), "microts_integrity")
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	cfg := microts.Config{
		DataDir:        tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: microts.MemTableConfig{
			MaxSize:      64 * 1024 * 1024,
			MaxCount:     3000,
			IdleDuration: 10 * time.Second,
		},
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

	metrics.GC()
	memBeforeWrite := metrics.ReadMemStats()
	fmt.Printf("Before write: %s\n", metrics.FormatMemStats(memBeforeWrite))
	fmt.Printf("Generating and writing %d points...\n", count)

	timer := metrics.NewTimer()
	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint("db1", "cpu", ts)
		expectedPoints[i] = p
		if err := db.Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			os.Exit(1)
		}
	}
	writeElapsed := timer.Elapsed()

	metrics.GC()
	memAfterWrite := metrics.ReadMemStats()
	writeDelta := metrics.CalcDelta(memBeforeWrite, memAfterWrite)

	fmt.Printf("Write completed in %v, TPS: %.2f\n", writeElapsed, metrics.TPS(count, writeElapsed))
	fmt.Printf("After write: %s\n", metrics.FormatMemStats(memAfterWrite))
	fmt.Printf("Write memory delta: %s\n\n", writeDelta.Format())

	// 等待空闲 flush 触发，确保所有数据已落盘
	fmt.Printf("Waiting 15s for idle flush to trigger...\n")
	time.Sleep(15 * time.Second)

	fmt.Printf("Reading back data...\n")
	metrics.GC()
	memBeforeRead := metrics.ReadMemStats()
	fmt.Printf("Before read: %s\n", metrics.FormatMemStats(memBeforeRead))

	readTimer := metrics.NewTimer()
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
	readElapsed := readTimer.Elapsed()

	metrics.GC()
	memAfterRead := metrics.ReadMemStats()
	readDelta := metrics.CalcDelta(memBeforeRead, memAfterRead)

	fmt.Printf("Read completed in %v, TPS: %.2f\n", readElapsed, metrics.TPS(len(resp.Rows), readElapsed))
	fmt.Printf("After read: %s\n", metrics.FormatMemStats(memAfterRead))
	fmt.Printf("Read memory delta: %s\n\n", readDelta.Format())

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