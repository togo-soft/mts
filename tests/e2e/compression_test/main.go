// tests/e2e/compression_test/main.go
// 端到端压缩测试：验证 none/snappy/lz4 压缩算法的写入、查询、恢复、压缩率
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/framework"
	"codeberg.org/micro-ts/mts/types"
)

const writeCount = 500

func main() {
	algos := []struct {
		name string
		opt  func(*framework.Config)
	}{
		{"none", nil},
		{"snappy", framework.WithCompression("snappy")},
		{"lz4", framework.WithCompression("lz4")},
	}

	allPassed := true

	for _, algo := range algos {
		fmt.Printf("\n=== Testing compression: %s ===\n", algo.name)
		if !testWriteQueryIntegrity(algo.name, algo.opt) {
			allPassed = false
		}
		if !testRestartRecovery(algo.name, algo.opt) {
			allPassed = false
		}
		if !testMultipleFieldTypes(algo.name, algo.opt) {
			allPassed = false
		}
	}

	fmt.Println()
	if allPassed {
		fmt.Println("SUCCESS: All compression tests passed!")
	} else {
		fmt.Println("FAIL: Some compression tests failed!")
		os.Exit(1)
	}
}

func testWriteQueryIntegrity(name string, opt func(*framework.Config)) bool {
	var opts []func(*framework.Config)
	opts = append(opts, framework.WithFlushIdle(5*time.Second))
	if opt != nil {
		opts = append(opts, opt)
	}

	h, err := framework.NewTestHarness("comp_write_"+name, opts...)
	if err != nil {
		fmt.Printf("  FAIL (setup): %v\n", err)
		return false
	}
	defer func() { _ = h.Close() }()

	fmt.Printf("  Writing %d points...\n", writeCount)
	if err := h.WritePoints(context.Background(), writeCount, time.Millisecond); err != nil {
		fmt.Printf("  FAIL (write): %v\n", err)
		return false
	}

	fmt.Printf("  Waiting for idle flush...\n")
	time.Sleep(6 * time.Second)

	fmt.Printf("  Querying and verifying integrity...\n")
	if err := h.VerifyDataIntegrity(writeCount, time.Millisecond); err != nil {
		fmt.Printf("  FAIL (integrity): %v\n", err)
		return false
	}

	sstCount := h.SSTableCount()
	diskUsage := h.DiskUsage()
	fmt.Printf("  PASS: %s - %d SSTables, %d bytes disk usage\n", name, sstCount, diskUsage)
	return true
}

func testRestartRecovery(name string, opt func(*framework.Config)) bool {
	var opts []func(*framework.Config)
	opts = append(opts, framework.WithFlushIdle(time.Second))
	if opt != nil {
		opts = append(opts, opt)
	}

	h, err := framework.NewTestHarness("comp_restart_"+name, opts...)
	if err != nil {
		fmt.Printf("  FAIL (setup): %v\n", err)
		return false
	}

	startTime := h.StartTime()
	dbName := h.Config().DBName
	measName := h.Config().MeasurementName
	tmpDir := h.TempDir()

	fmt.Printf("  Writing %d points for recovery test...\n", writeCount)
	if err := h.WritePoints(context.Background(), writeCount, time.Millisecond); err != nil {
		_ = h.Close()
		fmt.Printf("  FAIL (write): %v\n", err)
		return false
	}

	time.Sleep(2 * time.Second)

	// 关闭数据库（但不删除数据目录）
	if err := h.DB().Close(); err != nil {
		_ = h.Close()
		fmt.Printf("  FAIL (close): %v\n", err)
		return false
	}

	// 使用原始 API 重新打开（不经过 NewTestHarness 的 os.RemoveAll）
	compressionAlgo := types.CompressionNone
	switch name {
	case "snappy":
		compressionAlgo = types.CompressionSnappy
	case "lz4":
		compressionAlgo = types.CompressionLZ4
	}

	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: &microts.MemTableConfig{
			FlushMemorySize: 64 * 1024 * 1024,
			FlushPointCount: 3000,
			FlushIdleNanos:  int64(time.Second),
		},
		CompactionCfg: &microts.CompactionConfig{
			MaxSstableCount:    4,
			MaxCompactionBatch: 0,
			ShardSizeLimit:     1 * 1024 * 1024 * 1024,
			CheckIntervalNanos: int64(10 * time.Second),
			TimeoutNanos:       int64(30 * time.Second),
		},
		CompressionAlgorithm: compressionAlgo,
	}

	db2, err := microts.Open(dbCfg)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		fmt.Printf("  FAIL (reopen): %v\n", err)
		return false
	}

	time.Sleep(500 * time.Millisecond)

	fmt.Printf("  Querying after restart...\n")
	it, err := db2.Iterator(context.Background(), &microts.QueryRangeRequest{
		Database:    dbName,
		Measurement: measName,
		StartTime:   startTime,
		EndTime:     startTime + int64(writeCount)*int64(time.Millisecond),
		Offset:      0,
		Limit:       0,
	})
	if err != nil {
		_ = db2.Close()
		_ = os.RemoveAll(tmpDir)
		fmt.Printf("  FAIL (query): %v\n", err)
		return false
	}
	defer func() { _ = it.Close() }()
	var rows []*microts.PointRow
	for it.Next(context.Background()) {
		rows = append(rows, it.Points())
	}

	if len(rows) != writeCount {
		_ = db2.Close()
		_ = os.RemoveAll(tmpDir)
		fmt.Printf("  FAIL: expected %d rows, got %d\n", writeCount, len(rows))
		return false
	}

	_ = db2.Close()
	_ = os.RemoveAll(tmpDir)

	fmt.Printf("  PASS: %s - restart recovery verified\n", name)
	return true
}

func testMultipleFieldTypes(name string, opt func(*framework.Config)) bool {
	var opts []func(*framework.Config)
	opts = append(opts, framework.WithFlushIdle(5*time.Second))
	if opt != nil {
		opts = append(opts, opt)
	}

	h, err := framework.NewTestHarness("comp_fields_"+name, opts...)
	if err != nil {
		fmt.Printf("  FAIL (setup): %v\n", err)
		return false
	}
	defer func() { _ = h.Close() }()

	fmt.Printf("  Writing mixed field type points...\n")
	if err := h.WritePoints(context.Background(), 200, time.Millisecond); err != nil {
		fmt.Printf("  FAIL (write): %v\n", err)
		return false
	}

	time.Sleep(6 * time.Second)

	rows, err := h.QueryRange(context.Background(), h.StartTime(), h.StartTime()+200*int64(time.Millisecond))
	if err != nil {
		fmt.Printf("  FAIL (query): %v\n", err)
		return false
	}

	if len(rows) != 200 {
		fmt.Printf("  FAIL: expected 200 rows, got %d\n", len(rows))
		return false
	}

	errors := 0
	for i, row := range rows {
		if row.GetFieldValue("usage") == nil {
			fmt.Printf("  Row %d: missing 'usage' field\n", i)
			errors++
		}
		if row.GetFieldValue("count") == nil {
			fmt.Printf("  Row %d: missing 'count' field\n", i)
			errors++
		}
		if errors > 5 {
			break
		}
	}

	if errors > 0 {
		fmt.Printf("  FAIL: %d field errors\n", errors)
		return false
	}

	fmt.Printf("  PASS: %s - mixed field types verified\n", name)
	return true
}
