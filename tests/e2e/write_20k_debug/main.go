// tests/e2e/write_20k_debug/main.go
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
)

func main() {
	tmpDir := filepath.Join(os.TempDir(), "microts_debug")
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

	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()
	const count = 20000

	fmt.Printf("Writing %d points...\n", count)
	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint("db1", "cpu", ts)
		if err := db.Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			os.Exit(1)
		}
	}
	fmt.Printf("Write completed.\n")

	// 等待数据落盘后统计存储
	fmt.Printf("Waiting for data flush...\n")
	time.Sleep(2 * time.Second)

	dataDir := filepath.Join(tmpDir, "db1", "cpu")
	fmt.Printf("\n%s\n", metrics.FormatStorageReport(dataDir, count, 80))
}
