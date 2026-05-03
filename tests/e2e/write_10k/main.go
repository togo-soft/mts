// tests/e2e/write_10k/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"micro-ts"
	"micro-ts/tests/e2e/pkg/data_gen"
)

func main() {
	tmpDir := filepath.Join(os.TempDir(), "microts_write_10k")
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
	const count = 10000

	start := time.Now()
	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint("db1", "cpu", ts)
		if err := db.Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			os.Exit(1)
		}
	}
	elapsed := time.Since(start)
	tps := float64(count) / elapsed.Seconds()

	fmt.Printf("Write 10K: %d points in %v, TPS: %.2f\n", count, elapsed, tps)
}