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
	tmpDir := filepath.Join(os.TempDir(), "microts_schema_test")
	os.RemoveAll(tmpDir)

	cfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: time.Hour,
		MemTableCfg: microts.MemTableConfig{
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

	baseTime := time.Now().UnixNano()

	// 写入一些数据
	p := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   baseTime,
		Fields: map[string]*types.FieldValue{
			"usage": types.NewFieldValue(float64(1.5)),
			"count": types.NewFieldValue(int64(10)),
		},
	}
	db.Write(context.Background(), p)

	// 等待 flush
	time.Sleep(6 * time.Second)
	db.Close()

	// 找 schema 文件并读取
	dataDir := filepath.Join(tmpDir, "db1", "cpu")
	filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && info.Name() == "_schema.json" {
			fmt.Printf("Schema file: %s\n", path)
			content, _ := os.ReadFile(path)
			fmt.Printf("Content: %s\n", string(content))
		}
		return nil
	})
}
