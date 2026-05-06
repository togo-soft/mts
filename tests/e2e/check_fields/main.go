package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/data_gen"
)

func main() {
	gen := data_gen.NewDataGenerator(42)
	baseTime := time.Now().UnixNano()
	
	p := gen.GeneratePoint("db1", "cpu", baseTime)
	
	fmt.Printf("Point Fields:\n")
	for name, val := range p.Fields {
		if val == nil {
			fmt.Printf("  %s: nil\n", name)
		} else {
			fmt.Printf("  %s: %v (type: %T)\n", name, val.GetValue(), val.GetValue())
		}
	}
	
	// 现在测试写入并查看 schema
	tmpDir := filepath.Join(os.TempDir(), "microts_fields_test")
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

	db, _ := microts.Open(cfg)
	db.Write(context.Background(), p)
	time.Sleep(6 * time.Second)
	db.Close()
	
	// 读取 schema
	dataDir := filepath.Join(tmpDir, "db1", "cpu")
	filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && info.Name() == "_schema.json" {
			fmt.Printf("\nSchema: %s\n", path)
			content, _ := os.ReadFile(path)
			fmt.Printf("Content: %s\n", string(content))
		}
		return nil
	})
}
