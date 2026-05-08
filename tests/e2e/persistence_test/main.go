// tests/e2e/persistence_test/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"

	microts "codeberg.org/micro-ts/mts"
	"codeberg.org/micro-ts/mts/types"
)

func main() {
	tmpDir := filepath.Join(os.TempDir(), "microts_persistence_test")
	_ = os.RemoveAll(tmpDir)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	shardDuration := 500 * time.Millisecond
	dbCfg := microts.Config{
		DataDir:       tmpDir,
		ShardDuration: shardDuration,
	}

	tags := map[string]string{"host": "server1", "region": "us-east"}

	// 第一次打开数据库，写入数据
	fmt.Printf("=== First session ===\n")
	db1, err := microts.Open(dbCfg)
	if err != nil {
		fmt.Printf("Open db1 failed: %v\n", err)
		os.Exit(1)
	}

	// 写入 2 个点，都在同一个 shard
	baseTime := time.Now().Add(-1 * time.Second)
	for i := 0; i < 2; i++ {
		p := &types.Point{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        tags,
			Timestamp:   baseTime.Add(time.Duration(i) * 100 * time.Millisecond).UnixNano(),
			Fields: map[string]*types.FieldValue{
				"usage": types.NewFieldValue(float64(80.0 + float64(i))),
			},
		}
		if err := db1.Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed: %v\n", err)
			_ = db1.Close()
			os.Exit(1)
		}
	}
	fmt.Printf("Wrote 2 points with same tags in same shard\n")

	// 等待数据刷盘
	time.Sleep(300 * time.Millisecond)

	// 关闭数据库
	if err := db1.Close(); err != nil {
		fmt.Printf("Close db1 failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("db1 closed\n")

	// 验证 metadata.db 存在
	metaDBPath := filepath.Join(tmpDir, "metadata.db")
	if _, err := os.Stat(metaDBPath); os.IsNotExist(err) {
		fmt.Printf("FAIL: metadata.db not found after close\n")
		os.Exit(1)
	}
	fmt.Printf("metadata.db exists: %s\n", metaDBPath)

	// 用 bbolt 读取 metadata.db，验证 series 数据已持久化
	bdb, err := bolt.Open(metaDBPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		fmt.Printf("FAIL: open metadata.db: %v\n", err)
		os.Exit(1)
	}
	seriesCount := 0
	_ = bdb.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte("db1"))
		if dbBucket == nil {
			fmt.Printf("FAIL: db1 bucket not found\n")
			os.Exit(1)
		}
		measBucket := dbBucket.Bucket([]byte("cpu"))
		if measBucket == nil {
			fmt.Printf("FAIL: cpu bucket not found\n")
			os.Exit(1)
		}
		seriesBucket := measBucket.Bucket([]byte("series"))
		if seriesBucket == nil {
			fmt.Printf("FAIL: series bucket not found\n")
			os.Exit(1)
		}
		c := seriesBucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if string(k) == "_next_sid" {
				continue
			}
			seriesCount++
		}
		return nil
	})
	_ = bdb.Close()
	fmt.Printf("Series count in metadata.db: %d\n", seriesCount)
	if seriesCount == 0 {
		fmt.Printf("FAIL: no series persisted in metadata.db\n")
		os.Exit(1)
	}

	// 检查数据目录（shard 数据，非元数据）
	dataDir := filepath.Join(tmpDir, "db1", "cpu")
	entries, _ := os.ReadDir(dataDir)
	fmt.Printf("Data directory:\n")
	for _, e := range entries {
		fmt.Printf("  %s\n", e.Name())
	}

	// 第二次打开数据库
	fmt.Printf("\n=== Second session ===\n")

	func() {
		db2, err := microts.Open(dbCfg)
		if err != nil {
			fmt.Printf("Open db2 failed: %v\n", err)
			os.Exit(1)
		}
		defer func() { _ = db2.Close() }()

		// 写入相同 tags 的新数据（同一个 shard）
		newTimestamp := baseTime.Add(200 * time.Millisecond).UnixNano()
		fmt.Printf("Writing new point with same tags at timestamp %d...\n", newTimestamp)
		p3 := &types.Point{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        tags,
			Timestamp:   newTimestamp,
			Fields: map[string]*types.FieldValue{
				"usage": types.NewFieldValue(float64(95.0)),
			},
		}

		if err := db2.Write(context.Background(), p3); err != nil {
			fmt.Printf("Write failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Write succeeded\n")

		// 写入不同 tags 的数据
		p4 := &types.Point{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server2", "region": "us-west"},
			Timestamp:   time.Now().UnixNano(),
			Fields: map[string]*types.FieldValue{
				"usage": types.NewFieldValue(float64(70.0)),
			},
		}

		fmt.Printf("Writing point with different tags...\n")
		if err := db2.Write(context.Background(), p4); err != nil {
			fmt.Printf("Write failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Write succeeded\n")
	}()

	// 验证 metadata.db 仍然存在
	if _, err := os.Stat(metaDBPath); os.IsNotExist(err) {
		fmt.Printf("FAIL: metadata.db was deleted\n")
		os.Exit(1)
	}
	fmt.Printf("metadata.db still exists\n")

	// 再次读取 metadata.db 验证 series 数量增加了（db2 已关闭，bolt 锁已释放）
	bdb2, err := bolt.Open(metaDBPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		fmt.Printf("FAIL: open metadata.db second time: %v\n", err)
		os.Exit(1)
	}
	seriesCount2 := 0
	_ = bdb2.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte("db1"))
		if dbBucket == nil {
			os.Exit(1)
		}
		measBucket := dbBucket.Bucket([]byte("cpu"))
		if measBucket == nil {
			os.Exit(1)
		}
		seriesBucket := measBucket.Bucket([]byte("series"))
		if seriesBucket == nil {
			os.Exit(1)
		}
		c := seriesBucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if string(k) == "_next_sid" {
				continue
			}
			seriesCount2++
		}
		return nil
	})
	_ = bdb2.Close()
	fmt.Printf("Series count in metadata.db after second session: %d\n", seriesCount2)
	if seriesCount2 < 2 {
		fmt.Printf("FAIL: expected at least 2 series (different tags), got %d\n", seriesCount2)
		os.Exit(1)
	}

	fmt.Printf("\nSUCCESS: Metadata persistence is working!\n")
	fmt.Printf("  - metadata.db correctly persisted after first session\n")
	fmt.Printf("  - Database reopened successfully in second session\n")
	fmt.Printf("  - Same tags and different tags both accepted writes\n")
	fmt.Printf("  - Series count verified in metadata.db: %d → %d\n", seriesCount, seriesCount2)
}
