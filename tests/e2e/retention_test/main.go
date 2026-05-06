// tests/e2e/retention_test/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/micro-ts/mts/tests/e2e/pkg/framework"
	"codeberg.org/micro-ts/mts/types"
)

func main() {
	// 使用短 shard duration 和 retention period 便于测试
	shardDuration := 500 * time.Millisecond
	retentionPeriod := 2 * time.Second
	checkInterval := 200 * time.Millisecond

	h, err := framework.NewTestHarness("retention_test",
		framework.WithIdleDuration(100*time.Millisecond),
		framework.WithRetentionPeriod(retentionPeriod),
		framework.WithRetentionCheckInterval(checkInterval),
		framework.WithShardDuration(shardDuration), // 在创建时设置
	)
	if err != nil {
		fmt.Printf("Setup failed: %v\n", err)
		return
	}
	defer func() { _ = h.Close() }()

	// 写入一个数据点，使用较老的时间戳
	oldTimestamp := time.Now().Add(-10 * time.Second).UnixNano()
	p := &types.Point{
		Database:    h.Config().DBName,
		Measurement: h.Config().MeasurementName,
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   oldTimestamp,
		Fields: map[string]*types.FieldValue{
			"usage": types.NewFieldValue(float64(50.5)),
		},
	}

	fmt.Printf("Writing point with old timestamp: %d\n", oldTimestamp)
	fmt.Printf("Current time: %d\n", time.Now().UnixNano())
	fmt.Printf("Shard duration: %v\n", h.Config().ShardDuration)
	fmt.Printf("Retention period: %v\n", retentionPeriod)

	if err := h.DB().Write(context.Background(), p); err != nil {
		fmt.Printf("Write failed: %v\n", err)
		return
	}

	// 等待数据刷盘到 SSTable
	fmt.Printf("\nWaiting for flush to SSTable...\n")
	time.Sleep(500 * time.Millisecond)

	// 检查数据目录
	dataDir := h.DataDir()
	fmt.Printf("Data directory: %s\n", dataDir)
	entries, _ := os.ReadDir(dataDir)
	for _, e := range entries {
		fmt.Printf("  Entry: %s (isDir=%v)\n", e.Name(), e.IsDir())
		if e.IsDir() {
			subDir := filepath.Join(dataDir, e.Name())
			subEntries, _ := os.ReadDir(subDir)
			for _, se := range subEntries {
				fmt.Printf("    %s\n", se.Name())
			}
		}
	}

	// 验证数据可以查询到
	resp, err := h.QueryRange(context.Background(), oldTimestamp, oldTimestamp+int64(time.Second))
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		return
	}
	fmt.Printf("\nBefore retention: got %d rows\n", len(resp.Rows))

	// 计算 shard 的时间范围
	shardDurationNs := int64(h.Config().ShardDuration)
	shardStart := (oldTimestamp / shardDurationNs) * shardDurationNs
	shardEnd := shardStart + shardDurationNs
	fmt.Printf("Shard time range: [%d, %d)\n", shardStart, shardEnd)
	fmt.Printf("Current time: %d\n", time.Now().UnixNano())
	fmt.Printf("Retention cutoff would be: %d\n", time.Now().Add(-retentionPeriod).UnixNano())
	fmt.Printf("Should delete (EndTime < cutoff): %v\n", shardEnd < time.Now().Add(-retentionPeriod).UnixNano())

	// 等待 retention 服务删除过期数据
	fmt.Printf("\nWaiting for retention to kick in (retention=%v, checkInterval=%v)...\n",
		retentionPeriod, checkInterval)
	waitTime := retentionPeriod + checkInterval + 500*time.Millisecond
	fmt.Printf("Total wait time: %v\n", waitTime)
	time.Sleep(waitTime)

	// 再次验证数据目录
	fmt.Printf("\nData directory after retention wait:\n")
	entries2, _ := os.ReadDir(dataDir)
	if len(entries2) == 0 {
		fmt.Printf("  (empty - shard directory deleted!)\n")
	} else {
		for _, e := range entries2 {
			fmt.Printf("  Entry: %s (isDir=%v)\n", e.Name(), e.IsDir())
		}
	}

	// 再次查询，应该返回空（数据已过期）
	resp, err = h.QueryRange(context.Background(), oldTimestamp, oldTimestamp+int64(time.Second))
	if err != nil {
		fmt.Printf("Query after retention failed: %v\n", err)
		return
	}
	fmt.Printf("\nAfter retention: got %d rows\n", len(resp.Rows))

	if len(resp.Rows) == 0 {
		fmt.Printf("SUCCESS: Expired data was cleaned up by retention service!\n")
	} else {
		fmt.Printf("FAIL: Expected 0 rows after retention, got %d\n", len(resp.Rows))
		os.Exit(1)
	}
}
