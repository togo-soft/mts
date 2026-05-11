// tests/e2e/query_100k/main.go
// 查询端测用例：100K 数据写入 → 多次刷盘 → Compaction 合并 → 分页查询延迟/内存分析
package main

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/micro-ts/mts/tests/e2e/pkg/data_gen"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/framework"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
	"codeberg.org/micro-ts/mts/types"
)

func main() {
	const count = 100000
	const pointInterval = int64(100 * time.Microsecond) // 集中在 1 个 Shard
	maxCount := int32(count / 6)

	h, err := framework.NewTestHarness("query_100k",
		framework.WithMaxCount(maxCount),
		framework.WithIdleDuration(1*time.Minute),
		framework.WithCompaction(3, 3*time.Second),
	)
	if err != nil {
		fmt.Printf("FAIL: setup: %v\n", err)
		return
	}
	defer func() { _ = h.Close() }()

	gen := data_gen.NewDataGenerator(42)
	baseTime := h.StartTime()
	endTime := baseTime + int64(count)*pointInterval

	metrics.GC()
	memBefore := metrics.ReadMemStats()

	fmt.Printf("Query 100K Benchmark\n")
	fmt.Printf("====================\n")
	fmt.Printf("Total points:   %d\n", count)
	fmt.Printf("MaxCount/flush: %d (~%d flushes)\n\n", maxCount, count/int(maxCount))

	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*pointInterval
		p := gen.GeneratePoint(h.Config().DBName, h.Config().MeasurementName, ts)
		if err := h.DB().Write(context.Background(), p); err != nil {
			fmt.Printf("FAIL: write at %d: %v\n", i, err)
			return
		}
	}

	metrics.GC()
	memAfterWrite := metrics.ReadMemStats()
	writeDelta := metrics.CalcDelta(memBefore, memAfterWrite)
	fmt.Printf("Write: %s, Δ: %s\n", metrics.FormatMemStats(memAfterWrite), writeDelta.Format())
	fmt.Printf("SSTables after write: %d\n\n", h.SSTableCount())

	fmt.Println("Waiting for compaction...")
	_ = h.WaitForCompaction(6, 60*time.Second)
	time.Sleep(3 * time.Second)

	sstCount := h.SSTableCount()
	diskBytes := h.DiskUsage()
	fmt.Printf("SSTables after compaction: %d\n", sstCount)
	fmt.Printf("Disk usage: %.2f MB (%.2f bytes/point)\n\n",
		float64(diskBytes)/(1024*1024), float64(diskBytes)/float64(count))

	metrics.GC()
	memBeforeQuery := metrics.ReadMemStats()

	const queryLimit = 2000
	timer := metrics.NewTimer()
	resp, err := h.DB().QueryRange(context.Background(), &types.QueryRangeRequest{
		Database:    h.Config().DBName,
		Measurement: h.Config().MeasurementName,
		StartTime:   baseTime,
		EndTime:     endTime,
		Offset:      0,
		Limit:       queryLimit,
	})
	elapsed := timer.Elapsed()

	if err != nil {
		fmt.Printf("FAIL: query: %v\n", err)
		return
	}

	metrics.GC()
	memAfterQuery := metrics.ReadMemStats()
	queryDelta := metrics.CalcDelta(memBeforeQuery, memAfterQuery)

	fmt.Printf("=== Query Result (paginated, limit=%d) ===\n", queryLimit)
	fmt.Printf("Rows returned:  %d\n", len(resp.Rows))
	fmt.Printf("Query latency:  %v\n", elapsed)
	fmt.Printf("Query TPS:      %.2f\n", metrics.TPS(len(resp.Rows), elapsed))
	fmt.Printf("Memory before:  %s\n", metrics.FormatMemStats(memBeforeQuery))
	fmt.Printf("Memory after:   %s\n", metrics.FormatMemStats(memAfterQuery))
	fmt.Printf("Memory delta:   %s\n", queryDelta.Format())
}
