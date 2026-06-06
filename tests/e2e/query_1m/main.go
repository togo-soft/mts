// tests/e2e/query_1m/main.go
// 查询端测用例：1M 数据写入 → 多次刷盘 → Compaction 合并 → 分页查询延迟/内存分析
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
	const count = 1000000
	const pointInterval = int64(100 * time.Microsecond)
	maxCount := int32(count / 6)

	h, err := framework.NewTestHarness("query_1m",
		framework.WithFlushCount(50000),
		framework.WithFlushIdle(10*time.Second),
		framework.WithCompaction(3, 5*time.Second),
	)
	if err != nil {
		fmt.Printf("FAIL: setup: %v\n", err)
		return
	}
	defer func() { _ = h.Close() }()

	gen := data_gen.NewDataGenerator(42)
	baseTime := h.StartTime()
	endTime := baseTime + int64(count)*pointInterval
	tsFmt := "15:04:05.000"

	overallTimer := metrics.NewTimer()

	metrics.GC()
	memBefore := metrics.ReadMemStats()

	fmt.Printf("Query 1M Benchmark\n")
	fmt.Printf("==================\n")
	fmt.Printf("Total points:   %d\n", count)
	fmt.Printf("MaxCount/flush: %d (~%d flushes)\n\n", maxCount, count/int(maxCount))

	// === Write Phase ===
	writeTimer := metrics.NewTimer()
	writeStart := time.Now()
	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*pointInterval
		p := gen.GeneratePoint(h.Config().DBName, h.Config().MeasurementName, ts)
		if err := h.DB().Write(context.Background(), p); err != nil {
			fmt.Printf("FAIL: write at %d: %v\n", i, err)
			return
		}
	}
	writeElapsed := writeTimer.Elapsed()

	metrics.GC()
	memAfterWrite := metrics.ReadMemStats()
	writeDelta := metrics.CalcDelta(memBefore, memAfterWrite)
	fmt.Printf("=== Write Phase ===\n")
	fmt.Printf("  Start:       %s\n", writeStart.Format(tsFmt))
	fmt.Printf("  End:         %s\n", time.Now().Format(tsFmt))
	fmt.Printf("  Duration:    %v\n", writeElapsed)
	fmt.Printf("  Write TPS:   %.2f\n", metrics.TPS(count, writeElapsed))
	fmt.Printf("  Memory:      %s, Δ: %s\n", metrics.FormatMemStats(memAfterWrite), writeDelta.Format())
	fmt.Printf("  SSTables:    %d\n\n", h.SSTableCount())

	// === Compaction Phase ===
	compactTimer := metrics.NewTimer()
	compactStart := time.Now()
	fmt.Println("=== Compaction Phase ===")
	fmt.Printf("  Start:       %s\n", compactStart.Format(tsFmt))
	_ = h.WaitForCompaction(10, 120*time.Second)
	time.Sleep(5 * time.Second)
	compactElapsed := compactTimer.Elapsed()

	sstCount := h.SSTableCount()
	diskBytes := h.DiskUsage()
	fmt.Printf("  End:         %s\n", time.Now().Format(tsFmt))
	fmt.Printf("  Duration:    %v\n", compactElapsed)
	fmt.Printf("  SSTables:    %d\n", sstCount)
	fmt.Printf("  Disk usage:  %.2f MB (%.2f bytes/point)\n\n",
		float64(diskBytes)/(1024*1024), float64(diskBytes)/float64(count))

	// === Query Phase ===
	const queryLimit = 2000
	metrics.GC()
	memBeforeQuery := metrics.ReadMemStats()

	queryTimer := metrics.NewTimer()
	queryStart := time.Now()
	it, err := h.DB().Iterator(context.Background(), &types.QueryRangeRequest{
		Database:    h.Config().DBName,
		Measurement: h.Config().MeasurementName,
		StartTime:   baseTime,
		EndTime:     endTime,
		Offset:      0,
		Limit:       queryLimit,
	})
	if err != nil {
		fmt.Printf("FAIL: query: %v\n", err)
		return
	}
	defer func() { _ = it.Close() }()
	var rows []*types.PointRow
	for it.Next(context.Background()) {
		rows = append(rows, it.Points())
	}
	queryElapsed := queryTimer.Elapsed()

	metrics.GC()
	memAfterQuery := metrics.ReadMemStats()
	queryDelta := metrics.CalcDelta(memBeforeQuery, memAfterQuery)

	fmt.Printf("=== Query Phase ===\n")
	fmt.Printf("  Start:       %s\n", queryStart.Format(tsFmt))
	fmt.Printf("  End:         %s\n", time.Now().Format(tsFmt))
	fmt.Printf("  Duration:    %v\n", queryElapsed)
	fmt.Printf("  Query TPS:   %.2f\n", metrics.TPS(len(rows), queryElapsed))
	fmt.Printf("  Rows:        %d (limit=%d)\n", len(rows), queryLimit)
	fmt.Printf("  Memory:      %s, Δ: %s\n", metrics.FormatMemStats(memAfterQuery), queryDelta.Format())
	fmt.Printf("  Memory before: %s\n", metrics.FormatMemStats(memBeforeQuery))

	// === Summary ===
	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("  Total elapsed: %v\n", overallTimer.Elapsed())
	fmt.Printf("  Write:         %v (%.1f%%)\n", writeElapsed, 100*float64(writeElapsed)/float64(overallTimer.Elapsed()))
	fmt.Printf("  Compaction:    %v (%.1f%%)\n", compactElapsed, 100*float64(compactElapsed)/float64(overallTimer.Elapsed()))
	fmt.Printf("  Query:         %v (%.1f%%)\n", queryElapsed, 100*float64(queryElapsed)/float64(overallTimer.Elapsed()))
	fmt.Printf("  Disk:           %.2f MB\n", float64(diskBytes)/(1024*1024))
	fmt.Printf("  Final SSTables: %d\n", sstCount)
}
