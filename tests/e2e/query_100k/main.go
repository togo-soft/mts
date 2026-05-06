// tests/e2e/query_100k/main.go
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
	h, err := framework.NewTestHarness("query_100k")
	if err != nil {
		fmt.Printf("Setup failed: %v\n", err)
		return
	}
	defer func() { _ = h.Close() }()

	gen := data_gen.NewDataGenerator(42)
	baseTime := h.StartTime()
	const count = 100000

	metrics.GC()
	memBeforeWrite := metrics.ReadMemStats()

	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint(h.Config().DBName, h.Config().MeasurementName, ts)
		if err := h.DB().Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			return
		}
	}

	metrics.GC()
	memAfterWrite := metrics.ReadMemStats()
	writeDelta := metrics.CalcDelta(memBeforeWrite, memAfterWrite)

	fmt.Printf("Write 100K: %d points\n", count)
	fmt.Printf("After write: %s, Δ: %s\n\n", metrics.FormatMemStats(memAfterWrite), writeDelta.Format())

	fmt.Printf("Waiting 15s for idle flush to trigger...\n")
	time.Sleep(15 * time.Second)

	metrics.GC()
	memBeforeQuery := metrics.ReadMemStats()
	fmt.Printf("Before query: %s\n", metrics.FormatMemStats(memBeforeQuery))

	const queryLimit = 2000
	timer := metrics.NewTimer()
	resp, err := h.DB().QueryRange(context.Background(), &types.QueryRangeRequest{
		Database:    h.Config().DBName,
		Measurement: h.Config().MeasurementName,
		StartTime:   baseTime,
		EndTime:     baseTime + int64(count)*int64(time.Second),
		Offset:      0,
		Limit:       queryLimit,
	})
	elapsed := timer.Elapsed()

	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		return
	}

	metrics.GC()
	memAfterQuery := metrics.ReadMemStats()
	queryDelta := metrics.CalcDelta(memBeforeQuery, memAfterQuery)

	fmt.Printf("Query 100K (paginated): %d rows in %v, TPS: %.2f (limit=%d)\n", len(resp.Rows), elapsed, metrics.TPS(len(resp.Rows), elapsed), queryLimit)
	fmt.Printf("After query: %s\n", metrics.FormatMemStats(memAfterQuery))
	fmt.Printf("Query memory delta: %s\n", queryDelta.Format())
}