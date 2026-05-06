// tests/e2e/write_100k/main.go
package main

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/micro-ts/mts/tests/e2e/pkg/data_gen"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/framework"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/metrics"
)

func main() {
	h, err := framework.NewTestHarness("write_100k")
	if err != nil {
		fmt.Printf("Setup failed: %v\n", err)
		return
	}
	defer func() { _ = h.Close() }()

	metrics.GC()
	memBefore := metrics.ReadMemStats()
	fmt.Printf("Before write: %s\n", metrics.FormatMemStats(memBefore))

	gen := data_gen.NewDataGenerator(42)
	baseTime := h.StartTime()
	const count = 100000

	timer := metrics.NewTimer()
	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := gen.GeneratePoint(h.Config().DBName, h.Config().MeasurementName, ts)
		if err := h.DB().Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			return
		}
	}
	elapsed := timer.Elapsed()

	metrics.GC()
	memAfter := metrics.ReadMemStats()
	delta := metrics.CalcDelta(memBefore, memAfter)

	fmt.Printf("Write 100K: %d points in %v, TPS: %.2f\n", count, elapsed, metrics.TPS(count, elapsed))
	fmt.Printf("After write: %s\n", metrics.FormatMemStats(memAfter))
	fmt.Printf("Memory delta: %s\n", delta.Format())

	fmt.Printf("\nWaiting for data flush...\n")
	time.Sleep(2 * time.Second)

	fmt.Printf("\n%s\n", metrics.FormatStorageReport(h.DataDir(), count, 80))
}
