// tests/e2e/simple_integrity/main.go
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
	h, err := framework.NewTestHarness("simple_test", framework.WithIdleDuration(5*time.Second))
	if err != nil {
		fmt.Printf("Setup failed: %v\n", err)
		return
	}
	defer func() { _ = h.Close() }()

	const count = 100

	gen := data_gen.NewDataGenerator(42)
	baseTime := h.StartTime()

	timer := metrics.NewWriteSummary(count)
	fmt.Printf("Writing %d points...\n", count)
	for i := 0; i < count; i++ {
		ts := baseTime + int64(i)*time.Second.Nanoseconds()
		p := gen.GeneratePoint(h.Config().DBName, h.Config().MeasurementName, ts)
		if err := h.DB().Write(context.Background(), p); err != nil {
			fmt.Printf("Write failed at %d: %v\n", i, err)
			return
		}
	}
	timer.Finish()
	fmt.Printf("%s\n", timer.Format())

	fmt.Printf("Waiting for idle flush...\n")
	time.Sleep(6 * time.Second)

	fmt.Printf("Querying (same session)...\n")
	resp, err := h.QueryRange(context.Background(), h.StartTime(), h.StartTime()+int64(count)*int64(time.Second))
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		return
	}

	fmt.Printf("Got %d rows, expected %d\n", len(resp), count)

	if err := h.VerifyDataIntegrity(count, time.Second); err != nil {
		fmt.Printf("FAIL: %v\n", err)
	} else {
		fmt.Printf("SUCCESS: All data verified!\n")
	}
}
