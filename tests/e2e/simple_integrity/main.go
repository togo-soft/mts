// tests/e2e/simple_integrity/main.go
package main

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/micro-ts/mts/tests/e2e/pkg/framework"
)

func main() {
	h, err := framework.NewTestHarness("simple_test", framework.WithIdleDuration(5*time.Second))
	if err != nil {
		fmt.Printf("Setup failed: %v\n", err)
		return
	}
	defer func() { _ = h.Close() }()

	const count = 100

	fmt.Printf("Writing %d points...\n", count)
	if err := h.WritePoints(context.Background(), count, time.Second); err != nil {
		fmt.Printf("Write failed: %v\n", err)
		return
	}

	fmt.Printf("Waiting for idle flush...\n")
	time.Sleep(6 * time.Second)

	fmt.Printf("Querying (same session)...\n")
	resp, err := h.QueryRange(context.Background(), h.StartTime(), h.StartTime()+int64(count)*int64(time.Second))
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		return
	}

	fmt.Printf("Got %d rows, expected %d\n", len(resp.Rows), count)

	if err := h.VerifyDataIntegrity(count, time.Second); err != nil {
		fmt.Printf("FAIL: %v\n", err)
	} else {
		fmt.Printf("SUCCESS: All data verified!\n")
	}
}