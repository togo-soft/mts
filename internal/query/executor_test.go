// internal/query/executor_test.go
package query

import (
	"context"
	"testing"
	"time"

	"micro-ts/types"
)

func TestQueryExecutor_Execute(t *testing.T) {
	executor := NewExecutor(nil)

	req := &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   time.Now().UnixNano() - int64(time.Hour),
		EndTime:     time.Now().UnixNano(),
		Fields:      []string{"usage"},
		Limit:       100,
	}

	resp, err := executor.Execute(context.TODO(), req)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if resp == nil {
		t.Fatalf("expected non-nil response")
	}
}

func TestQueryExecutor_Execute_BasicFields(t *testing.T) {
	executor := NewExecutor(nil)

	now := time.Now()
	req := &types.QueryRangeRequest{
		Database:    "testdb",
		Measurement: "memory",
		StartTime:   now.UnixNano() - int64(2*time.Hour),
		EndTime:     now.UnixNano(),
		Fields:      []string{"used", "free"},
		Limit:       50,
	}

	resp, err := executor.Execute(context.TODO(), req)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if resp.Database != req.Database {
		t.Errorf("expected Database %s, got %s", req.Database, resp.Database)
	}
	if resp.Measurement != req.Measurement {
		t.Errorf("expected Measurement %s, got %s", req.Measurement, resp.Measurement)
	}
	if resp.StartTime != req.StartTime {
		t.Errorf("expected StartTime %d, got %d", req.StartTime, resp.StartTime)
	}
	if resp.EndTime != req.EndTime {
		t.Errorf("expected EndTime %d, got %d", req.EndTime, resp.EndTime)
	}
}

func TestQueryExecutor_Execute_EmptyResult(t *testing.T) {
	executor := NewExecutor(nil)

	req := &types.QueryRangeRequest{
		Database:    "emptydb",
		Measurement: "empty",
		StartTime:   time.Now().UnixNano() - int64(time.Hour),
		EndTime:     time.Now().UnixNano(),
		Fields:      []string{"field1"},
		Limit:       10,
	}

	resp, err := executor.Execute(context.TODO(), req)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if resp.TotalCount != 0 {
		t.Errorf("expected TotalCount 0, got %d", resp.TotalCount)
	}
	if resp.HasMore {
		t.Error("expected HasMore to be false")
	}
	if len(resp.Rows) != 0 {
		t.Errorf("expected empty Rows, got %d rows", len(resp.Rows))
	}
}
