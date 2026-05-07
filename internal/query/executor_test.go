// internal/query/executor_test.go
package query

import (
	"context"
	"strings"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/types"
)

func TestQueryExecutor_Execute_NotImplemented(t *testing.T) {
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
	if err == nil {
		t.Fatal("expected error for unimplemented executor")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("expected 'not implemented' in error, got: %v", err)
	}
	if resp != nil {
		t.Errorf("expected nil response, got %+v", resp)
	}
}

func TestQueryExecutor_NewExecutor(t *testing.T) {
	executor := NewExecutor(nil)
	if executor == nil {
		t.Fatal("expected non-nil executor")
	}
}

func TestQueryExecutor_Execute_WithContext(t *testing.T) {
	executor := NewExecutor(nil)

	req := &types.QueryRangeRequest{
		Database:    "testdb",
		Measurement: "memory",
		StartTime:   time.Now().UnixNano() - int64(2*time.Hour),
		EndTime:     time.Now().UnixNano(),
		Fields:      []string{"used", "free"},
		Limit:       50,
	}

	_, err := executor.Execute(context.TODO(), req)
	if err == nil {
		t.Fatal("expected error for unimplemented executor")
	}
}

func TestQueryExecutor_Execute_EmptyReq(t *testing.T) {
	executor := NewExecutor(nil)

	req := &types.QueryRangeRequest{
		Database:    "emptydb",
		Measurement: "empty",
		StartTime:   time.Now().UnixNano() - int64(time.Hour),
		EndTime:     time.Now().UnixNano(),
		Fields:      []string{"field1"},
		Limit:       10,
	}

	_, err := executor.Execute(context.TODO(), req)
	if err == nil {
		t.Fatal("expected error for unimplemented executor")
	}
}
