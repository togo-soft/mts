// internal/query/executor_test.go
package query

import (
	"context"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

// dataIter 为测试提供数据源迭代器。
func dataIter(rows []*types.PointRow) *Iterator {
	req := &types.QueryRangeRequest{}
	it := &Iterator{req: req}
	sliceIt := &sliceIterator{rows: rows}
	it.heap = make(mergeHeap, 0, 1)
	if sliceIt.Current() != nil {
		it.heap = append(it.heap, sliceIt)
	}
	it.fetchNextValid()
	return it
}

func TestBuildPipeline_EmptyOps(t *testing.T) {
	_, err := BuildPipeline(nil, nil)
	if err == nil {
		t.Fatal("expected error for empty ops")
	}
}

func TestBuildPipeline_ScanOnly(t *testing.T) {
	rows := []*types.PointRow{{Timestamp: 100, Fields: []*types.FieldEntry{{Key: "cpu", Value: types.NewFieldValue(42.0)}}}}
	iter := dataIter(rows)
	ops := []*types.OperatorSpec{
		{Op: &types.OperatorSpec_Scan{Scan: &types.ScanSpec{}}},
	}

	head, err := BuildPipeline(iter, ops)
	if err != nil {
		t.Fatalf("BuildPipeline failed: %v", err)
	}

	row, err := head.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if row == nil {
		t.Fatal("expected non-nil row")
	}
}

func TestBuildPipeline_Project(t *testing.T) {
	rows := []*types.PointRow{{
		Timestamp: 100,
		Fields: []*types.FieldEntry{
			{Key: "cpu", Value: types.NewFieldValue(42.0)},
			{Key: "mem", Value: types.NewFieldValue(128.0)},
		},
	}}
	iter := dataIter(rows)
	ops := []*types.OperatorSpec{
		{Op: &types.OperatorSpec_Scan{Scan: &types.ScanSpec{}}},
		{Op: &types.OperatorSpec_Project{Project: &types.ProjectSpec{Fields: []string{"cpu"}}}},
	}

	head, err := BuildPipeline(iter, ops)
	if err != nil {
		t.Fatalf("BuildPipeline failed: %v", err)
	}

	row, err := head.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if row == nil {
		t.Fatal("expected non-nil row")
	}
	if len(row.Fields) != 1 || row.Fields[0].Key != "cpu" {
		t.Errorf("expected 1 field 'cpu', got %+v", row.Fields)
	}
}

func TestRowIterator_OpenNextClose(t *testing.T) {
	rows := []*types.PointRow{{Timestamp: 100, Fields: []*types.FieldEntry{{Key: "cpu", Value: types.NewFieldValue(42.0)}}}}
	iter := dataIter(rows)
	ops := []*types.OperatorSpec{
		{Op: &types.OperatorSpec_Scan{Scan: &types.ScanSpec{}}},
	}
	head, err := BuildPipeline(iter, ops)
	if err != nil {
		t.Fatalf("BuildPipeline failed: %v", err)
	}

	ri := NewRowIterator(head)
	if err := ri.Open(context.Background()); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if !ri.Next(context.Background()) {
		t.Fatal("expected Next to return true")
	}
	row := ri.Points()
	if row == nil || row.Timestamp != 100 {
		t.Errorf("expected timestamp 100, got %+v", row)
	}

	if err := ri.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if ri.Next(context.Background()) {
		t.Fatal("expected Next to return false after close")
	}
}

func TestRowIterator_ContextCancel(t *testing.T) {
	rows := []*types.PointRow{{Timestamp: 100, Fields: []*types.FieldEntry{{Key: "cpu", Value: types.NewFieldValue(42.0)}}}}
	iter := dataIter(rows)
	ops := []*types.OperatorSpec{
		{Op: &types.OperatorSpec_Scan{Scan: &types.ScanSpec{}}},
	}
	head, _ := BuildPipeline(iter, ops)

	ri := NewRowIterator(head)
	_ = ri.Open(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if ri.Next(ctx) {
		t.Fatal("expected Next to return false after context cancel")
	}
	_ = ri.Close()
}
