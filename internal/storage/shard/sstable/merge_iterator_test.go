package sstable

import (
	"fmt"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

type nilRefManager struct{}

func (n *nilRefManager) AcquireSSTRef(path string) bool { return true }

func (n *nilRefManager) ReleaseSSTRef(path string) {}

func writeTestSSTable(t *testing.T, dir string, seq uint64, points []*types.Point) (string, Schema) {
	t.Helper()
	w, err := NewWriter(dir, seq, 0, CompressionNone, FlagSorted)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	ips := pointsToInternal(points)
	if err := w.WritePoints(ips); err != nil {
		t.Fatalf("WritePoints: %v", err)
	}

	schema := w.Schema()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	return filepath.Join(dir, "data", fmt.Sprintf("sst_%d.bin", seq)), schema
}

func TestMergeIterator_SingleFile(t *testing.T) {
	dir := t.TempDir()
	points := []*types.Point{
		{Timestamp: 100, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
		{Timestamp: 200, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(2.0)}},
		{Timestamp: 300, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(3.0)}},
	}
	fp, schema := writeTestSSTable(t, dir, 0, points)

	mi, err := NewMergeIterator([]string{fp}, 0, 0, schema, &nilRefManager{}, nil, nil)
	if err != nil {
		t.Fatalf("NewMergeIterator: %v", err)
	}
	defer func() { _ = mi.Close() }()

	var timestamps []int64
	for mi.Next() {
		timestamps = append(timestamps, mi.Point().Timestamp)
	}
	if len(timestamps) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(timestamps))
	}
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i] < timestamps[i-1] {
			t.Errorf("not sorted at %d: %d < %d", i, timestamps[i], timestamps[i-1])
		}
	}
}

func TestMergeIterator_MultiFile(t *testing.T) {
	dir := t.TempDir()
	points1 := []*types.Point{
		{Timestamp: 100, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
		{Timestamp: 300, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(3.0)}},
	}
	points2 := []*types.Point{
		{Timestamp: 200, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(2.0)}},
		{Timestamp: 400, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(4.0)}},
	}

	fp1, schema1 := writeTestSSTable(t, dir, 0, points1)
	fp2, _ := writeTestSSTable(t, dir, 1, points2)

	mi, err := NewMergeIterator([]string{fp1, fp2}, 0, 0, schema1, &nilRefManager{}, nil, nil)
	if err != nil {
		t.Fatalf("NewMergeIterator: %v", err)
	}
	defer func() { _ = mi.Close() }()

	var timestamps []int64
	for mi.Next() {
		timestamps = append(timestamps, mi.Point().Timestamp)
	}
	if len(timestamps) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(timestamps))
	}
	expected := []int64{100, 200, 300, 400}
	for i, exp := range expected {
		if timestamps[i] != exp {
			t.Errorf("pos %d: expected %d, got %d", i, exp, timestamps[i])
		}
	}
}

func TestMergeIterator_EndTimeFilter(t *testing.T) {
	dir := t.TempDir()
	points := []*types.Point{
		{Timestamp: 100, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
		{Timestamp: 200, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(2.0)}},
		{Timestamp: 300, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(3.0)}},
		{Timestamp: 400, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(4.0)}},
	}
	fp, schema := writeTestSSTable(t, dir, 0, points)

	// endTime=300 过滤：只返回 <300 的行
	mi, err := NewMergeIterator([]string{fp}, 0, 300, schema, &nilRefManager{}, nil, nil)
	if err != nil {
		t.Fatalf("NewMergeIterator: %v", err)
	}
	defer func() { _ = mi.Close() }()

	var timestamps []int64
	for mi.Next() {
		timestamps = append(timestamps, mi.Point().Timestamp)
	}
	if len(timestamps) != 2 {
		t.Fatalf("expected 2 rows with endTime=300, got %d: %v", len(timestamps), timestamps)
	}
	if timestamps[0] != 100 || timestamps[1] != 200 {
		t.Errorf("expected [100, 200], got %v", timestamps)
	}
}

func TestMergeIterator_EmptyFiles(t *testing.T) {
	mi, err := NewMergeIterator(nil, 0, 0, Schema{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewMergeIterator: %v", err)
	}
	defer func() { _ = mi.Close() }()
	if mi.Next() {
		t.Error("expected no rows from empty file list")
	}
}

func TestMergeIterator_NonexistentFile(t *testing.T) {
	mi, err := NewMergeIterator([]string{"/nonexistent/sst_0.bin"}, 0, 0, Schema{}, &nilRefManager{}, nil, nil)
	if err != nil {
		t.Fatalf("NewMergeIterator should not error on missing file: %v", err)
	}
	defer func() { _ = mi.Close() }()
	if mi.Next() {
		t.Error("expected no rows from nonexistent file")
	}
}

func TestMergeIterator_CloseReleasesResources(t *testing.T) {
	dir := t.TempDir()
	points := []*types.Point{
		{Timestamp: 100, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(1.0)}},
	}
	fp, schema := writeTestSSTable(t, dir, 0, points)

	mi, err := NewMergeIterator([]string{fp}, 0, 0, schema, &nilRefManager{}, nil, nil)
	if err != nil {
		t.Fatalf("NewMergeIterator: %v", err)
	}

	if err := mi.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	// 关闭后 Next 应返回 false
	if mi.Next() {
		t.Error("Next after Close should return false")
	}
}
