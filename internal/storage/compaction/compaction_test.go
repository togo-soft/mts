package compaction

import (
	"container/heap"
	"testing"
	"time"
)

func TestDefaultCompactionConfig(t *testing.T) {
	cfg := DefaultCompactionConfig()
	if cfg == nil {
		t.Fatal("DefaultCompactionConfig should not return nil")
	}
	if cfg.MaxSSTableCount != 4 {
		t.Errorf("expected MaxSSTableCount=4, got %d", cfg.MaxSSTableCount)
	}
	if cfg.MaxCompactionBatch != 0 {
		t.Errorf("expected MaxCompactionBatch=0, got %d", cfg.MaxCompactionBatch)
	}
	if cfg.ShardSizeLimit != ShardSizeLimit {
		t.Errorf("expected ShardSizeLimit=%d, got %d", ShardSizeLimit, cfg.ShardSizeLimit)
	}
	if cfg.CheckInterval != time.Hour {
		t.Errorf("expected CheckInterval=1h, got %v", cfg.CheckInterval)
	}
	if cfg.Timeout != 30*time.Minute {
		t.Errorf("expected Timeout=30m, got %v", cfg.Timeout)
	}
}

func TestNewCompactionTask(t *testing.T) {
	inputFiles := []string{"/path/to/sst_1", "/path/to/sst_2"}
	outputPath := "/path/to/output"

	task := NewCompactionTask(inputFiles, outputPath)
	if task == nil {
		t.Fatal("NewCompactionTask should not return nil")
	}
	if len(task.InputFiles) != 2 {
		t.Errorf("expected 2 input files, got %d", len(task.InputFiles))
	}
	if task.OutputPath != outputPath {
		t.Errorf("expected outputPath=%s, got %s", outputPath, task.OutputPath)
	}
	if task.Progress != 0 {
		t.Errorf("expected progress=0, got %d", task.Progress)
	}
	if task.StartedAt.IsZero() {
		t.Error("startedAt should not be zero")
	}
}

func TestMergeHeap_Len(t *testing.T) {
	h := MergeHeap{}
	if h.Len() != 0 {
		t.Errorf("expected len=0, got %d", h.Len())
	}

	h = append(h, &MergeHeapItem{})
	if h.Len() != 1 {
		t.Errorf("expected len=1, got %d", h.Len())
	}
}

func TestMergeHeap_Less(t *testing.T) {
	h := MergeHeap{
		{Timestamp: 100},
		{Timestamp: 200},
	}

	if !h.Less(0, 1) {
		t.Error("timestamp 100 should be less than 200")
	}

	h[0].Timestamp = 100
	h[1].Timestamp = 100
	h[0].Idx = 0
	h[1].Idx = 1

	if !h.Less(0, 1) {
		t.Error("idx 0 should be less than idx 1 when timestamps equal")
	}
}

func TestMergeHeap_Swap(t *testing.T) {
	h := MergeHeap{
		{Timestamp: 100, Idx: 0},
		{Timestamp: 200, Idx: 1},
	}

	h.Swap(0, 1)

	if h[0].Timestamp != 200 || h[1].Timestamp != 100 {
		t.Error("Swap did not work correctly")
	}
}

func TestMergeHeap_PushPop(t *testing.T) {
	h := make(MergeHeap, 0)

	heap.Push(&h, &MergeHeapItem{Timestamp: 100, Idx: 0})
	heap.Push(&h, &MergeHeapItem{Timestamp: 50, Idx: 1})
	heap.Push(&h, &MergeHeapItem{Timestamp: 200, Idx: 2})

	if h.Len() != 3 {
		t.Errorf("expected len=3, got %d", h.Len())
	}

	item := heap.Pop(&h).(*MergeHeapItem)
	if item.Timestamp != 50 {
		t.Errorf("expected timestamp=50, got %d", item.Timestamp)
	}

	if h.Len() != 2 {
		t.Errorf("expected len=2, got %d", h.Len())
	}
}

func TestMergeIterator_Empty(t *testing.T) {
	mergeIter := NewMergeIterator(nil)

	if mergeIter.Next() {
		t.Error("Next should return false for empty iterator list")
	}

	if mergeIter.Point() != nil {
		t.Error("Point should be nil when heap is empty")
	}
}

func TestCompactionProgress_Fields(t *testing.T) {
	now := time.Now()
	cp := &CompactionProgress{
		InputFiles: []string{"a", "b"},
		OutputFile: "out",
		Progress:   50,
		Status:     "running",
		StartedAt:  now,
	}

	if len(cp.InputFiles) != 2 {
		t.Errorf("expected 2 input files, got %d", len(cp.InputFiles))
	}
	if cp.OutputFile != "out" {
		t.Errorf("expected OutputFile=out, got %s", cp.OutputFile)
	}
	if cp.Progress != 50 {
		t.Errorf("expected Progress=50, got %d", cp.Progress)
	}
	if cp.Status != "running" {
		t.Errorf("expected Status=running, got %s", cp.Status)
	}
}

func TestShardSizeLimit(t *testing.T) {
	if ShardSizeLimit != 1*1024*1024*1024 {
		t.Errorf("expected ShardSizeLimit=1GB, got %d", ShardSizeLimit)
	}
}

func TestDirSize(t *testing.T) {
	tmpDir := t.TempDir()
	// 空目录
	size, err := DirSize(tmpDir)
	if err != nil {
		t.Fatalf("DirSize failed: %v", err)
	}
	if size != 0 {
		t.Errorf("expected 0 for empty dir, got %d", size)
	}
}
