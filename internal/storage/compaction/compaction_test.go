package compaction

import (
	"container/heap"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// mockShardAccess 实现 ShardAccess 接口，用于测试 Manager。
type mockShardAccess struct {
	dir    string
	schema sstable.Schema
	seq    uint64
	refs   map[string]int32
	unused map[string]bool
}

func (m *mockShardAccess) CompressionAlgorithm() sstable.CompressionAlgorithm {
	return sstable.CompressionNone
}

func (m *mockShardAccess) Dir() string { return m.dir }

func (m *mockShardAccess) DataDir() string { return filepath.Join(m.dir, "data") }

func (m *mockShardAccess) NextSSTSeq() uint64 { m.seq++; return m.seq - 1 }

func (m *mockShardAccess) GetSchema() (sstable.Schema, error) { return m.schema, nil }

func (m *mockShardAccess) IsSSTUnused(path string) bool { return m.unused[path] }

func (m *mockShardAccess) AcquireSSTRef(path string) bool { m.refs[path]++; return true }

func (m *mockShardAccess) ReleaseSSTRef(path string) { m.refs[path]-- }

// pointsToInternal 将 []*types.Point 转换为 []types.InternalPoint。
func pointsToInternal(points []*types.Point) []types.InternalPoint {
	result := make([]types.InternalPoint, len(points))
	for i, p := range points {
		result[i] = types.PointToInternal(p, 0)
	}
	return result
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg == nil {
		t.Fatal("DefaultConfig should not return nil")
	}
	if cfg.MaxSstableCount != 4 {
		t.Errorf("expected MaxSstableCount=4, got %d", cfg.MaxSstableCount)
	}
	if cfg.MaxCompactionBatch != 0 {
		t.Errorf("expected MaxCompactionBatch=0, got %d", cfg.MaxCompactionBatch)
	}
	if cfg.ShardSizeLimit != ShardSizeLimit {
		t.Errorf("expected ShardSizeLimit=%d, got %d", ShardSizeLimit, cfg.ShardSizeLimit)
	}
	if cfg.CheckIntervalNanos != int64(time.Hour) {
		t.Errorf("expected CheckIntervalNanos=1h, got %d", cfg.CheckIntervalNanos)
	}
	if cfg.TimeoutNanos != int64(30*time.Minute) {
		t.Errorf("expected TimeoutNanos=30m, got %d", cfg.TimeoutNanos)
	}
}

func TestNewTask(t *testing.T) {
	inputFiles := []string{"/path/to/sst_1", "/path/to/sst_2"}
	outputPath := "/path/to/output"

	task := NewTask(inputFiles, outputPath)
	if task == nil {
		t.Fatal("NewTask should not return nil")
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

func TestProgress_Fields(t *testing.T) {
	now := time.Now()
	cp := &Progress{
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

func TestMergeIterator_Next_Point(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建第一个 SSTable（单文件 .bin 格式）
	w1, err := sstable.NewWriter(tmpDir, 0, 0, sstable.CompressionNone, sstable.FlagSorted)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	points1 := []*types.Point{
		{Timestamp: 1000, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(1.0))}},
		{Timestamp: 3000, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(3.0))}},
	}
	if err := w1.WritePoints(pointsToInternal(points1)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	schema1 := w1.Schema()
	if err := w1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 创建第二个 SSTable（单文件 .bin 格式）
	w2, err := sstable.NewWriter(tmpDir, 1, 0, sstable.CompressionNone, sstable.FlagSorted)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	points2 := []*types.Point{
		{Timestamp: 2000, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(2.0))}},
		{Timestamp: 4000, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(4.0))}},
	}
	if err := w2.WritePoints(pointsToInternal(points2)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	schema2 := w2.Schema()
	if err := w2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 合并 schema
	mergedSchema := schema1
	for k, v := range schema2.Fields {
		mergedSchema.Fields[k] = v
	}

	dataDir := filepath.Join(tmpDir, "data")
	r1, err := sstable.NewReader(filepath.Join(dataDir, "sst_0.bin"), mergedSchema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r1.Close() }()

	r2, err := sstable.NewReader(filepath.Join(dataDir, "sst_1.bin"), mergedSchema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r2.Close() }()

	it1, err := r1.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}
	it2, err := r2.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	mergeIter := NewMergeIterator([]*sstable.Iterator{it1, it2})

	// 验证按时间戳顺序合并
	expectedTimestamps := []int64{1000, 2000, 3000, 4000}
	for i, expected := range expectedTimestamps {
		if !mergeIter.Next() {
			t.Fatalf("expected Next()=true at index %d", i)
		}
		pt := mergeIter.Point()
		if pt == nil {
			t.Fatalf("expected non-nil Point at index %d", i)
		}
		if pt.Timestamp != expected {
			t.Errorf("expected timestamp=%d, got %d at index %d", expected, pt.Timestamp, i)
		}
	}

	// 所有数据消费完后
	if mergeIter.Next() {
		t.Error("expected Next()=false after all points consumed")
	}
	if mergeIter.Point() != nil {
		t.Error("expected nil Point after all points consumed")
	}
}

func TestMergeIterator_AfterEmpty(t *testing.T) {
	tmpDir := t.TempDir()

	// 第一个 SSTable：只有 1 个点（迭代器会最先变空）
	w1, err := sstable.NewWriter(tmpDir, 0, 0, sstable.CompressionNone, sstable.FlagSorted)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	points1 := []*types.Point{
		{Timestamp: 1000, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(1.0))}},
	}
	if err := w1.WritePoints(pointsToInternal(points1)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	schema1 := w1.Schema()
	if err := w1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 第二个 SSTable：2 个点
	w2, err := sstable.NewWriter(tmpDir, 1, 0, sstable.CompressionNone, sstable.FlagSorted)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	points2 := []*types.Point{
		{Timestamp: 2000, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(2.0))}},
		{Timestamp: 3000, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(3.0))}},
	}
	if err := w2.WritePoints(pointsToInternal(points2)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	schema2 := w2.Schema()
	if err := w2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 合并 schema
	mergedSchema := schema1
	for k, v := range schema2.Fields {
		mergedSchema.Fields[k] = v
	}

	dataDir := filepath.Join(tmpDir, "data")
	r1, err := sstable.NewReader(filepath.Join(dataDir, "sst_0.bin"), mergedSchema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r1.Close() }()

	r2, err := sstable.NewReader(filepath.Join(dataDir, "sst_1.bin"), mergedSchema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r2.Close() }()

	it1, err := r1.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}
	it2, err := r2.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	mergeIter := NewMergeIterator([]*sstable.Iterator{it1, it2})

	// 第一个点来自 iter1（timestamp=1000）
	if !mergeIter.Next() {
		t.Fatal("expected Next()=true for first point")
	}
	pt := mergeIter.Point()
	if pt == nil || pt.Timestamp != 1000 {
		t.Errorf("expected timestamp=1000, got %v", pt)
	}

	// iter1 为空后，后续点应来自 iter2
	if !mergeIter.Next() {
		t.Fatal("expected Next()=true for second point")
	}
	pt = mergeIter.Point()
	if pt == nil || pt.Timestamp != 2000 {
		t.Errorf("expected timestamp=2000 after iter1 empty, got %v", pt)
	}

	if !mergeIter.Next() {
		t.Fatal("expected Next()=true for third point")
	}
	pt = mergeIter.Point()
	if pt == nil || pt.Timestamp != 3000 {
		t.Errorf("expected timestamp=3000, got %v", pt)
	}

	if mergeIter.Next() {
		t.Error("expected Next()=false after all points consumed")
	}
}

func TestManager_Commit(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// 创建有效的输出 .bin 文件
	outputPath := filepath.Join(dataDir, "sst_0.bin")
	if err := os.WriteFile(outputPath, []byte("valid sstable data"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	mock := &mockShardAccess{
		dir:    tmpDir,
		schema: sstable.Schema{Fields: make(map[string]sstable.FieldType)},
		refs:   make(map[string]int32),
		unused: make(map[string]bool),
	}

	cm := NewManager(mock, nil)
	task := NewTask(nil, outputPath)
	task.MergedFiles = []string{}

	err := cm.Commit(task)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// 输出文件应该仍然存在
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Error("output file should still exist after commit")
	}
}

func TestManager_Commit_OutputIsDir(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// 创建目录（不是文件）作为 "output"
	dirPath := filepath.Join(dataDir, "sst_dir")
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	mock := &mockShardAccess{
		dir:    tmpDir,
		schema: sstable.Schema{Fields: make(map[string]sstable.FieldType)},
		refs:   make(map[string]int32),
		unused: make(map[string]bool),
	}

	cm := NewManager(mock, nil)
	task := NewTask(nil, dirPath)
	task.MergedFiles = []string{}

	err := cm.Commit(task)
	if err == nil {
		t.Error("Commit should fail when output is a directory")
	}
}

func TestManager_Commit_MergedFilesNilFallback(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	_ = os.MkdirAll(dataDir, 0755)

	outputPath := filepath.Join(dataDir, "sst_1.bin")
	if err := os.WriteFile(outputPath, []byte("valid sstable data"), 0644); err != nil {
		t.Fatal(err)
	}

	mock := &mockShardAccess{
		dir:    tmpDir,
		schema: sstable.Schema{Fields: make(map[string]sstable.FieldType)},
		refs:   make(map[string]int32),
		unused: make(map[string]bool),
	}
	cm := NewManager(mock, DefaultConfig())

	// MergedFiles 为 nil 时应回退到 InputFiles
	task := &Task{
		InputFiles:  []string{filepath.Join(dataDir, "input1.bin"), filepath.Join(dataDir, "input2.bin")},
		MergedFiles: nil,
		OutputPath:  outputPath,
	}

	// 创建 input 文件以通过验证
	for _, p := range task.InputFiles {
		_ = os.WriteFile(p, []byte("data"), 0644)
	}

	err := cm.Commit(task)
	if err != nil {
		t.Logf("Commit error (expected in test env): %v", err)
	}
	// 关键: Commit 不会 panic，MergedFiles nil 回退到 InputFiles 不会导致崩溃
}

func TestManager_Commit_DeferCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	_ = os.MkdirAll(dataDir, 0755)

	inputPath := filepath.Join(dataDir, "sst_10.bin")
	outputPath := filepath.Join(dataDir, "sst_merged.bin")

	_ = os.WriteFile(inputPath, []byte("input data"), 0644)
	_ = os.WriteFile(outputPath, []byte("merged data"), 0644)

	mock := &mockShardAccess{
		dir:    tmpDir,
		schema: sstable.Schema{Fields: make(map[string]sstable.FieldType)},
		refs:   make(map[string]int32),
		unused: map[string]bool{inputPath: true},
	}

	cm := NewManager(mock, DefaultConfig())

	task := &Task{
		InputFiles:  []string{inputPath},
		MergedFiles: []string{inputPath},
		OutputPath:  outputPath,
	}

	// Commit 应该成功
	err := cm.Commit(task)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestManager_Commit_OutputNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	_ = os.MkdirAll(dataDir, 0755)

	outputPath := filepath.Join(dataDir, "sst_nonexistent.bin")

	mock := &mockShardAccess{
		dir:    tmpDir,
		schema: sstable.Schema{Fields: make(map[string]sstable.FieldType)},
		refs:   make(map[string]int32),
		unused: make(map[string]bool),
	}

	cm := NewManager(mock, DefaultConfig())

	task := &Task{
		InputFiles:  []string{},
		MergedFiles: []string{},
		OutputPath:  outputPath,
	}

	err := cm.Commit(task)
	if err == nil {
		t.Error("expected error when output file doesn't exist")
	}
}

func TestManager_Merge_ContextCancel(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建 SSTable .bin 文件（包含数据，确保 Merge 进入循环）
	w, err := sstable.NewWriter(tmpDir, 0, 0, sstable.CompressionNone, sstable.FlagSorted)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	points := []*types.Point{
		{Timestamp: 1000, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(1.0))}},
		{Timestamp: 2000, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(2.0))}},
	}
	if err := w.WritePoints(pointsToInternal(points)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	schema := w.Schema()
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	inputPath := filepath.Join(tmpDir, "data", "sst_0.bin")
	outputPath := filepath.Join(tmpDir, "data", "sst_1.bin")

	mock := &mockShardAccess{
		dir:    tmpDir,
		schema: schema,
		refs:   make(map[string]int32),
		unused: make(map[string]bool),
	}
	mock.unused[inputPath] = true

	cm := NewManager(mock, nil)

	task := NewTask([]string{inputPath}, outputPath)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	err = cm.Merge(ctx, task)
	if err == nil {
		t.Error("Merge should return error on cancelled context")
	}

	// 清理 Merge 残留的输出文件
	_ = os.Remove(outputPath)
}

func TestManager_SetConfig(t *testing.T) {
	cm := NewManager(nil, DefaultConfig())

	newCfg := &Config{
		MaxSstableCount:    8,
		MaxCompactionBatch: 20,
		ShardSizeLimit:     2 * 1024 * 1024 * 1024,
		CheckIntervalNanos: int64(30 * time.Minute),
		TimeoutNanos:       int64(15 * time.Minute),
	}
	cm.SetConfig(newCfg)

	if cm.Config.MaxSstableCount != 8 {
		t.Errorf("MaxSstableCount = %d, want 8", cm.Config.MaxSstableCount)
	}
	if cm.Config.MaxCompactionBatch != 20 {
		t.Errorf("MaxCompactionBatch = %d, want 20", cm.Config.MaxCompactionBatch)
	}
	if cm.Config.ShardSizeLimit != 2*1024*1024*1024 {
		t.Errorf("ShardSizeLimit = %d, want 2GB", cm.Config.ShardSizeLimit)
	}
}

func TestManager_SetConfig_NilConfig(t *testing.T) {
	cfg := DefaultConfig()
	cm := NewManager(nil, cfg)
	cm.SetConfig(nil)
	if cm.Config != cfg {
		t.Error("config should not change on nil input")
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
