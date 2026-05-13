# P0 查询流式化与 MemTable 有序性加固 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除 ShardIterator 全量加载 OOM 风险并加固 MemTable 有序性保证

**Architecture:** SSTableMergeIterator 用 heap 归并多个 sstable.Iterator 的流式输出；ShardIterator 用 MemTableIterator + SSTableMergeIterator 二路归并；移除 Shard.Read 系列冗余方法；MemTable 用 sorted 标志 + Sort() 防御性排序

**Tech Stack:** Go 1.22+, container/heap, sort, sync

---

### Task 1: MemTable Write() 逻辑修正 + Sort() 方法

**Files:**
- Modify: `internal/storage/memtable/memtable.go:68-76`

- [ ] **Step 1: 修正 Write() 的 sorted 逻辑**

将 `memtable.go:68-76` 中的排序检查改为：

```go
// 如果已知未排序或末尾乱序，执行全量排序
if !m.sorted || (m.activeCount > 1 && m.active[m.activeCount-1].Timestamp < m.active[m.activeCount-2].Timestamp) {
	sort.Slice(m.active, func(i, j int) bool {
		return m.active[i].Timestamp < m.active[j].Timestamp
	})
}
m.sorted = true
```

- [ ] **Step 2: 新增 Sort() 公开方法**

在 `memtable.go` 末尾（`ActiveFull()` 方法之后）添加：

```go
// Sort 对 active 进行排序，确保数据有序。
// 用于 WAL Replay 后或任何需要防御性排序的场景。
func (m *MemTable) Sort() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activeCount > 1 {
		sort.Slice(m.active, func(i, j int) bool {
			return m.active[i].Timestamp < m.active[j].Timestamp
		})
	}
	m.sorted = true
}
```

- [ ] **Step 3: 运行 memtable 现有测试确认无回归**

```bash
go test ./internal/storage/memtable/ -v -count=1
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add internal/storage/memtable/memtable.go
git commit -m "fix(memtable): 修正 sorted 字段语义并新增 Sort() 防御性排序方法"
```

---

### Task 2: WAL Replay 后显式排序

**Files:**
- Modify: `internal/storage/shard/shard.go:590-596`

- [ ] **Step 1: 在 ReplayWAL 最终 flush 前调用 Sort()**

在 `shard.go` ReplayWAL 方法中，replay 循环结束后，最终 flush 前（约 590 行），添加：

```go
// replay 完成后，确保 MemTable 数据有序
s.memTable.Sort()

// replay 完成后，如果 MemTable 还有数据，flush 到 SSTable
if s.memTable.Count() > 0 {
```

- [ ] **Step 2: 运行 shard 测试确认无回归**

```bash
go test ./internal/storage/shard/ -v -count=1 -run TestReplay
```

Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/shard.go
git commit -m "fix(shard): WAL Replay 结束后显式排序 MemTable 确保数据有序"
```

---

### Task 3: MemTable 新测试

**Files:**
- Modify: `internal/storage/memtable/memtable_test.go`

- [ ] **Step 1: 新增 TestMemTable_SortAfterSwap 测试**

在 `memtable_test.go` 末尾添加：

```go
func TestMemTable_SortAfterSwap(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	// 写入乱序数据：模拟 Swap 后写入乱序点
	m.sorted = false // 模拟未排序状态
	_ = m.Write(types.InternalPoint{Timestamp: 100, Sid: 1})
	_ = m.Write(types.InternalPoint{Timestamp: 50, Sid: 2})
	_ = m.Write(types.InternalPoint{Timestamp: 200, Sid: 3})

	// 验证有序
	iter := m.Iterator()
	var timestamps []int64
	for iter.Next() {
		timestamps = append(timestamps, iter.Point().Timestamp)
	}
	if len(timestamps) != 3 {
		t.Fatalf("expected 3 points, got %d", len(timestamps))
	}
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i] < timestamps[i-1] {
			t.Errorf("timestamps not sorted at %d: %d < %d", i, timestamps[i], timestamps[i-1])
		}
	}
}
```

- [ ] **Step 2: 新增 TestMemTable_SortMethod 测试**

```go
func TestMemTable_SortMethod(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	_ = m.Write(types.InternalPoint{Timestamp: 300, Sid: 1})
	_ = m.Write(types.InternalPoint{Timestamp: 100, Sid: 2})
	_ = m.Write(types.InternalPoint{Timestamp: 200, Sid: 3})

	m.Sort()

	iter := m.Iterator()
	prev := int64(0)
	for iter.Next() {
		ts := iter.Point().Timestamp
		if ts < prev {
			t.Errorf("Sort() failed: %d < %d", ts, prev)
		}
		prev = ts
	}
}
```

- [ ] **Step 3: 新增 TestMemTable_SortedFlag 测试**

```go
func TestMemTable_SortedFlag(t *testing.T) {
	m := NewMemTable(DefaultMemTableConfig())

	// 初始状态 sorted 为 false
	if m.sorted {
		t.Error("expected sorted=false after NewMemTable")
	}

	// Write 后 sorted 应为 true
	_ = m.Write(types.InternalPoint{Timestamp: 100, Sid: 1})
	if !m.sorted {
		t.Error("expected sorted=true after Write")
	}

	// Swap 后 sorted 应为 false
	m.Swap()
	if m.sorted {
		t.Error("expected sorted=false after Swap")
	}

	// Sort() 后 sorted 应为 true
	m.Sort()
	if !m.sorted {
		t.Error("expected sorted=true after Sort()")
	}
}
```

- [ ] **Step 4: 运行测试确认全部通过**

```bash
go test ./internal/storage/memtable/ -v -count=1
```

Expected: ALL PASS

- [ ] **Step 5: 验证覆盖率**

```bash
go test ./internal/storage/memtable/ -cover -coverprofile=/tmp/mem_cover.out
go tool cover -func=/tmp/mem_cover.out
rm -f /tmp/mem_cover.out
```

Expected: ≥90%

- [ ] **Step 6: Commit**

```bash
git add internal/storage/memtable/memtable_test.go
git commit -m "test(memtable): 新增 sorted 标志和 Sort() 方法测试"
```

---

### Task 4: 新增 SSTableMergeIterator

**Files:**
- Create: `internal/storage/shard/sstable/merge_iterator.go`

- [ ] **Step 1: 创建 merge_iterator.go**

```go
package sstable

import (
	"container/heap"
	"os"

	"codeberg.org/micro-ts/mts/types"
)

// SSTableRefManager 管理 SSTable 文件的引用计数。
// 由 Shard 实现，用于防止 Compaction 删除正在读取的文件。
type SSTableRefManager interface {
	AcquireSSTRef(path string) bool
	ReleaseSSTRef(path string)
}

// mergeItem 是堆中的一个条目。
type mergeItem struct {
	iter  *Iterator
	point *types.PointRow // 当前 peek 的行
	idx   int             // 在 heap 切片中的位置
}

// mergeHeap 实现 container/heap.Interface，按时间戳升序。
type mergeHeap []*mergeItem

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	ti := h[i].point.Timestamp
	tj := h[j].point.Timestamp
	if ti == tj {
		return h[i].idx < h[j].idx
	}
	return ti < tj
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}

func (h *mergeHeap) Push(x any) {
	item := x.(*mergeItem)
	item.idx = len(*h)
	*h = append(*h, item)
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // 帮助 GC
	*h = old[0 : n-1]
	return item
}

// MergeIterator 合并多个 SSTable 文件，按时间戳升序输出。
// 内部对每个文件使用 sstable.Iterator 进行块级流式读取。
type MergeIterator struct {
	heap      mergeHeap
	current   *types.PointRow
	readers   []*Reader
	err       error
	endTime   int64
	refMgr    SSTableRefManager
	openFiles []string
}

// NewMergeIterator 创建多 SSTable 合并迭代器。
//
// 参数：
//   - filePaths: SSTable 文件路径列表
//   - startTime: 查询起始时间（包含）
//   - endTime:   查询结束时间（不包含）
//   - schema:    解码所需的 Schema
//   - refMgr:    引用计数管理器（Shard 实现），可为 nil
//   - fields:    需要投影的字段（nil=全部字段）
func NewMergeIterator(filePaths []string, startTime, endTime int64, schema Schema, refMgr SSTableRefManager, fields []string) (*MergeIterator, error) {
	mi := &MergeIterator{
		heap:      make(mergeHeap, 0, len(filePaths)),
		endTime:   endTime,
		refMgr:    refMgr,
		openFiles: make([]string, 0, len(filePaths)),
	}

	for _, fp := range filePaths {
		if refMgr != nil {
			if !refMgr.AcquireSSTRef(fp) {
				continue // 文件已被删除
			}
		}

		r, err := NewReader(fp, schema)
		if err != nil {
			if refMgr != nil {
				refMgr.ReleaseSSTRef(fp)
			}
			continue // 跳过无法打开的文件
		}
		mi.readers = append(mi.readers, r)
		mi.openFiles = append(mi.openFiles, fp)

		iter, err := r.NewIterator(fields)
		if err != nil {
			if refMgr != nil {
				refMgr.ReleaseSSTRef(fp)
			}
			continue
		}

		// 定位到起始时间
		if startTime > 0 {
			if err := iter.SeekToTime(startTime); err != nil {
				continue
			}
		}

		// 读取第一条匹配的数据
		if iter.Next() {
			row := iter.Point()
			if row == nil {
				continue
			}
			// 检查是否在时间范围内
			if endTime > 0 && row.Timestamp >= endTime {
				continue
			}
			item := &mergeItem{iter: iter, point: row}
			heap.Push(&mi.heap, item)
		}
	}

	return mi, nil
}

// Next 移动到下一个时间戳最小的行。
func (mi *MergeIterator) Next() bool {
	if len(mi.heap) == 0 {
		return false
	}

	// 弹出堆顶（最小时间戳）
	item := heap.Pop(&mi.heap).(*mergeItem)
	mi.current = item.point

	// 从同一 iterator 获取下一行
	for item.iter.Next() {
		row := item.iter.Point()
		if row == nil {
			continue
		}
		if mi.endTime > 0 && row.Timestamp >= mi.endTime {
			continue
		}
		item.point = row
		heap.Push(&mi.heap, item)
		return true
	}

	// 该 iterator 已耗尽
	return true
}

// Point 返回当前行。
func (mi *MergeIterator) Point() *types.PointRow {
	return mi.current
}

// Close 关闭所有底层 Reader 并释放引用。
func (mi *MergeIterator) Close() error {
	for _, r := range mi.readers {
		_ = r.Close()
	}
	if mi.refMgr != nil {
		for _, fp := range mi.openFiles {
			mi.refMgr.ReleaseSSTRef(fp)
		}
	}
	mi.readers = nil
	mi.openFiles = nil
	mi.heap = nil
	return nil
}

// Err 返回迭代过程中发生的错误。
func (mi *MergeIterator) Err() error {
	return mi.err
}
```

- [ ] **Step 2: 编译验证**

```bash
go build ./internal/storage/shard/sstable/
```

Expected: 编译成功

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/sstable/merge_iterator.go
git commit -m "feat(sstable): 新增 MergeIterator 支持多 SSTable 流式堆归并"
```

---

### Task 5: SSTableMergeIterator 测试

**Files:**
- Create: `internal/storage/shard/sstable/merge_iterator_test.go`

- [ ] **Step 1: 编写完整测试文件**

```go
package sstable

import (
	"os"
	"path/filepath"
	"testing"
)

type nilRefManager struct{}

func (n *nilRefManager) AcquireSSTRef(path string) bool { return true }
func (n *nilRefManager) ReleaseSSTRef(path string)      {}

func writeTestSSTable(t *testing.T, dir string, seq uint64, points []*types.PointRow) string {
	t.Helper()
	w, err := NewWriter(dir, seq, 64*1024, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ips := make([]InternalPoint, len(points))
	for i, p := range points {
		ips[i] = InternalPoint{
			Timestamp: p.Timestamp,
			Sid:       p.Sid,
			Fields:    InternalFieldsFromMap(p.Fields),
		}
	}
	if err := w.WritePoints(ips); err != nil {
		t.Fatalf("WritePoints: %v", err)
	}
	outputPath, err := w.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	return outputPath
}

func TestMergeIterator_SingleFile(t *testing.T) {
	dir := t.TempDir()
	points := []*types.PointRow{
		{Timestamp: 100, Sid: 1, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(1.0))}},
		{Timestamp: 200, Sid: 2, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(2.0))}},
		{Timestamp: 300, Sid: 3, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(3.0))}},
	}
	fp := writeTestSSTable(t, dir, 0, points)
	_ = dir // use dir
	_ = fp  // use fp

	mi, err := NewMergeIterator([]string{fp}, 0, 0, Schema{Fields: map[string]FieldType{"v": FieldTypeFloat64}}, &nilRefManager{}, nil)
	if err != nil {
		t.Fatalf("NewMergeIterator: %v", err)
	}
	defer mi.Close()

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
	points1 := []*types.PointRow{
		{Timestamp: 100, Sid: 1, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(1.0))}},
		{Timestamp: 300, Sid: 3, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(3.0))}},
	}
	points2 := []*types.PointRow{
		{Timestamp: 200, Sid: 2, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(2.0))}},
		{Timestamp: 400, Sid: 4, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(4.0))}},
	}
	fp1 := writeTestSSTable(t, dir, 0, points1)
	fp2 := writeTestSSTable(t, dir, 1, points2)

	mi, err := NewMergeIterator([]string{fp1, fp2}, 0, 0, Schema{Fields: map[string]FieldType{"v": FieldTypeFloat64}}, &nilRefManager{}, nil)
	if err != nil {
		t.Fatalf("NewMergeIterator: %v", err)
	}
	defer mi.Close()

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

func TestMergeIterator_TimeRange(t *testing.T) {
	dir := t.TempDir()
	points := []*types.PointRow{
		{Timestamp: 100, Sid: 1, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(1.0))}},
		{Timestamp: 200, Sid: 2, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(2.0))}},
		{Timestamp: 300, Sid: 3, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(3.0))}},
		{Timestamp: 400, Sid: 4, Fields: map[string]*types.FieldValue{"v": types.NewFieldValue(float64(4.0))}},
	}
	fp := writeTestSSTable(t, dir, 0, points)

	// 只查询 [150, 350) 范围
	mi, err := NewMergeIterator([]string{fp}, 150, 350, Schema{Fields: map[string]FieldType{"v": FieldTypeFloat64}}, &nilRefManager{}, nil)
	if err != nil {
		t.Fatalf("NewMergeIterator: %v", err)
	}
	defer mi.Close()

	var timestamps []int64
	for mi.Next() {
		timestamps = append(timestamps, mi.Point().Timestamp)
	}
	if len(timestamps) != 2 {
		t.Fatalf("expected 2 rows in range, got %d", len(timestamps))
	}
	if timestamps[0] != 200 || timestamps[1] != 300 {
		t.Errorf("expected [200, 300], got %v", timestamps)
	}
}

func TestMergeIterator_EmptyFiles(t *testing.T) {
	mi, err := NewMergeIterator(nil, 0, 0, Schema{}, nil, nil)
	if err != nil {
		t.Fatalf("NewMergeIterator: %v", err)
	}
	defer mi.Close()
	if mi.Next() {
		t.Error("expected no rows from empty file list")
	}
}

func TestMergeIterator_NonexistentFile(t *testing.T) {
	mi, err := NewMergeIterator([]string{"/nonexistent/sst_0.bin"}, 0, 0, Schema{}, &nilRefManager{}, nil)
	if err != nil {
		t.Fatalf("NewMergeIterator should not error on missing file: %v", err)
	}
	defer mi.Close()
	if mi.Next() {
		t.Error("expected no rows from nonexistent file")
	}
}
```

- [ ] **Step 2: 运行测试**

```bash
go test ./internal/storage/shard/sstable/ -run TestMergeIterator -v -count=1
```

Expected: ALL PASS

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/sstable/merge_iterator_test.go
git commit -m "test(sstable): 新增 MergeIterator 流式归并测试"
```

---

### Task 6: 新增 Shard.listSSTableFiles() 辅助方法

**Files:**
- Modify: `internal/storage/shard/shard_io.go` — 新增方法，保留原有代码供测试编译（后续 Task 移除）

- [ ] **Step 1: 在 shard_io.go 末尾添加 listSSTableFiles**

```go
// listSSTableFiles 列出 Shard 中所有可读的 SSTable 文件路径。
// 自动处理 flat（data/sst_*.bin）和 leveled（data/L0/sst_*.bin, ...）两种目录结构。
func (s *Shard) listSSTableFiles() []string {
	dataDir := filepath.Join(s.dir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return nil
	}

	var files []string

	// 如果使用 Level Compaction，扫描 L0, L1, L2 等目录
	if s.levelCompaction != nil {
		for level := 0; ; level++ {
			levelDir := filepath.Join(dataDir, fmt.Sprintf("L%d", level))
			entries, err := os.ReadDir(levelDir)
			if err != nil {
				break // 没有更多层级目录
			}
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				if !strings.HasPrefix(entry.Name(), "sst_") || !strings.HasSuffix(entry.Name(), ".bin") {
					continue
				}
				files = append(files, filepath.Join(levelDir, entry.Name()))
			}
		}
		return files
	}

	// flat 结构：data/sst_*.bin
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), "sst_") || !strings.HasSuffix(entry.Name(), ".bin") {
			continue
		}
		files = append(files, filepath.Join(dataDir, entry.Name()))
	}
	return files
}
```

- [ ] **Step 2: 编译验证**

```bash
go build ./internal/storage/shard/
```

Expected: 编译成功

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/shard_io.go
git commit -m "feat(shard): 新增 listSSTableFiles() 提取 SSTable 文件扫描逻辑"
```

---

### Task 7: 重写 ShardIterator 使用流式归并

**Files:**
- Modify: `internal/storage/shard/iterator.go`

- [ ] **Step 1: 修改 ShardIterator 结构体**

将 `iterator.go` 中 ShardIterator 结构体的 `rows`/`rowIdx` 替换为 `sstIter`：

```go
// ShardIterator 是单个 Shard 的数据迭代器。
//
// 功能：
//   - 合并 MemTable 和 SSTable 的数据源（流式读取，不预加载）
//   - 按时间戳升序返回数据
//   - 支持时间范围过滤
type ShardIterator struct {
	shard     *Shard
	startTime int64
	endTime   int64

	memIter *memtable.MemTableIterator
	sstIter *sstable.MergeIterator // 流式 SSTable 归并（替代 rows 预加载）

	// 当前 peek
	memRow *types.PointRow
	sstRow *types.PointRow

	produced int
	maxRows  int
	err      error

	mu sync.RWMutex
}
```

- [ ] **Step 2: 重写 NewShardIterator**

```go
func NewShardIterator(shard *Shard, startTime, endTime int64, maxRows int) *ShardIterator {
	si := &ShardIterator{
		shard:     shard,
		startTime: startTime,
		endTime:   endTime,
		maxRows:   maxRows,
	}

	// 创建 MemTable 迭代器
	si.memIter = shard.memTable.Iterator()
	if si.memIter.Next() {
		ip := si.memIter.Point()
		si.memRow = si.pointToRow(ip)
	}

	// 创建流式 SSTable MergeIterator
	sstFiles := shard.listSSTableFiles()
	if len(sstFiles) > 0 {
		schema, err := shard.GetSchema()
		if err != nil {
			si.err = fmt.Errorf("get schema: %w", err)
			return si
		}
		sstIter, err := sstable.NewMergeIterator(sstFiles, startTime, endTime, schema, shard, nil)
		if err != nil {
			si.err = fmt.Errorf("create SSTable merge iterator: %w", err)
			return si
		}
		si.sstIter = sstIter
		if sstIter.Next() {
			si.sstRow = sstIter.Point()
		}
	}

	return si
}
```

需要在 import 中添加 `"fmt"`。

- [ ] **Step 3: 重写 nextSstRowLocked**

```go
func (si *ShardIterator) nextSstRowLocked() *types.PointRow {
	if si.sstIter == nil {
		return nil
	}
	if si.sstIter.Next() {
		return si.sstIter.Point()
	}
	return nil
}
```

- [ ] **Step 4: 新增 Close 方法（在文件末尾）**

```go
// Close 释放 SSTable MergeIterator 持有的资源。
func (si *ShardIterator) Close() {
	si.mu.Lock()
	defer si.mu.Unlock()
	if si.sstIter != nil {
		_ = si.sstIter.Close()
		si.sstIter = nil
	}
}
```

- [ ] **Step 5: 编译验证**

```bash
go build ./internal/storage/shard/
```

Expected: 编译成功

- [ ] **Step 6: Commit**

```bash
git add internal/storage/shard/iterator.go
git commit -m "refactor(shard): ShardIterator 改用 SSTableMergeIterator 流式归并替代全量预加载"
```

---

### Task 8: 移除 Shard.Read / readFromSSTable / readSSTableFile

**Files:**
- Modify: `internal/storage/shard/shard_io.go`

- [ ] **Step 1: 移除 Read 方法（shard_io.go:116-153）**

删除 `func (s *Shard) Read(...)` 整个方法。

- [ ] **Step 2: 移除 readFromSSTable 方法（shard_io.go:155-230）**

删除 `func (s *Shard) readFromSSTable(...)` 整个方法。

- [ ] **Step 3: 移除 readSSTableFile 方法（shard_io.go:232-256）**

删除 `func (s *Shard) readSSTableFile(...)` 整个方法。

- [ ] **Step 4: 清理不再使用的 import**

移除 `shard_io.go` import 中的 `"sort"` 和 `"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"`（检查 listSSTableFiles 是否仍需这些 import）。

确认 `"sort"` 不再使用（Read 中的 sort.Slice 已移除，listSSTableFiles 不需要 sort）。sstable import 可能在 listSSTableFiles 中不需要——listSSTableFiles 只做文件扫描，不需要 sstable 包。

- [ ] **Step 5: Commit**

```bash
git add internal/storage/shard/shard_io.go
git commit -m "refactor(shard): 移除 Shard.Read 等全量加载方法，统一使用 ShardIterator"
```

---

### Task 9: 迁移 21 处 Shard.Read 调用方

**Files:**
- Modify: `internal/storage/shard/shard_test.go` (14 处)
- Modify: `internal/storage/shard/shard_extra_test.go` (5 处)
- Modify: `internal/storage/shard/shard_io.go` — 不移除（listSSTableFiles 已保留）
- Verify: `internal/engine/engine_query.go` — 确认不调用 Shard.Read

- [ ] **Step 1: 在 shard_test.go 顶部添加 collectAll 辅助函数**

```go
// collectAll 从 ShardIterator 收集所有结果，模拟旧的 Shard.Read 行为。
func collectAll(si *ShardIterator) []*types.PointRow {
	var rows []*types.PointRow
	for row := si.Next(); row != nil; row = si.Next() {
		rows = append(rows, row)
	}
	return rows
}
```

- [ ] **Step 2: 迁移 shard_test.go 中 14 处 Read 调用**

每处 `s.Read(start, end)` 替换为：
```go
iter := NewShardIterator(s, start, end, 0)
rows := collectAll(iter)
iter.Close()
```

以第一个调用（`shard_test.go:133`）为例：
```go
// 旧：
rows, err := s.Read(0, 20*1e9)
// 新：
iter := NewShardIterator(s, 0, 20*1e9, 0)
rows := collectAll(iter)
iter.Close()
```

**所有 14 处在 shard_test.go 中的具体行号**：133, 288, 350, 407, 482, 600, 638, 719, 757, 813, 862, 926, 969（需确认最后一个）

注意：813 行是 `rows, err := s.Read(0, math.MaxInt64)` — 旧代码有 err 返回值，新代码无 err。ShardIterator 的错误通过 `si.Err()` 获取，在 collectAll 之后检查：
```go
iter := NewShardIterator(s, 0, math.MaxInt64, 0)
rows := collectAll(iter)
if err := iter.Err(); err != nil {
    t.Fatalf("iterator error: %v", err)
}
iter.Close()
```

862 行是并发测试中的 Read 调用，保持 goroutine 内模式不变，只替换 API。

- [ ] **Step 3: 迁移 shard_extra_test.go 中 5 处 Read 调用**

同样模式替换行 442, 468, 613, 811, 988, 1242, 1315 (7 处)。

- [ ] **Step 4: 迁移 write_and_compact/main.go 中的 Read 调用**

```go
// 旧：
rows, err := s.Read(baseTime, baseTime+int64(pointsPerFlush)*int64(time.Millisecond))
// 新：
iter := shard.NewShardIterator(s, baseTime, baseTime+int64(pointsPerFlush)*int64(time.Millisecond), 0)
defer iter.Close()
var rows []*types.PointRow
for row := iter.Next(); row != nil; row = iter.Next() {
    rows = append(rows, row)
}
if err := iter.Err(); err != nil {
    // handle error
}
```

需要在该文件 import 中添加 `"codeberg.org/micro-ts/mts/internal/storage/shard"`。

- [ ] **Step 5: 确认 engine_query.go 无 Shard.Read 调用**

```bash
grep -n '\.Read(' internal/engine/engine_query.go
```

Expected: no matches（生产路径已走 QueryIterator）

- [ ] **Step 6: 运行全部 shard 测试**

```bash
go test ./internal/storage/shard/ -v -count=1
```

Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add internal/storage/shard/shard_test.go internal/storage/shard/shard_extra_test.go tests/e2e/write_and_compact/main.go
git commit -m "test: 迁移所有 Shard.Read 调用方到 ShardIterator 流式 API"
```

---

### Task 10: 新增 ShardIterator 流式验证测试

**Files:**
- Modify: `internal/storage/shard/iterator_test.go`（如有）或 `internal/storage/shard/shard_extra_test.go`

- [ ] **Step 1: 新增 TestShardIterator_Streaming 测试**

在 `shard_extra_test.go` 末尾添加：

```go
func TestShardIterator_Streaming(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultTestShardConfig(dir)
	cfg.MemTableCfg.MaxCount = 100
	s := NewShard(cfg, slog.Default())
	defer s.Close()

	// 写入 200 个数据点，触发至少一次 flush 生成 SSTable
	baseTime := time.Now().UnixNano()
	for i := 0; i < 200; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		p := &types.Point{
			Database:    "db",
			Measurement: "m",
			Tags:        map[string]string{"host": "s1"},
			Timestamp:   ts,
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(float64(i)),
			},
		}
		if err := s.Write(p); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	// 等待 flush 完成
	time.Sleep(100 * time.Millisecond)

	// 通过 ShardIterator 查询（流式模式）
	iter := NewShardIterator(s, baseTime, baseTime+int64(500)*int64(time.Second), 0)
	defer iter.Close()

	count := 0
	var prev int64
	for row := iter.Next(); row != nil; row = iter.Next() {
		if row.Timestamp < prev {
			t.Errorf("not sorted: %d < %d at pos %d", row.Timestamp, prev, count)
		}
		prev = row.Timestamp
		count++
	}
	if count != 200 {
		t.Errorf("expected 200 rows, got %d", count)
	}
}
```

- [ ] **Step 2: 运行测试**

```bash
go test ./internal/storage/shard/ -run TestShardIterator_Streaming -v -count=1
```

Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/shard_extra_test.go
git commit -m "test(shard): 新增 ShardIterator 流式归并端到端测试"
```

---

### Task 11: 最终验证

**Files:** 无新增，运行全量检查

- [ ] **Step 1: golangci-lint**

```bash
golangci-lint run ./internal/storage/shard/... ./internal/storage/memtable/... ./internal/storage/shard/sstable/...
```

Expected: 0 issues

- [ ] **Step 2: goimports-reviser 格式化**

```bash
goimports-reviser -format -rm-unused ./internal/storage/shard/ ./internal/storage/memtable/ ./internal/storage/shard/sstable/
```

- [ ] **Step 3: 运行全部 shard 单元测试 + 覆盖率**

```bash
go test ./internal/storage/shard/... -cover -count=1
go test ./internal/storage/memtable/ -cover -count=1
```

Expected: ALL PASS, 覆盖率 ≥ 90%

- [ ] **Step 4: 运行全部 e2e 测试**

```bash
cd tests/e2e/integrity && go build && ./integrity
cd ../simple_integrity && go build && ./simple_integrity
cd ../write_1k && go build && ./write_1k
# ... 全部 e2e 测试
```

Expected: ALL PASS

- [ ] **Step 5: 清理 e2e 构建产物**

```bash
find tests/e2e -type f -executable -name "main" -delete 2>/dev/null
```

- [ ] **Step 6: 最终 Commit**

```bash
git add -A
git commit -m "chore: golangci-lint + goimports-reviser 格式化"
```
