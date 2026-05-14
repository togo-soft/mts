# P2 优化项实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 WAL checkpoint 机制、Metrics 指标预埋、Compaction 配置动态调整、文件权限合规 4 项 P2 优化。

**Architecture:** 四项独立优化，互不依赖，可并行实施。P2-1 在异步 flush Phase 3 后记录 checkpoint，重启时跳过已持久化 segment；P2-2 用 expvar 标准库定义指标变量并在关键路径埋点，不暴露 HTTP；P2-3 在 CompactionManager/LevelCompactionManager 添加 SetConfig 方法并通过 Engine→ShardManager 链路暴露；P2-4 新增 SafeWriteFile + SafeMkdirAll 包装函数并替换所有不安全调用。

**Tech Stack:** Go 1.21+, expvar (标准库), 现有 WAL/SSTable/Compaction 基础设施

---

## 文件结构

```
internal/storage/wal/
├── wal.go              (MODIFY) - Replay 方法支持 checkpoint 跳过
├── checkpoint.go       (CREATE) - WAL checkpoint 结构体与读写
├── wal_test.go         (MODIFY) - checkpoint 测试

internal/metrics/
├── metrics.go          (CREATE) - expvar 指标定义与注册
├── metrics_test.go     (CREATE) - 指标功能测试

internal/storage/shard/
├── shard_flush.go      (MODIFY) - Phase 3 后写 checkpoint
├── shard.go            (MODIFY) - ReplayWAL 传递 checkpoint
├── manager.go          (MODIFY) - 暴露 SetCompactionConfig 方法
├── shard_io.go         (MODIFY) - Write/WriteBatch 埋点
├── shard_extra_test.go (MODIFY) - checkpoint/metrics 集成测试

internal/storage/compaction/
├── compaction.go       (MODIFY) - SetConfig 方法 + SafeCreate 替换 + ticker 重置
├── level.go            (MODIFY) - SetConfig 方法 + ticker 重置
├── level_manifest.go   (MODIFY) - Save 方法使用 SafeWriteFile

internal/engine/
├── engine.go           (MODIFY) - 暴露 SetCompactionConfig 方法
├── engine_write.go     (MODIFY) - Write/WriteBatch 埋点
├── engine_query.go     (MODIFY) - Query 埋点

internal/storage/
├── util.go             (MODIFY) - 新增 SafeWriteFile
├── util_test.go        (MODIFY) - SafeWriteFile 测试

types/
├── compaction_config.go (MODIFY) - 添加 CompactionConfigSetter 接口（如需要）
```

---

### Task 1: WAL Checkpoint 数据结构与读写

**Files:**
- Create: `internal/storage/wal/checkpoint.go`
- Create: `internal/storage/wal/checkpoint_test.go`

- [ ] **Step 1: 创建 checkpoint.go**

```go
package wal

import (
	"encoding/json"
	"os"
	"path/filepath"
)

const checkpointFileName = "wal_checkpoint"

// Checkpoint 记录已持久化到 SSTable 的 WAL 位置。
// Async flush Phase 3 完成后写入，重启时跳过对应 segment。
type Checkpoint struct {
	Generation uint64 `json:"generation"`
	Segment    uint64 `json:"segment"`
}

// CheckpointPath 返回 checkpoint 文件路径。
func CheckpointPath(walDir string) string {
	return filepath.Join(walDir, checkpointFileName)
}

// Save 写入 checkpoint 到 WAL 目录。
func (cp *Checkpoint) Save(walDir string) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	path := CheckpointPath(walDir)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// LoadCheckpoint 从 WAL 目录加载 checkpoint。
// 文件不存在时返回 nil, nil。
func LoadCheckpoint(walDir string) (*Checkpoint, error) {
	path := CheckpointPath(walDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

// ClearCheckpoint 删除 checkpoint 文件（TruncateAfterFlush 调用后，所有数据已持久化）。
func ClearCheckpoint(walDir string) error {
	path := CheckpointPath(walDir)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
```

- [ ] **Step 2: 编写 checkpoint 测试**

```go
// internal/storage/wal/checkpoint_test.go
package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpoint_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	cp := &Checkpoint{Generation: 42, Segment: 7}
	if err := cp.Save(dir); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadCheckpoint(dir)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Generation != 42 || loaded.Segment != 7 {
		t.Errorf("expected gen=42 seg=7, got gen=%d seg=%d", loaded.Generation, loaded.Segment)
	}
}

func TestCheckpoint_LoadNotExist(t *testing.T) {
	dir := t.TempDir()
	cp, err := LoadCheckpoint(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cp != nil {
		t.Error("expected nil for nonexistent checkpoint")
	}
}

func TestCheckpoint_ClearCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cp := &Checkpoint{Generation: 1, Segment: 1}
	if err := cp.Save(dir); err != nil {
		t.Fatal(err)
	}
	if err := ClearCheckpoint(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(CheckpointPath(dir)); !os.IsNotExist(err) {
		t.Error("checkpoint file should be removed")
	}
}

func TestCheckpoint_ClearNonExistent(t *testing.T) {
	dir := t.TempDir()
	if err := ClearCheckpoint(dir); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpoint_SaveOverwrite(t *testing.T) {
	dir := t.TempDir()
	cp1 := &Checkpoint{Generation: 1, Segment: 3}
	cp2 := &Checkpoint{Generation: 2, Segment: 5}
	_ = cp1.Save(dir)
	_ = cp2.Save(dir)
	loaded, _ := LoadCheckpoint(dir)
	if loaded.Generation != 2 || loaded.Segment != 5 {
		t.Errorf("expected overwritten gen=2 seg=5, got gen=%d seg=%d", loaded.Generation, loaded.Segment)
	}
}

func TestCheckpointPath(t *testing.T) {
	p := CheckpointPath("/data/wal")
	expected := filepath.Join("/data/wal", checkpointFileName)
	if p != expected {
		t.Errorf("expected %s, got %s", expected, p)
	}
}
```

- [ ] **Step 3: 运行测试验证通过**

```bash
go test ./internal/storage/wal/ -run TestCheckpoint -v -count=1
```

- [ ] **Step 4: Commit**

```bash
git add internal/storage/wal/checkpoint.go internal/storage/wal/checkpoint_test.go
git commit -m "feat(wal): 添加 wal checkpoint 数据结构与读写方法"
```

---

### Task 2: WAL Replay 集成 checkpoint 跳过逻辑

**Files:**
- Modify: `internal/storage/wal/wal.go:439-473` (Replay 方法)
- Modify: `internal/storage/wal/wal_test.go` (添加 checkpoint skip 测试)

- [ ] **Step 1: 修改 Replay 方法支持 checkpoint 跳过**

`wal.go` 中 `Replay` 方法修改为：

```go
// Replay 流式回放 WAL segment。
// 如果存在 checkpoint，跳过已持久化的 segment（generation 匹配且 segNum <= checkpoint segment）。
// 如果 generation 不匹配（gen 变更），忽略旧 checkpoint 并从第一个未跳过 segment 开始。
func (w *WAL) Replay(fn func(payload []byte) error) error {
	entries, err := listSegments(w.dir)
	if err != nil {
		return err
	}

	// 加载 checkpoint，跳过已完成 flush 的旧 segment
	cp, _ := LoadCheckpoint(w.dir)

	// 跳过已持久化的 segment
	var toReplay []segmentEntry
	if cp != nil {
		w.cfg.Logger.Info("WAL checkpoint found, skipping persisted segments",
			"checkpoint_gen", cp.Generation,
			"checkpoint_seg", cp.Segment)
		for _, e := range entries {
			// 跳过 generation 匹配且 segNum <= checkpoint segment 的 segment
			if e.Gen == cp.Generation && e.Num <= cp.Segment {
				continue
			}
			toReplay = append(toReplay, e)
		}
	} else {
		toReplay = entries
	}

	w.replayedSegs = len(toReplay)

	for _, e := range toReplay {
		// ... 原有 replay 逻辑不变 ...
		file, err := os.Open(e.Path)
		if err != nil {
			w.cfg.Logger.Warn("failed to open WAL segment for replay", "path", e.Path, "error", err)
			continue
		}
		if _, err := file.Seek(0, 0); err != nil {
			_ = file.Close()
			return err
		}
		_, _, compressed, err := readSegmentHeader(file)
		if err != nil {
			_ = file.Close()
			w.cfg.Logger.Warn("failed to read WAL segment header", "path", e.Path, "error", err)
			continue
		}
		_, err = readRecords(file, int64(segmentHeaderSize), fn, compressed)
		_ = file.Close()
		if err != nil {
			w.cfg.Logger.Warn("WAL replay encountered error", "path", e.Path, "error", err)
		}
	}
	return nil
}
```

- [ ] **Step 2: 修改 TruncateAfterFlush 清理 checkpoint**

`TruncateAfterFlush` 删除所有旧 segment 后，数据全部持久化到 SSTable，checkpoint 也应清理：

```go
func (w *WAL) TruncateAfterFlush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed.Load() {
		return nil
	}

	if err := w.rotateLocked(); err != nil {
		return err
	}

	entries, err := listSegments(w.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Gen == w.gen && e.Num == w.segNum {
			continue
		}
		if rmErr := os.Remove(e.Path); rmErr != nil {
			w.cfg.Logger.Warn("failed to remove old WAL segment", "path", e.Path, "error", rmErr)
		}
	}

	// 清理旧 checkpoint，sync flush 后所有数据已持久化
	_ = ClearCheckpoint(w.dir)

	return nil
}
```

- [ ] **Step 3: 编写 Replay checkpoint skip 集成测试**

在 `wal_test.go` 中添加：

```go
func TestReplay_WithCheckpoint_SkipsPersistedSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, SegmentSize: 1024, SyncMode: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// 写入 seg1 的数据
	_, _ = w.Write([]byte("seg1-data"))
	w.Rotate()
	// 写入 seg2 的数据
	_, _ = w.Write([]byte("seg2-data"))
	w.Rotate()
	// 写入 seg3 的数据
	_, _ = w.Write([]byte("seg3-data"))

	// 记录 checkpoint：seg2 已持久化
	cp := &Checkpoint{Generation: w.Generation(), Segment: 2}
	if err := cp.Save(dir); err != nil {
		t.Fatal(err)
	}

	w.Close()

	// 重新打开，replay 应跳过 seg1 和 seg2
	w2, err := Open(Config{Dir: dir, SegmentSize: 1024, SyncMode: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	var replayed []string
	err = w2.Replay(func(data []byte) error {
		replayed = append(replayed, string(data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 {
		t.Errorf("expected 1 replayed entry, got %d: %v", len(replayed), replayed)
	}
	if len(replayed) > 0 && replayed[0] != "seg3-data" {
		t.Errorf("expected seg3-data, got %s", replayed[0])
	}
}

func TestReplay_NoCheckpoint_ReplaysAll(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, SegmentSize: 1024, SyncMode: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("data1"))
	_, _ = w.Write([]byte("data2"))
	w.Close()

	w2, _ := Open(Config{Dir: dir, SegmentSize: 1024, SyncMode: SyncNone})
	defer w2.Close()

	var replayed []string
	_ = w2.Replay(func(data []byte) error {
		replayed = append(replayed, string(data))
		return nil
	})
	if len(replayed) != 2 {
		t.Errorf("expected 2 replayed entries, got %d", len(replayed))
	}
}
```

- [ ] **Step 4: 运行测试**

```bash
go test ./internal/storage/wal/ -run 'TestReplay_WithCheckpoint|TestReplay_NoCheckpoint' -v -count=1
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/wal/wal.go internal/storage/wal/wal_test.go
git commit -m "feat(wal): replay 集成 checkpoint 跳过已持久化 segment"
```

---

### Task 3: Async Flush Phase 3 写 checkpoint

**Files:**
- Modify: `internal/storage/shard/shard_flush.go:229-270` (executeAsyncFlush Phase 3)
- Modify: `internal/storage/shard/shard.go:557-603` (ReplayWAL)
- Modify: `internal/storage/shard/shard_extra_test.go` (集成测试)

- [ ] **Step 1: Phase 3 后写入 checkpoint**

在 `executeAsyncFlush` Phase 3 的 WAL truncate 后写入 checkpoint：

```go
// Phase 3: 持锁 → ClearPassive → 原子 rename → 注册 → WAL 清理
s.mu.Lock()

s.memTable.ClearPassive()

if err := os.Rename(info.tmpPath, info.finalPath); err != nil {
	s.mu.Unlock()
	slog.Error("async flush: rename tmp to final failed", "tmp", info.tmpPath, "final", info.finalPath, "error", err)
	_ = os.Remove(info.tmpPath)
	s.triggerBackgroundCompaction()
	return
}

if info.useLevelCompaction {
	var size int64
	if fi, statErr := os.Stat(info.finalPath); statErr == nil {
		size = fi.Size()
	}
	s.levelCompaction.AddPart(0, compaction.PartInfo{
		Name:    fmt.Sprintf("sst_%d", info.sstSeq),
		Size:    size,
		MinTime: info.minTime,
		MaxTime: info.maxTime,
	})
}

if s.wal != nil {
	if walErr := s.wal.TruncateCurrent(); walErr != nil {
		slog.Warn("failed to truncate WAL after async flush", "error", walErr)
	}
	// 写入 checkpoint，记录当前已持久化的 WAL 位置
	cp := &wal.Checkpoint{
		Generation: s.wal.Generation(),
		Segment:    s.wal.SegmentNum(),
	}
	if cpErr := cp.Save(filepath.Join(s.dir, "wal")); cpErr != nil {
		slog.Warn("failed to save WAL checkpoint", "error", cpErr)
	}
}

s.mu.Unlock()
```

需要在 `shard_flush.go` 顶部添加 `wal` 包引用：
```go
import (
	// ... existing imports ...
	"codeberg.org/micro-ts/mts/internal/storage/wal"
)
```

同时修改 `flushLocked` 中的 `TruncateAfterFlush` 调用后也写入 checkpoint：

```go
// WAL 清理（replay 期间跳过）
if !s.replaying && s.wal != nil {
	if err := s.wal.TruncateAfterFlush(); err != nil {
		slog.Warn("failed to truncate WAL after flush", "error", err)
	}
	// 全量 flush 后 checkpoint 已由 TruncateAfterFlush 内部清理
}
```

- [ ] **Step 2: 添加集成测试**

在 `shard_extra_test.go` 中添加：

```go
func TestAsyncFlush_WritesCheckpoint(t *testing.T) {
	dir := t.TempDir()
	// 创建带 WAL 的 shard
	cfg := newTestShardConfig(dir)
	cfg.CompactionCfg = nil
	s := NewShard(cfg)
	defer s.Close()

	// 写入足够数据触发 async flush
	for i := 0; i < 2000; i++ {
		_ = s.Write(&types.Point{
			Tags:      map[string]string{"host": "a"},
			Timestamp: time.Now().UnixNano() + int64(i),
			Fields:    map[string]*types.FieldValue{"v": {Value: &types.FieldValue_FloatValue{FloatValue: float64(i)}}},
		})
	}

	// 等待 async flush 完成
	time.Sleep(500 * time.Millisecond)

	// 验证 checkpoint 文件存在
	cp, err := wal.LoadCheckpoint(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil {
		t.Fatal("expected checkpoint to exist after async flush")
	}
	if cp.Generation == 0 {
		t.Error("expected non-zero generation in checkpoint")
	}
}
```

- [ ] **Step 3: 运行测试验证**

```bash
go test ./internal/storage/shard/ -run TestAsyncFlush_WritesCheckpoint -v -count=1 -timeout=30s
go test ./internal/storage/wal/ -v -count=1
```

- [ ] **Step 4: Commit**

```bash
git add internal/storage/shard/shard_flush.go internal/storage/shard/shard_extra_test.go
git commit -m "feat(shard): async flush phase 3 后写入 wal checkpoint"
```

---

### Task 4: 创建 internal/metrics/ 包

**Files:**
- Create: `internal/metrics/metrics.go`
- Create: `internal/metrics/metrics_test.go`

- [ ] **Step 1: 创建 metrics.go 定义 expvar 指标**

```go
// Package metrics 提供 expvar 指标定义与埋点。
// 不暴露 HTTP 端口，仅通过 expvar 标准库提供指标数据。
// 外部可通过 expvar.Handler 或自定义 HTTP handler 按需暴露（需显式启动）。
package metrics

import "expvar"

// 计数器维度名
const (
	DimWrite      = "write"
	DimFlush      = "flush"
	DimCompaction = "compaction"
	DimQuery      = "query"
	DimMemTable   = "memtable"
	DimWAL        = "wal"
)

// Write 子系统指标
var (
	WriteTotal   = expvar.NewInt("write_total")
	WriteBytes   = expvar.NewInt("write_bytes")
	WriteErrors  = expvar.NewInt("write_errors")
	WriteBatchTotal = expvar.NewInt("write_batch_total")
)

// Flush 子系统指标
var (
	FlushTotal    = expvar.NewInt("flush_total")
	FlushPoints   = expvar.NewInt("flush_points")
	FlushErrors   = expvar.NewInt("flush_errors")
	FlushDuration = expvar.NewInt("flush_duration_ms")
)

// Compaction 子系统指标
var (
	CompactionTotal     = expvar.NewInt("compaction_total")
	CompactionInputFiles  = expvar.NewInt("compaction_input_files")
	CompactionOutputCount = expvar.NewInt("compaction_output_count")
	CompactionDupCount    = expvar.NewInt("compaction_dup_count")
	CompactionErrors      = expvar.NewInt("compaction_errors")
)

// Query 子系统指标
var (
	QueryTotal     = expvar.NewInt("query_total")
	QueryPoints    = expvar.NewInt("query_points")
	QueryErrors    = expvar.NewInt("query_errors")
	QueryDuration  = expvar.NewInt("query_duration_ms")
)

// MemTable 子系统指标
var (
	MemTableActiveCount = expvar.NewInt("memtable_active_count")
	MemTableSwapTotal   = expvar.NewInt("memtable_swap_total")
)

// WAL 子系统指标
var (
	WALWriteTotal   = expvar.NewInt("wal_write_total")
	WALWriteBytes   = expvar.NewInt("wal_write_bytes")
	WALReplayTotal  = expvar.NewInt("wal_replay_total")
	WALRotateTotal  = expvar.NewInt("wal_rotate_total")
)

// Gauge 快照类型指标（Map 存储）
var (
	MemTableGauge = expvar.NewMap("memtable")
	WALGauge      = expvar.NewMap("wal")
	ShardGauge    = expvar.NewMap("shard")
)

// Incr 安全递增 expvar.Int，nil 安全。
func Incr(v *expvar.Int, delta int64) {
	if v != nil {
		v.Add(delta)
	}
}
```

- [ ] **Step 2: 编写 metrics 测试**

```go
// internal/metrics/metrics_test.go
package metrics

import (
	"testing"
)

func TestMetrics_WriteCounters(t *testing.T) {
	WriteTotal.Add(1)
	WriteBytes.Add(100)
	WriteErrors.Add(0)

	if WriteTotal.String() != "1" {
		t.Errorf("WriteTotal = %s, want 1", WriteTotal.String())
	}
	if WriteBytes.String() != "100" {
		t.Errorf("WriteBytes = %s, want 100", WriteBytes.String())
	}
}

func TestMetrics_FlushCounters(t *testing.T) {
	FlushTotal.Add(1)
	FlushPoints.Add(500)

	if FlushTotal.String() != "1" {
		t.Errorf("FlushTotal = %s, want 1", FlushTotal.String())
	}
}

func TestMetrics_CompactionCounters(t *testing.T) {
	CompactionTotal.Add(1)
	CompactionOutputCount.Add(1000)
	CompactionDupCount.Add(50)

	if CompactionTotal.String() != "1" {
		t.Errorf("CompactionTotal = %s, want 1", CompactionTotal.String())
	}
}

func TestMetrics_QueryCounters(t *testing.T) {
	QueryTotal.Add(1)
	QueryPoints.Add(200)

	if QueryTotal.String() != "1" {
		t.Errorf("QueryTotal = %s, want 1", QueryTotal.String())
	}
}

func TestMetrics_MemTableCounters(t *testing.T) {
	MemTableActiveCount.Add(1500)
	MemTableSwapTotal.Add(10)

	if MemTableActiveCount.String() != "1500" {
		t.Errorf("MemTableActiveCount = %s, want 1500", MemTableActiveCount.String())
	}
}

func TestMetrics_WALCounters(t *testing.T) {
	WALWriteTotal.Add(1)
	WALWriteBytes.Add(500)

	if WALWriteTotal.String() != "1" {
		t.Errorf("WALWriteTotal = %s, want 1", WALWriteTotal.String())
	}
}

func TestMetrics_GaugeMaps(t *testing.T) {
	MemTableGauge.Add("active_count", 1000)
	WALGauge.Add("gen", 42)
	ShardGauge.Add("sst_count", 5)

	if MemTableGauge.Get("active_count").String() != "1000" {
		t.Errorf("expected 1000, got %s", MemTableGauge.Get("active_count").String())
	}
}

func TestIncr_NilSafe(t *testing.T) {
	Incr(nil, 10) // 不应 panic
}
```

- [ ] **Step 3: 运行测试**

```bash
go test ./internal/metrics/ -v -count=1
```

- [ ] **Step 4: Commit**

```bash
git add internal/metrics/
git commit -m "feat(metrics): 创建 expvar 指标定义包，预埋 Write/Flush/Compaction/Query/MemTable/WAL 计数器"
```

---

### Task 5: 关键路径埋点 — Write/Flush

**Files:**
- Modify: `internal/storage/shard/shard_io.go` (Write/WriteBatch 埋点)
- Modify: `internal/storage/shard/shard_flush.go` (异步 flush 埋点)
- Modify: `internal/storage/shard/shard.go` (ReplayWAL 埋点)

- [ ] **Step 1: Write/WriteBatch 埋点**

在 `shard_io.go` 的 `Write` 方法中：

```go
import (
	// ... existing ...
	"codeberg.org/micro-ts/mts/internal/metrics"
)

func (s *Shard) Write(point *types.Point) error {
	// ... backpressure ...

	s.mu.Lock()
	// ... alloc sid, validate, serialize, wal write, memtable write ...

	if err != nil {
		s.mu.Unlock()
		metrics.Incr(metrics.WriteErrors, 1)
		return err
	}

	metrics.Incr(metrics.WriteTotal, 1)
	// ... unlock, shouldFlush ...
}
```

在 `WriteBatch` 方法成功后：

```go
	metrics.Incr(metrics.WriteBatchTotal, 1)
	metrics.Incr(metrics.WriteTotal, int64(len(ips)))
```

- [ ] **Step 2: Flush 埋点**

在 `shard_flush.go` 的 `executeAsyncFlush` Phase 3 成功完成后：

```go
	metrics.Incr(metrics.FlushTotal, 1)
	metrics.Incr(metrics.FlushPoints, int64(len(passive)))
```

在 `writeSSTableSync` 成功写入后（flushLocked 调用链）：

```go
	metrics.Incr(metrics.FlushTotal, 1)
	metrics.Incr(metrics.FlushPoints, int64(len(points)))
```

- [ ] **Step 3: Compilation 验证**

```bash
go build ./internal/storage/shard/
go vet ./internal/storage/shard/
```

- [ ] **Step 4: Commit**

```bash
git add internal/storage/shard/shard_io.go internal/storage/shard/shard_flush.go
git commit -m "feat(metrics): Write/WriteBatch/Flush 关键路径埋点"
```

---

### Task 6: 关键路径埋点 — Compaction/Query/MemTable/WAL

**Files:**
- Modify: `internal/storage/compaction/compaction.go` (Compact 埋点)
- Modify: `internal/storage/compaction/level.go` (Level Compact 埋点)
- Modify: `internal/engine/engine_query.go` (Query 埋点)
- Modify: `internal/storage/memtable/memtable.go` (Swap 埋点)

- [ ] **Step 1: Compaction 埋点**

在 `compaction.go` 的 `Compact` 方法成功后：

```go
import "codeberg.org/micro-ts/mts/internal/metrics"

// Compact 成功后:
metrics.Incr(metrics.CompactionTotal, 1)
metrics.Incr(metrics.CompactionInputFiles, int64(len(task.MergedFiles)))
metrics.Incr(metrics.CompactionOutputCount, int64(task.OutputCount))
metrics.Incr(metrics.CompactionDupCount, int64(task.DuplicateCount))
```

Level compaction 合并成功后也同样添加（`level.go` line 282 附近）。

- [ ] **Step 2: Query 埋点**

在 `engine_query.go` 的 Query 方法中：

```go
metrics.Incr(metrics.QueryTotal, 1)
// ... 执行查询 ...
metrics.Incr(metrics.QueryPoints, int64(pointCount))
```

- [ ] **Step 3: MemTable Swap 埋点**

在 `memtable.go` 的 `Swap` 方法被调用时：

```go
metrics.Incr(metrics.MemTableSwapTotal, 1)
metrics.MemTableActiveCount.Add(int64(m.activeCount))
```

- [ ] **Step 4: WAL 埋点**

在 `wal.go` 的 `Write`/`WriteBatch` 成功后埋点（注意避免循环依赖，通过调用方埋点）：

WAL 埋点在 shard_io.go 中已经覆盖（Write → WAL Write），这里补充 `Replay` 路径：

在 `shard.go` 的 `ReplayWAL` 中：

```go
metrics.Incr(metrics.WALReplayTotal, 1)
```

- [ ] **Step 5: Compilation 验证**

```bash
go build ./...
go vet ./...
```

- [ ] **Step 6: Commit**

```bash
git add internal/storage/compaction/compaction.go internal/storage/compaction/level.go internal/engine/engine_query.go internal/storage/memtable/memtable.go internal/storage/shard/shard.go
git commit -m "feat(metrics): Compaction/Query/MemTable/WAL 关键路径埋点"
```

---

### Task 7: CompactionManager SetConfig 方法

**Files:**
- Modify: `internal/storage/compaction/compaction.go` (添加 SetConfig)
- Modify: `internal/storage/compaction/compaction_test.go` (SetConfig 测试)

- [ ] **Step 1: 添加 SetConfig 方法**

在 `compaction.go` 中 `CompactionManager` 结构体后添加：

```go
// SetConfig 运行时更新 Compaction 配置。
// 更新后自动重置 ticker 以使用新的 CheckInterval。
func (cm *CompactionManager) SetConfig(config *CompactionConfig) {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	if config == nil {
		return
	}

	cm.Config = config

	// 重置定时器以应用新的 CheckInterval
	if cm.Ticker != nil && config.CheckInterval > 0 {
		cm.Ticker.Reset(config.CheckInterval)
	}
}
```

- [ ] **Step 2: 编写 SetConfig 测试**

在 `compaction_test.go` 中添加：

```go
func TestCompactionManager_SetConfig(t *testing.T) {
	cm := NewCompactionManager(nil, DefaultCompactionConfig())

	// 修改配置
	newCfg := &CompactionConfig{
		MaxSSTableCount:    8,
		MaxCompactionBatch: 20,
		ShardSizeLimit:     2 * 1024 * 1024 * 1024,
		CheckInterval:      30 * time.Minute,
		Timeout:            15 * time.Minute,
	}
	cm.SetConfig(newCfg)

	if cm.Config.MaxSSTableCount != 8 {
		t.Errorf("MaxSSTableCount = %d, want 8", cm.Config.MaxSSTableCount)
	}
	if cm.Config.MaxCompactionBatch != 20 {
		t.Errorf("MaxCompactionBatch = %d, want 20", cm.Config.MaxCompactionBatch)
	}
	if cm.Config.ShardSizeLimit != 2*1024*1024*1024 {
		t.Errorf("ShardSizeLimit = %d, want 2GB", cm.Config.ShardSizeLimit)
	}
}

func TestCompactionManager_SetConfig_NilConfig(t *testing.T) {
	cfg := DefaultCompactionConfig()
	cm := NewCompactionManager(nil, cfg)
	cm.SetConfig(nil)
	if cm.Config != cfg {
		t.Error("config should not change on nil input")
	}
}
```

- [ ] **Step 3: 运行测试**

```bash
go test ./internal/storage/compaction/ -run TestCompactionManager_SetConfig -v -count=1
```

- [ ] **Step 4: Commit**

```bash
git add internal/storage/compaction/compaction.go internal/storage/compaction/compaction_test.go
git commit -m "feat(compaction): 添加 CompactionManager.SetConfig 运行时配置更新方法"
```

---

### Task 8: LevelCompactionManager SetConfig 方法 + Engine/ShardManager 链路

**Files:**
- Modify: `internal/storage/compaction/level.go` (添加 SetConfig)
- Modify: `internal/storage/shard/manager.go` (添加 SetCompactionConfig)
- Modify: `internal/engine/engine.go` (添加 SetCompactionConfig)
- Test: 相关测试文件

- [ ] **Step 1: 添加 LevelCompactionManager.SetConfig**

在 `level.go` 中：

```go
// SetConfig 运行时更新 Level Compaction 配置。
// 更新后自动重置 ticker 以使用新的 CheckInterval。
func (lcm *LevelCompactionManager) SetConfig(config *LevelCompactionConfig) {
	lcm.manifestMu.Lock()
	defer lcm.manifestMu.Unlock()

	if config == nil {
		return
	}

	lcm.config = config

	// 重置定时器
	if lcm.ticker != nil && config.CheckInterval > 0 {
		lcm.ticker.Reset(config.CheckInterval)
	}
}
```

- [ ] **Step 2: 添加 SetConfig 测试**

在 `level_compaction_test.go` 中添加：

```go
func TestLevelCompactionManager_SetConfig(t *testing.T) {
	lcm := &LevelCompactionManager{
		config:  DefaultLevelCompactionConfig(),
		stopCh:  make(chan struct{}),
	}

	newCfg := &LevelCompactionConfig{
		Enabled:             true,
		LevelConfigs:        DefaultLevelConfigs(),
		L0ToL1SizeThreshold: 10 * 1024 * 1024,
		MaxCompactionParts:  5,
		TombstoneRetention:  30 * time.Minute,
		CheckInterval:       10 * time.Minute,
		Timeout:             15 * time.Minute,
	}
	lcm.SetConfig(newCfg)

	if lcm.config.L0ToL1SizeThreshold != 10*1024*1024 {
		t.Errorf("L0ToL1SizeThreshold = %d, want 10MB", lcm.config.L0ToL1SizeThreshold)
	}
	if lcm.config.MaxCompactionParts != 5 {
		t.Errorf("MaxCompactionParts = %d, want 5", lcm.config.MaxCompactionParts)
	}
}
```

- [ ] **Step 3: 添加 ShardManager.SetCompactionConfig**

在 `manager.go` 中：

```go
// SetCompactionConfig 更新所有现有 Shard 的 Compaction 配置。
// 新创建的 Shard 不受影响（仍使用原始配置）。
func (m *ShardManager) SetCompactionConfig(config *compaction.CompactionConfig) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, s := range m.shards {
		if s.compaction != nil {
			s.compaction.SetConfig(config)
		}
	}
}
```

- [ ] **Step 4: 添加 Engine.SetCompactionConfig**

在 `engine.go` 中：

```go
// SetCompactionConfig 运行时更新所有 Shard 的 Compaction 配置。
func (e *Engine) SetCompactionConfig(config *compaction.CompactionConfig) {
	e.shardManager.SetCompactionConfig(config)
}
```

- [ ] **Step 5: 验证编译**

```bash
go build ./...
go vet ./...
```

- [ ] **Step 6: Commit**

```bash
git add internal/storage/compaction/level.go internal/storage/shard/manager.go internal/engine/engine.go
git commit -m "feat(compaction): 添加 LevelCompactionManager.SetConfig 及 Engine→ShardManager 配置链路"
```

---

### Task 9: 新增 SafeWriteFile + SafeMkdirAll (storage 包内) 并替换不安全调用

**Files:**
- Modify: `internal/storage/util.go` (新增 SafeWriteFile)
- Modify: `internal/storage/util_test.go` (SafeWriteFile 测试)
- Modify: `internal/storage/compaction/compaction.go:247,254` (os.Create → SafeCreate)
- Modify: `internal/storage/compaction/level.go:72` (os.WriteFile → SafeWriteFile)
- Modify: `internal/storage/compaction/level_manifest.go:216` (os.WriteFile → SafeWriteFile)
- Modify: `internal/storage/compaction/tombstone.go:95` (os.WriteFile → SafeWriteFile)
- Modify: `internal/storage/shard/shard_flush.go:68,210` (os.MkdirAll → storage.SafeMkdirAll)

- [ ] **Step 1: 在 util.go 添加 SafeWriteFile**

```go
// SafeWriteFile 安全地写入数据到文件。
//
// 参数：
//   - path: 文件路径
//   - data: 要写入的数据
//   - perm: 文件权限（通常应为 0600）
//
// 行为：
//   先写入临时文件，再原子 rename，确保不会留下部分写入的文件。
//   自动创建父目录（权限 0700）。
//
// 安全检查：
//   路径不能包含 .. 路径遍历组件。
func SafeWriteFile(path string, data []byte, perm uint32) error {
	if !isPathSafe(path) {
		return &PathError{Op: "write", Path: path, Err: ErrInvalidPath}
	}

	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, os.FileMode(perm)); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
```

- [ ] **Step 2: 编写 SafeWriteFile 测试**

在 `util_test.go` 中添加：

```go
func TestSafeWriteFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	data := []byte("hello world")

	if err := SafeWriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}

	read, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(read) != "hello world" {
		t.Errorf("got %q, want %q", string(read), "hello world")
	}

	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Errorf("expected 0600, got %o", info.Mode().Perm())
	}
}

func TestSafeWriteFile_AutoCreateParent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a", "b", "test.txt")

	if err := SafeWriteFile(path, []byte("nested"), 0600); err != nil {
		t.Fatal(err)
	}

	info, _ := os.Stat(filepath.Join(dir, "a", "b"))
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Errorf("parent dir permission = %o, want 0700", info.Mode().Perm())
	}
}

func TestSafeWriteFile_PathTraversal(t *testing.T) {
	err := SafeWriteFile("../../etc/passwd", []byte("bad"), 0600)
	if err == nil {
		t.Error("expected error for path traversal")
	}
}
```

- [ ] **Step 3: 替换 os.Create → storage.SafeCreate**

`compaction.go` 中：

```go
// MarkWriting 开始写入标记。
func (cm *CompactionManager) MarkWriting(sstPath string) error {
	writingFlag := sstPath + ".writing"
	f, err := storage.SafeCreate(writingFlag, 0600)
	if err != nil {
		return err
	}
	return f.Close()
}
```

（`storage.SafeCreate` 已自动创建父目录，移除嵌套的 `os.MkdirAll` 回退逻辑）

- [ ] **Step 4: 替换 os.WriteFile → storage.SafeWriteFile**

- `level.go:72`: `os.WriteFile(tmpPath, data, 0600)` → `storage.SafeWriteFile(tmpPath, data, 0600)`
- `level_manifest.go:216`: `os.WriteFile(tmpPath, data, 0600)` → `storage.SafeWriteFile(tmpPath, data, 0600)`
- `tombstone.go:95`: `os.WriteFile(tmpPath, data, 0600)` → `storage.SafeWriteFile(tmpPath, data, 0600)`

- [ ] **Step 5: 替换 os.MkdirAll → storage.SafeMkdirAll**

- `shard_flush.go:68`: `os.MkdirAll(l0Dir, 0700)` → `storage.SafeMkdirAll(l0Dir, 0700)`
- `shard_flush.go:210`: `os.MkdirAll(dataDir, 0700)` → `storage.SafeMkdirAll(dataDir, 0700)`

- [ ] **Step 6: 运行测试与 lint**

```bash
go test ./internal/storage/ -run TestSafeWriteFile -v -count=1
go test ./internal/storage/compaction/ -v -count=1
go test ./internal/storage/shard/ -v -count=1
golangci-lint run ./internal/storage/... ./internal/storage/compaction/... ./internal/storage/shard/...
```

- [ ] **Step 7: Commit**

```bash
git add internal/storage/util.go internal/storage/util_test.go internal/storage/compaction/compaction.go internal/storage/compaction/level.go internal/storage/compaction/level_manifest.go internal/storage/compaction/tombstone.go internal/storage/shard/shard_flush.go
git commit -m "feat(storage): 新增 SafeWriteFile 并替换所有不安全文件系统调用"
```

---

### Task 10: 补齐 compaction 包 missing storage import + 最终验证

**Files:**
- Modify: `internal/storage/compaction/level.go` (确认 storage import 存在)
- Modify: `internal/storage/compaction/level_manifest.go` (确认 storage import 存在)
- Modify: `internal/storage/compaction/tombstone.go` (确认 storage import 存在)

- [ ] **Step 1: 检查各文件 import 完整性**

检查 `level.go`, `level_manifest.go`, `tombstone.go` 是否已有 `"codeberg.org/micro-ts/mts/internal/storage"` 引用。如有缺失，添加该 import；如已有（例如 `level.go` 无但需要 `storage.SafeWriteFile`），添加 import。

`level.go` 当前 imports:
```go
import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)
```

需要添加 `"codeberg.org/micro-ts/mts/internal/storage"`。

`level_manifest.go` 当前 imports（需检查确认后补充）：
需要添加 `"codeberg.org/micro-ts/mts/internal/storage"`。

`tombstone.go` 当前 imports（需检查确认后补充）：
需要添加 `"codeberg.org/micro-ts/mts/internal/storage"`。

- [ ] **Step 2: 编译验证**

```bash
go build ./...
```

如果 `os` 包因替换后不再使用，需要移除对应 import。

- [ ] **Step 3: 运行完整测试**

```bash
go test ./internal/storage/... -count=1
go test ./internal/engine/... -count=1
go test ./internal/metrics/... -count=1
golangci-lint run ./...
```

- [ ] **Step 4: 运行 goimports-reviser 格式化**

```bash
goimports-reviser -project-name codeberg.org/micro-ts/mts -rm-unused ./internal/...
```

- [ ] **Step 5: 运行 E2E 测试**

```bash
cd tests/e2e/compaction_test && go build && ./compaction_test && cd ../../..
cd tests/e2e/wal_test && go build && ./wal_test && cd ../../..
cd tests/e2e/persistence_test && go build && ./persistence_test && cd ../../..
```

- [ ] **Step 6: 清理与最终 Commit**

```bash
# 清理 E2E 测试临时产物
rm -f tests/e2e/compaction_test/compaction_test
rm -f tests/e2e/wal_test/wal_test
rm -f tests/e2e/persistence_test/persistence_test
```

```bash
git add internal/storage/compaction/level.go internal/storage/compaction/level_manifest.go internal/storage/compaction/tombstone.go
git commit -m "fix(compaction): 补齐 compaction 包 missing storage import 并清理未使用依赖"
```
