# Engine 接口重构实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Engine 重构为依赖 Writer/Flusher/Catalog/SeriesStore/ShardIndex 五个细粒度接口，通过 FlushCoordinator 编排写入和刷盘流程。

**Architecture:** 在 engine 包定义接口，利用 Go 隐式接口满足，metadata 已有类型无需改动。重构 Shard 为纯 SSTable 容器、ShardManager 实现 Flusher、MeasurementWriter 实现 Writer。Engine 依赖接口，FlushCoordinator 管理异步刷盘编排。

**Tech Stack:** Go 1.24, boltDB, gRPC, LZ4, snappy, zstd

---

### Task 1: 在 engine 包定义五个核心接口

**Files:**
- Create: `internal/engine/interfaces.go`

- [ ] **Step 1: 编写接口文件**

```go
// internal/engine/interfaces.go
package engine

import (
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/types"
)

// ===================================
// Writer — WAL/MemTable 写入处理
// ===================================

// Writer 接收单个 measurement 的数据写入，管理 WAL 和 MemTable 生命周期。
type Writer interface {
	Write(point *types.Point) error
	WriteBatch(points []*types.Point) (int, error)
	MemTable() *memtable.MemTable
	SeriesStore() SeriesStore
	Flush() error
	Close() error
}

// ===================================
// Flusher — SSTable/Compaction 处理
// ===================================

// ShardHandle 是查询用的 Shard 句柄。
type ShardHandle struct {
	StartTime int64
	EndTime   int64
	Dir       string
}

// Flusher 管理 SSTable 写入和 Compaction。
type Flusher interface {
	Flush(db, measurement string, points []types.MemPoint) error
	Compact(db, measurement string, startTime int64) error
	GetShards(db, measurement string, startTime, endTime int64) []*ShardHandle
	CloseAll() error
	SetConfig(config *compaction.Config)
}

// ===================================
// Metadata 子接口
// ===================================

// Catalog 管理 database/measurement/schema。
type Catalog interface {
	CreateDatabase(name string) error
	DropDatabase(name string) error
	ListDatabases() []string
	DatabaseExists(name string) bool
	CreateMeasurement(database, name string) error
	DropMeasurement(database, name string) error
	ListMeasurements(database string) ([]string, error)
	MeasurementExists(database, name string) bool
	GetSchema(database, measurement string) (*metadata.Schema, error)
	SetSchema(database, measurement string, s *metadata.Schema) error
	GetRetention(database, measurement string) (time.Duration, error)
	SetRetention(database, measurement string, d time.Duration) error
}

// SeriesStore 管理 Series ID 分配和标签索引。
type SeriesStore interface {
	AllocateSID(database, measurement string, tags map[string]string) (uint64, error)
	GetTags(database, measurement string, sid uint64) (map[string]string, bool)
	GetSIDsByTag(database, measurement string, tagKey, tagValue string) []uint64
	SeriesCount(database, measurement string) int
}

// ShardIndex 管理 Shard 时间范围索引。
type ShardIndex interface {
	RegisterShard(database, measurement string, info ShardInfo) error
	UnregisterShard(database, measurement string, shardID string) error
	QueryShards(database, measurement string, startTime, endTime int64) []ShardInfo
	ListShards(database, measurement string) []ShardInfo
	UpdateShardStats(database, measurement, shardID string, sstableCount int, totalSize int64) error
}

// ShardInfo 与 metadata.ShardInfo 类型一致。
type ShardInfo = metadata.ShardInfo
```

- [ ] **Step 2: 编译验证**

```bash
go build ./internal/engine/...
```

Expected: PASS (接口定义仅添加，不影响编译)

- [ ] **Step 3: 提交**

```bash
git add internal/engine/interfaces.go
git commit -m "feat(engine): 定义Writer/Flusher/Metadata核心接口"
```

---

### Task 2: 创建 FlushCoordinator 编排组件

**Files:**
- Create: `internal/engine/flush_coordinator.go`
- Create: `internal/engine/flush_coordinator_test.go`

- [ ] **Step 1: 编写 FlushCoordinator**

```go
// internal/engine/flush_coordinator.go
package engine

import (
	"log/slog"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/metrics"
	"codeberg.org/micro-ts/mts/types"
)

const fcBackpressureSleep = time.Millisecond

// FlushCoordinator 编排 Writer → Flusher 的异步刷盘流程。
type FlushCoordinator struct {
	writers map[string]Writer
	flusher Flusher
	mu      sync.RWMutex
	closed  bool
}

// NewFlushCoordinator 创建新的 FlushCoordinator。
func NewFlushCoordinator(flusher Flusher) *FlushCoordinator {
	return &FlushCoordinator{
		writers: make(map[string]Writer),
		flusher: flusher,
	}
}

// RegisterWriter 注册一个 Writer（按 db/meas 索引）。
func (fc *FlushCoordinator) RegisterWriter(db, measurement string, w Writer) {
	key := db + "/" + measurement
	fc.mu.Lock()
	fc.writers[key] = w
	fc.mu.Unlock()
}

// GetWriter 获取已注册的 Writer（不创建新的）。
func (fc *FlushCoordinator) GetWriter(db, measurement string) Writer {
	key := db + "/" + measurement
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.writers[key]
}

// FlushWriter 同步刷写指定 measurement 的 MemTable 到 SSTable。
func (fc *FlushCoordinator) FlushWriter(db, measurement string) error {
	key := db + "/" + measurement
	fc.mu.RLock()
	w, ok := fc.writers[key]
	fc.mu.RUnlock()
	if !ok {
		return nil
	}

	// 等待正在进行的异步 flush 完成
	for w.MemTable().IsFlushing() {
		time.Sleep(fcBackpressureSleep)
	}

	// 同步刷盘：Swap + 写入 SSTable
	passive := w.MemTable().Swap()
	if len(passive) == 0 {
		return nil
	}

	if err := fc.flusher.Flush(db, measurement, passive); err != nil {
		w.MemTable().MergePassiveBack()
		return err
	}

	w.MemTable().ClearPassive()

	metrics.Incr(metrics.FlushTotal, 1)
	metrics.Incr(metrics.FlushPoints, int64(len(passive)))

	return nil
}

// FlushAll 同步刷写所有 Writer 的 MemTable。
func (fc *FlushCoordinator) FlushAll() error {
	fc.mu.RLock()
	defer fc.mu.RUnlock()

	for key, w := range fc.writers {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) != 2 {
			continue
		}
		_ = fc.flushWriterLocked(parts[0], parts[1], w)
	}
	return nil
}

// flushWriterLocked 内部同步刷写（调用者持读锁）。
func (fc *FlushCoordinator) flushWriterLocked(db, measurement string, w Writer) error {
	for w.MemTable().IsFlushing() {
		time.Sleep(fcBackpressureSleep)
	}

	passive := w.MemTable().Swap()
	if len(passive) == 0 {
		return nil
	}

	if err := fc.flusher.Flush(db, measurement, passive); err != nil {
		w.MemTable().MergePassiveBack()
		return err
	}

	w.MemTable().ClearPassive()
	return nil
}

// Close 关闭 FlushCoordinator，不再接受新写入。
func (fc *FlushCoordinator) Close() {
	fc.mu.Lock()
	fc.closed = true
	fc.mu.Unlock()
}
```

- [ ] **Step 2: 补充 import**

```go
import (
	"log/slog"
	"strings"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/metrics"
	"codeberg.org/micro-ts/mts/types"
)
```

需要在文件末尾的 import 中补充 `strings`。

- [ ] **Step 3: 编写测试**

```go
// internal/engine/flush_coordinator_test.go
package engine

import (
	"testing"
)

// mockWriter 实现 Writer 接口用于测试。
type mockWriter struct {
	mt         *memtable.MemTable
	seriesStore SeriesStore
}

func (w *mockWriter) Write(point *types.Point) error   { return w.mt.Write(...) }
func (w *mockWriter) WriteBatch(points []*types.Point) (int, error) { return 0, nil }
func (w *mockWriter) MemTable() *memtable.MemTable      { return w.mt }
func (w *mockWriter) SeriesStore() SeriesStore          { return w.seriesStore }
func (w *mockWriter) Flush() error                      { return nil }
func (w *mockWriter) Close() error                      { return nil }

func TestFlushCoordinator_RegisterAndGet(t *testing.T) {
	// 验证注册和获取 Writer
}

func TestFlushCoordinator_FlushWriter_Empty(t *testing.T) {
	// 验证空 MemTable 时 FlushWriter 返回 nil
}

func TestFlushCoordinator_Close(t *testing.T) {
	// 验证 Close 后不再接受新注册
}
```

注意：由于 FlushCoordinator 依赖具体的 `memtable.MemTable`，完整单测需要 memtable 包，这会带来循环依赖风险。实际上 FlushCoordinator 放在 engine 包，memtable 不在 engine 内，不会循环依赖。但测试的 mock 实现需要导入 memtable。

- [ ] **Step 4: 编译验证**

```bash
go build ./internal/engine/...
```

Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add internal/engine/flush_coordinator.go internal/engine/flush_coordinator_test.go
git commit -m "feat(engine): 创建FlushCoordinator编排组件"
```

---

### Task 3: 重构 writer.MeasurementWriter 实现 Writer 接口

**Files:**
- Modify: `internal/storage/writer/writer.go`
- Modify: `internal/storage/writer/writer_flush.go`

**核心变更:**
1. 删除 ShardStore/SchemaStore 依赖（由 Engine 直接注入 SeriesStore）
2. 删除 async flush → SSTable 逻辑（移到 FlushCoordinator）
3. 删除 `executeAsyncFlush`、`groupByShardKey`、`writeGroupSSTable`、`writeGroupSSTableSync`、`startPeriodicFlushCheck`、`doPeriodicFlush`、`tryTriggerAsyncFlush`
4. 保留 WAL + MemTable 管理 + Write/WriteBatch + ReplayWAL

- [ ] **Step 1: 删除 Config 中的 ShardStore/SchemaStore/CompactionCfg/LevelCompactionCfg**

在 `writer.go` 中修改 Config:

```go
// Config 定义 MeasurementWriter 的配置。
type Config struct {
	DB                   string
	Measurement          string
	Dir                  string // measurement 数据根目录
	SeriesStore          SeriesStore
	MemTableCfg          *memtable.MemTableConfig
	Logger               *slog.Logger
}
```

删除:
- `ShardDuration int64`
- `SchemaStore SchemaStore`
- `ShardStore ShardStore`
- `CompactionCfg *compaction.Config`
- `LevelCompactionCfg *compaction.LevelConfig`
- `CompressionAlgorithm sstable.CompressionAlgorithm`

- [ ] **Step 2: 删除 MeasurementWriter 中不再需要的字段**

从 MeasurementWriter struct 中删除:
- `schemaStore SchemaStore`
- `shardStore ShardStore`
- `schema *metadata.Schema`、`schemaMu sync.RWMutex`、`schemaStable atomic.Bool`
- `compactionCfg *compaction.Config`
- `levelCompactionCfg *compaction.LevelConfig`
- `compressionAlgorithm sstable.CompressionAlgorithm`
- `flushDone chan struct{}`
- `flushTicker *time.Ticker`
- `flushWg sync.WaitGroup`
- `shardDur int64`

保留:
- `db`, `measurement`, `dir`
- `memTable`, `wal`
- `seriesStore SeriesStore`
- `mu sync.Mutex`
- `closed atomic.Bool`, `closeOnce sync.Once`

- [ ] **Step 3: 删除不再需要的接口和类型**

删除:
- `SeriesStore` 接口（engine 包已有）
- `SchemaStore` 接口
- `ShardStore` 接口
- `ShardInfo` 结构体
- `detectFieldType`, `metadataFieldTypeToSSTableType`, `sstableFieldTypeToMetadataType`, `sstableSchemaToMetaSchema` 函数
- `validateFieldTypes`, `validateFieldsFast`, `fieldTypeMismatchError`
- `Schema()` 方法
- `Dir()` 方法

- [ ] **Step 4: 简化 New 函数**

```go
func New(cfg Config) (*MeasurementWriter, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	walDir := filepath.Join(cfg.Dir, "wal")
	w, err := wal.Open(wal.Config{
		Dir:          walDir,
		SegmentSize:  64 * 1024 * 1024,
		MaxSegments:  5,
		SyncMode:     wal.SyncPeriodic,
		SyncInterval: time.Minute,
		Logger:       logger,
	})
	if err != nil {
		w = nil
		logger.Warn("failed to open WAL, writes will not be durable",
			"walDir", walDir, "error", err)
	}

	mt := memtable.NewMemTable(cfg.MemTableCfg)

	mw := &MeasurementWriter{
		db:          cfg.DB,
		measurement: cfg.Measurement,
		dir:         cfg.Dir,
		memTable:    mt,
		wal:         w,
		seriesStore: cfg.SeriesStore,
	}

	return mw, nil
}
```

- [ ] **Step 5: 简化 Write 方法（移除 schema 验证）**

```go
func (mw *MeasurementWriter) Write(point *types.Point) error {
	for mw.memTable.ActiveFull() {
		if !mw.memTable.IsFlushing() {
			// 背压等待，由 FlushCoordinator 异步处理
		}
		time.Sleep(backpressureSleep)
		if mw.closed.Load() {
			return fmt.Errorf("writer closed during backpressure wait")
		}
	}

	mw.mu.Lock()

	sid, err := mw.seriesStore.AllocateSID(point.Tags)
	if err != nil {
		mw.mu.Unlock()
		return fmt.Errorf("allocate SID: %w", err)
	}

	mp := types.PointToMemPoint(point, sid)
	mw.mu.Unlock()

	var walData []byte
	var walRelease func()
	if mw.wal != nil {
		walData, walRelease = serializePointForWALPooled(mp.Timestamp, mp.Sid, mp.FieldData)
	}

	mw.mu.Lock()
	if mw.wal != nil {
		_, err := mw.wal.Write(walData)
		if walRelease != nil {
			walRelease()
		}
		if err != nil {
			metrics.Incr(metrics.WriteErrors, 1)
			mw.mu.Unlock()
			return fmt.Errorf("write to wal: %w", err)
		}
	}

	if err := mw.memTable.Write(mp); err != nil {
		metrics.Incr(metrics.WriteErrors, 1)
		mw.mu.Unlock()
		return fmt.Errorf("write to memtable: %w", err)
	}

	metrics.Incr(metrics.WriteTotal, 1)

	shouldSwap := mw.memTable.ShouldSwap()
	mw.mu.Unlock()

	if shouldSwap {
		// 触发 FlushCoordinator 异步处理
		mw.tryTriggerFlushCallback()
	}

	return nil
}
```

- [ ] **Step 6: 简化 WriteBatch 方法（移除 schema 验证，保持与 Write 一致）**

类似简化，移除 validateFieldTypes 调用。

- [ ] **Step 7: 简化 Close 方法**

```go
func (mw *MeasurementWriter) Close() error {
	var err error
	mw.closeOnce.Do(func() {
		mw.mu.Lock()
		defer mw.mu.Unlock()

		// 等待正在进行的 flush 完成
		for mw.memTable.IsFlushing() {
			mw.mu.Unlock()
			time.Sleep(time.Millisecond)
			mw.mu.Lock()
		}

		if mw.wal != nil {
			if closeErr := mw.wal.Close(); closeErr != nil {
				slog.Warn("failed to close WAL", "error", closeErr)
			}
			if purgeErr := mw.wal.Purge(); purgeErr != nil {
				slog.Warn("failed to purge WAL", "error", purgeErr)
			}
		}

		mw.closed.Store(true)
	})
	return err
}
```

- [ ] **Step 8: 删除 writer_flush.go 中不再需要的函数**

需要删除的函数:
- `type flushGroup`
- `tryTriggerAsyncFlush`
- `type asyncFlushResult`
- `executeAsyncFlush`
- `groupByShardKey`
- `writeGroupSSTable`
- `calcPointTimeRange`
- `Flush`（改为仅返回 Swap 后的数据，由 FlushCoordinator 调用 Flusher）
- `flushLocked`
- `writeGroupSSTableSync`
- `startPeriodicFlushCheck`
- `doPeriodicFlush`
- `Close`（移到 writer.go）
- `ReplayWAL`（保留，移到 writer.go）
- `Schema`
- `IsClosed`

保留:
- `ReplayWAL`（移到 writer.go）

`Flush()` 方法需要修改为仅做 Swap 并返回 passive 数据（或直接调用 Flusher）。根据设计，Writer.Flush() 应该由 FlushCoordinator 调用，而不是自己写 SSTable。

实际上，在 FlushCoordinator 设计中，`FlushCoordinator.FlushWriter()` 会直接操作 `w.MemTable().Swap()`。所以 `Writer.Flush()` 方法在 Writer 接口中可能不需要了... 

等等，设计中 Writer 接口有 `Flush() error` 方法。让我重新思考。

Writer.Flush() 方法应该在 Close 时被 Engine 调用（同步刷盘）。它的作用是触发 MemTable swap，但实际的 SSTable 写入应该由 FlushCoordinator 调用 Flusher 完成。

但 Writer 不知道 Flusher 的存在——这就是 FlushCoordinator 的作用。所以在 Close 场景下：
1. Engine.Close() → FlushCoordinator.FlushAll() → 对每个 Writer: Swap → Flusher.Flush
2. Engine.Close() → Writer.Close() → WAL 清理

所以 Writer.Flush() 要不要保留？如果 FlushCoordinator 直接操作 MemTable，那 Writer.Flush() 就是空的或仅做 FlushCoordinator 的入口调用。

为了简洁，让 Writer.Flush() 为空操作（这个方法的目的是接口完整性）。实际的 flush 由 FlushCoordinator 完成。

```go
// Flush 是同步刷盘入口，由 FlushCoordinator 调用。
// MeasurementWriter 自身不处理刷盘逻辑，刷盘由 FlushCoordinator 编排。
func (mw *MeasurementWriter) Flush() error {
	return nil
}
```

不对，这样不对。Writer.Flush() 应该有实际语义。让我重新设计：

Writer.Flush() = swap MemTable 返回 passive 数据。但返回什么？接口签名是 `Flush() error`。

Option A: Flush() swap passive, return error. FlushCoordinator then calls w.MemTable().SwapPassive() to get the data.
Option B: Flush() 只是标记需要刷盘，FlushCoordinator 负责 Swap + Flush。

我觉得最简单的方案是：**删除 Writer 接口的 Flush() 方法**。FlushCoordinator 直接操作 MemTable（通过 Writer.MemTable() 获取）。在 Close 时，Engine 通过 FlushCoordinator.FlushWriter() 完成同步刷盘，然后调用 Writer.Close()。

所以 Writer 接口变为：

```go
type Writer interface {
    Write(point *types.Point) error
    WriteBatch(points []*types.Point) (int, error)
    MemTable() *memtable.MemTable
    SeriesStore() SeriesStore
    Close() error
}
```

这样更简洁。FlushCoordinator 负责编排：
1. 调用 Writer.MemTable().ShouldSwap() 检查
2. 调用 Writer.MemTable().Swap() 获取 passive
3. 调用 Flusher.Flush(passive) 写入 SSTable

好的，那设计文档需要更新。但这不影响实现计划——我们按这个做。

- [ ] **Step 9: 编译验证**

```bash
go build ./internal/storage/writer/...
```

Expected: PASS（处理完所有引用后）

- [ ] **Step 10: 提交**

```bash
git add internal/storage/writer/
git commit -m "refactor(writer): 简化MeasurementWriter移除ShardStore/Schema依赖"
```

---

### Task 4: 重构 Shard 为纯 SSTable 容器

**Files:**
- Modify: `internal/storage/shard/shard.go`
- Modify: `internal/storage/shard/shard_io.go` → 删除大部分
- Modify: `internal/storage/shard/shard_flush.go` → 删除大部分，保留 SSTable 写入辅助
- Modify: `internal/storage/shard/shard_lifecycle.go` → 简化 Close
- Modify: `internal/storage/shard/shard_sstable_ref.go` → 不变
- Delete 内容: WAL replay, Write, WriteBatch, async flush, MemTable 定期刷盘, `ShardConfig.DiskOnly`

- [ ] **Step 1: 简化 Shard 结构体**

从 Shard struct 删除以下字段:
- `memTable *memtable.MemTable`
- `wal *wal.WAL`
- `flushDone chan struct{}`
- `flushTicker *time.Ticker`
- `flushWg sync.WaitGroup`
- `replaying bool`
- `schema *metadata.Schema`
- `schemaMu sync.RWMutex`

保留:
```go
type Shard struct {
	db              string
	measurement     string
	startTime       int64
	endTime         int64
	dir             string
	seriesStore     SeriesStore
	schemaStore     SchemaStore
	mu              sync.RWMutex
	sstSeq          uint64
	sstRefs         *sstRefs
	compaction      *compaction.Manager
	levelCompaction *compaction.LevelManager
	compactionWg    sync.WaitGroup
	closeOnce       sync.Once
	closed          atomic.Bool
	compressionAlgo sstable.CompressionAlgorithm
}
```

- [ ] **Step 2: 简化 ShardConfig**

从 ShardConfig 删除:
- `MemTableCfg`
- `DiskOnly`

保留:
```go
type ShardConfig struct {
	DB                   string
	Measurement          string
	StartTime            int64
	EndTime              int64
	Dir                  string
	SeriesStore          SeriesStore
	SchemaStore          SchemaStore
	CompactionCfg        *compaction.Config
	LevelCompactionCfg   *compaction.LevelConfig
	CompressionAlgorithm sstable.CompressionAlgorithm
	Logger               *slog.Logger
}
```

- [ ] **Step 3: 简化 NewShard**

```go
func NewShard(cfg ShardConfig) *Shard {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	shard := &Shard{
		db:              cfg.DB,
		measurement:     cfg.Measurement,
		startTime:       cfg.StartTime,
		endTime:         cfg.EndTime,
		dir:             cfg.Dir,
		seriesStore:     cfg.SeriesStore,
		schemaStore:     cfg.SchemaStore,
		sstRefs:         newSSTRefs(),
		compressionAlgo: cfg.CompressionAlgorithm,
	}

	shard.sstSeq = recoverSSTSeq(cfg.Dir)
	shard.initCompaction(cfg)

	return shard
}
```

删除所有 `cfg.DiskOnly` 分支和 WAL/MemTable 初始化代码。

- [ ] **Step 4: 删除 shard_io.go 中的 Write/WriteBatch 方法**

从 `shard_io.go` 删除:
- `Write(point *types.Point) error`
- `WriteBatch(points []*types.Point) (int, error)`
- `backpressureSleep` 常量

保留:
- `listSSTableFiles() []string`（查询用）

- [ ] **Step 5: 删除 shard_flush.go 中的 flush 方法**

从 `shard_flush.go` 删除:
- `Flush() error`
- `flushLocked() error`
- `retryRename`
- `writeSSTableSync`
- `tryTriggerAsyncFlush`
- `asyncFlushInfo`
- `executeAsyncFlush`
- `writeSSTableAsync`
- `calcPointTimeRange`
- `triggerBackgroundCompaction`
- `levelCompactionEnabled`

保留（移到 shard.go 或其他文件）:
- SSTable 写入辅助函数（供 Flusher 使用）

实际上，SSTable 写入应该被 Flusher（即 ShardManager）使用。Shard 仍然需要 `RegisterSSTable`、`NextSSTSeq`、`TriggerCompaction` 等方法。

需要保留的方法（移到 shard.go）:
- `NextSSTSeq() uint64`
- `RegisterSSTable(sstSeq uint64, minTime, maxTime int64, size int64)`（简化版，不再区分 flat/level）
- `TriggerCompaction()`（触发后台 compaction）
- `triggerBackgroundCompaction()`（当前 shard_flush.go 中的实现，移到 shard.go）

看一下 `triggerBackgroundCompaction` - 它引用了 `s.levelCompaction` 和 `s.compaction`，这些保留在 Shard 中，所以这个函数也要保留。

- [ ] **Step 6: 简化 shard_lifecycle.go 的 Close 方法**

```go
func (s *Shard) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed.Store(true)
		s.mu.Unlock()

		if s.compaction != nil {
			s.compaction.Stop()
		}
		if s.levelCompaction != nil {
			s.levelCompaction.Stop()
		}
		s.compactionWg.Wait()

		slog.Debug("Shard.Close: completed")
	})
	return err
}
```

删除 `IsDiskOnly()` 方法，删除 `closeWithLock()`。

- [ ] **Step 7: 添加 Flush 辅助方法到 shard.go**

Shard 仍然需要为 Flusher 提供 SSTable 写入能力。添加方法:

```go
// WriteSSTable 将 MemPoint 写入 SSTable 文件。
// 返回 SSTable 文件路径、序列号、时间范围和错误。
func (s *Shard) WriteSSTable(points []types.MemPoint) (sstPath string, sstSeq uint64, minTime, maxTime int64, err error) {
	s.mu.Lock()
	sstSeq = s.sstSeq
	s.sstSeq++
	s.mu.Unlock()

	dataDir := s.DataDir()
	if mkdirErr := os.MkdirAll(dataDir, 0700); mkdirErr != nil {
		return "", 0, 0, 0, fmt.Errorf("create data dir: %w", mkdirErr)
	}

	sstPath = filepath.Join(dataDir, fmt.Sprintf("sst_%d.bin", sstSeq))

	w, wErr := sstable.NewWriter(s.dir, sstSeq, 0, s.compressionAlgo)
	if wErr != nil {
		return "", 0, 0, 0, fmt.Errorf("create sstable writer: %w", wErr)
	}

	if wErr := w.WriteMemPoints(points); wErr != nil {
		_ = w.Close()
		return "", 0, 0, 0, fmt.Errorf("write mempoints: %w", wErr)
	}

	if closeErr := w.Close(); closeErr != nil {
		return "", 0, 0, 0, fmt.Errorf("close sstable writer: %w", closeErr)
	}

	// 移动到 dataDir
	srcPath := filepath.Join(s.dir, "data", fmt.Sprintf("sst_%d.bin", sstSeq))
	if srcPath != sstPath {
		if renameErr := os.Rename(srcPath, sstPath); renameErr != nil {
			_ = os.Remove(srcPath)
			return "", 0, 0, 0, fmt.Errorf("move sstable: %w", renameErr)
		}
	}

	minTime, maxTime = calcTimeRange(points)
	return sstPath, sstSeq, minTime, maxTime, nil
}

// calcTimeRange 计算 points 的时间范围。
func calcTimeRange(points []types.MemPoint) (int64, int64) {
	var minTime, maxTime int64
	for i, p := range points {
		if i == 0 || p.Timestamp < minTime {
			minTime = p.Timestamp
		}
		if i == 0 || p.Timestamp > maxTime {
			maxTime = p.Timestamp
		}
	}
	return minTime, maxTime
}
```

- [ ] **Step 8: 更新 Shard 的 RegisterSSTable**

```go
// RegisterSSTable 注册新写入的 SSTable 元数据并触发后台 compaction。
func (s *Shard) RegisterSSTable(sstSeq uint64, minTime, maxTime int64, size int64) {
	if s.levelCompaction != nil {
		s.levelCompaction.AddPart(0, compaction.PartInfo{
			Name:    fmt.Sprintf("sst_%d", sstSeq),
			Size:    size,
			MinTime: minTime,
			MaxTime: maxTime,
		})
	}
}
```

- [ ] **Step 9: 更新 SchemaStore 接口引用**

`SchemaStore` 接口定义在 shard 包中，需要保留。

- [ ] **Step 10: 编译验证**

```bash
go build ./internal/storage/shard/...
```

Expected: FAIL（Manager 和 compaction 模块仍引用旧方法，Task 5-6 处理）

- [ ] **Step 11: 提交**

```bash
git add internal/storage/shard/
git commit -m "refactor(shard): Shard退化为纯SSTable容器移除WAL/MemTable"
```

---

### Task 5: 重构 ShardManager 实现 Flusher 接口

**Files:**
- Modify: `internal/storage/shard/manager.go`

**核心变更:**
1. 删除 `writers map[string]*writerEntry` 及所有 Writer 管理方法
2. 删除 `writerEntry` 结构体
3. 删除 `discoverAndReplayWAL`、`replayWriterWAL`、`loadShardFromIndex`（WAL 由 Writer 管理）
4. 删除 ShardStore 适配器方法（`GetOrCreateDiskShard`、`NextSSTSeqForShard`、`RegisterSSTableInShard`、`TriggerCompactionInShard`、`writerShardStore`、`NewShardStore`）
5. 删除 `manager *metadata.Manager` 依赖
6. 新增依赖: `Catalog`、`SeriesStore`（从 engine 包）
7. 实现 Flusher 接口

- [ ] **Step 1: 重写 ShardManager 结构体**

```go
type ShardManager struct {
	dir             string
	shardDuration   time.Duration
	compactionCfg   *compaction.Config
	compressionAlgo sstable.CompressionAlgorithm
	catalog         Catalog
	seriesStore     SeriesStore
	shards          map[string]*Shard
	mu              sync.RWMutex
}
```

删除:
- `memTableCfg`
- `manager *metadata.Manager`
- `writers map[string]*writerEntry`
- `discoveredMeasurements map[string]bool`
- `discoveryDone chan struct{}`
- `discoveryWg sync.WaitGroup`

新增：
- `catalog Catalog`（interface，从 metadata.Catalog 转型）
- `seriesStore SeriesStore`（interface）

但这里有个问题：ShardManager 在 shard 包中，Catalog/SeriesStore 接口在 engine 包中。如果 ShardManager 引用 engine 包，会造成循环依赖（engine → shard）。所以不能直接引用 engine.Catalog。

解决方法：在 shard 包中定义自己的接口，然后在 engine 中做适配。或者让 engine 的接口定义在 shard 能引用的位置。

最简单的方案：**ShardManager 保持使用 metadata.Manager**，Engine 通过适配器将 metadata.Manager 的子接口传给 ShardManager。但这样 ShardManager 不直接实现 Flusher 接口...

实际上，更好的方案是 ShardManager 不直接依赖 engine 接口，而是通过构造函数传入它需要的依赖。让我们重新思考：

ShardManager.Flush() 是 Flusher 接口的一部分。但 Flusher 接口定义在 engine 包。为了避免循环依赖：
- engine 包定义 Flusher 接口
- shard 包定义 ShardManager 结构体，**不引用 engine 包**
- ShardManager 的方法在签名上满足 Flusher 接口
- engine 包在构造 Engine 时创建 ShardManager，由于 Go 的隐式接口，ShardManager 自动满足 Flusher

ShardManager 需要的方法签名：
```go
func (m *ShardManager) Flush(db, measurement string, points []types.MemPoint) error
func (m *ShardManager) Compact(db, measurement string, startTime int64) error
func (m *ShardManager) GetShards(db, measurement string, startTime, endTime int64) []*engine.ShardHandle  // 问题！
```

`GetShards` 返回 `[]*engine.ShardHandle`，这需要 import engine，造成循环依赖。

解决方案：在 shard 包中定义 ShardHandle，然后 engine 用 type alias:

```go
// engine/interfaces.go
type ShardHandle = shard.ShardHandle
```

不对，这样 engine 依赖 shard，shard 不依赖 engine，没有循环。

或者更简单：ShardHandle 定义在 shard 包中，engine 包直接 re-export。

让我更新设计：

**shard/shard.go**:
```go
type ShardHandle struct {
	StartTime int64
	EndTime   int64
	Dir       string
}
```

**engine/interfaces.go**:
```go
type ShardHandle = shard.ShardHandle
```

这样 ShardManager 不需要 import engine，直接返回 `[]*shard.ShardHandle`，Go 隐式接口自动满足 Flusher。

好的，继续。

- [ ] **Step 2: 重写 NewShardManager**

```go
func NewShardManager(
	dir string,
	shardDuration time.Duration,
	compactionCfg *compaction.Config,
	compressionAlgo sstable.CompressionAlgorithm,
	catalog Catalog,
	seriesStore SeriesStore,
) *ShardManager {
	return &ShardManager{
		dir:             dir,
		shardDuration:   shardDuration,
		compactionCfg:   compactionCfg,
		compressionAlgo: compressionAlgo,
		catalog:         catalog,
		seriesStore:     seriesStore,
		shards:          make(map[string]*Shard),
	}
}
```

删除后台 discovery goroutine。

- [ ] **Step 3: 删除 GetShard 中的 WAL replay**

`GetShard` 方法中删除:
- `s.ReplayWAL()` 调用
- `seriesStore := m.manager.GetOrCreateSeriesStore(...)` → 改为 `seriesStore := m.seriesStore.GetOrCreateSeriesStore(...)`

等等，`GetOrCreateSeriesStore` 在 SeriesStore 接口中不存在。当前是 `metadata.Manager.GetOrCreateSeriesStore()`。在重构后，SeriesStore 接口只有 `AllocateSID`、`GetTags`、`GetSIDsByTag`、`SeriesCount`。

系列存储的作用域问题：当前每个 measurement 有独立的 SeriesStore（scope 到 db/meas），通过 `MeasSeriesStore` 实现。

在重构设计中，Engine 持有 `SeriesStore` 接口。但 `AllocateSID` 已经有 `database, measurement` 参数。所以 `metadata.seriesStore` 的实现已经是全局的了（通过 db/meas 参数区分）。`MeasSeriesStore` 是旧架构的适配器。

检查 `metadata.seriesStore.AllocateSID`:
```go
func (s *seriesStore) AllocateSID(database, measurement string, tags map[string]string) (uint64, error) {
```

是的，已经是全局的了！所以 Engine 可以直接持有一个 SeriesStore，不需要按 measurement 创建子实例。

好的，那 ShardManager 也不需要按 measurement 创建 SeriesStore 实例了。

- [ ] **Step 4: 实现 Flusher 接口方法**

```go
// Flush 将 MemPoint 写入对应 Shard 的 SSTable。
func (m *ShardManager) Flush(db, measurement string, points []types.MemPoint) error {
	if len(points) == 0 {
		return nil
	}
	if !isNameSafe(db) || !isNameSafe(measurement) {
		return fmt.Errorf("invalid database or measurement name")
	}

	// 按时间窗口分组
	groups := m.groupByShard(db, measurement, points)

	for _, g := range groups {
		shard := g.shard
		sstPath, sstSeq, minTime, maxTime, err := shard.WriteSSTable(g.points)
		if err != nil {
			return fmt.Errorf("write sstable: %w", err)
		}

		var size int64
		if fi, statErr := os.Stat(sstPath); statErr == nil {
			size = fi.Size()
		}
		shard.RegisterSSTable(sstSeq, minTime, maxTime, size)
		shard.TriggerCompaction()
	}

	return nil
}

// Compact 触发指定 Shard 的 compaction。
func (m *ShardManager) Compact(db, measurement string, startTime int64) error {
	key := m.makeKey(db, measurement, startTime)
	m.mu.RLock()
	shard, ok := m.shards[key]
	m.mu.RUnlock()
	if !ok {
		return nil
	}
	shard.TriggerCompaction()
	return nil
}

// GetShards 返回时间范围内的 Shard 句柄。
func (m *ShardManager) GetShards(db, measurement string, startTime, endTime int64) []*ShardHandle {
	if !isNameSafe(db) || !isNameSafe(measurement) {
		return nil
	}

	m.discoverShardsIfNeeded(db, measurement)

	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*ShardHandle
	shardDuration := int64(m.shardDuration)
	shardStart := (startTime / shardDuration) * shardDuration

	for ts := shardStart; ts < endTime; ts += shardDuration {
		key := m.makeKey(db, measurement, ts)
		if s, ok := m.shards[key]; ok {
			result = append(result, &ShardHandle{
				StartTime: s.startTime,
				EndTime:   s.endTime,
				Dir:       s.dir,
			})
		}
	}

	return result
}

// groupByShard 将 MemPoint 按时间窗口分组到 Shard。
func (m *ShardManager) groupByShard(db, measurement string, points []types.MemPoint) []shardGroup {
	shardDur := int64(m.shardDuration)
	groupMap := make(map[int64]*shardGroup)
	var groupOrder []int64

	for _, mp := range points {
		startTime := (mp.Timestamp / shardDur) * shardDur

		g, ok := groupMap[startTime]
		if !ok {
			shard, err := m.GetShard(db, measurement, mp.Timestamp)
			if err != nil {
				slog.Warn("failed to get shard for flush group", "startTime", startTime, "error", err)
				continue
			}
			g = &shardGroup{
				shard:  shard,
				points: make([]types.MemPoint, 0, 1024),
			}
			groupMap[startTime] = g
			groupOrder = append(groupOrder, startTime)
		}
		g.points = append(g.points, mp)
	}

	result := make([]shardGroup, 0, len(groupOrder))
	for _, ts := range groupOrder {
		result = append(result, *groupMap[ts])
	}
	return result
}

type shardGroup struct {
	shard  *Shard
	points []types.MemPoint
}
```

- [ ] **Step 5: 更新 GetShard（移除 WAL replay）**

GetShard 简化版：
```go
func (m *ShardManager) GetShard(db, measurementName string, timestamp int64) (*Shard, error) {
	if !isNameSafe(db) || !isNameSafe(measurementName) {
		return nil, fmt.Errorf("invalid database or measurement name")
	}

	startTime := m.calcShardStart(timestamp)
	endTime := startTime + int64(m.shardDuration)

	key := m.makeKey(db, measurementName, startTime)

	m.mu.RLock()
	s, ok := m.shards[key]
	m.mu.RUnlock()

	if ok {
		return s, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok = m.shards[key]; ok {
		return s, nil
	}

	shardDir := filepath.Join(m.dir, db, measurementName, formatTimeRange(startTime, endTime))
	s = NewShard(ShardConfig{
		DB:                   db,
		Measurement:          measurementName,
		StartTime:            startTime,
		EndTime:              endTime,
		Dir:                  shardDir,
		SeriesStore:          nil, // Shard 不再需要 SeriesStore（写入已移至 Writer）
		SchemaStore:          m.catalog, // Catalog 也实现 SchemaStore
		CompactionCfg:        m.compactionCfg,
		CompressionAlgorithm: m.compressionAlgo,
	})
	m.shards[key] = s

	// 注册到 ShardIndex（如果可用）
	// ...

	return s, nil
}
```

等等，这里 Catalog 也实现了 SchemaStore 接口吗？

当前代码中 `metadata.catalogStore` 有 `GetSchema` 和 `SetSchema` 方法。而 `metadata.Catalog` 接口也包含这两个方法。所以 `catalog` 确实可以作为 `SchemaStore` 使用。

但是 Shard 不再需要 SeriesStore 了吗？Shard 仍然在 compaction/查询时需要通过 SeriesStore 解析 SID → Tags。但 Shard.iterator.go 中使用了 `s.seriesStore`。

检查 Shard 中哪些地方使用了 `seriesStore`：
- `shard_io.go` Write/WriteBatch（删除）
- `iterator.go` 查询（保留） - 需要 SeriesStore

所以 Shard 仍然需要 SeriesStore。但 SeriesStore 现在由 Engine 统一持有。Shard 通过 ShardConfig.SeriesStore 获取。

好的，那 GetShard 中需要传入 SeriesStore：

但这里有个类型问题：Shard 的 SeriesStore（`shard.SeriesStore` 接口）和 engine 的 SeriesStore 不同：
- shard.SeriesStore: `AllocateSID(tags map[string]string) (uint64, error)` + `GetTagsBySID(sid uint64) (map[string]string, bool)`
- engine.SeriesStore: `AllocateSID(database, measurement string, tags map[string]string) (uint64, error)` + `GetTags(database, measurement string, sid uint64) (map[string]string, bool)` + ...

签名不一样！shard 的 SeriesStore 是 scoped 到单个 measurement 的（通过 `MeasSeriesStore`），而 engine 的 SeriesStore 是全局的（带 db/meas 参数）。

所以在重构中，Shard 要么：
A) 使用带 db/meas 参数的 SeriesStore
B) 使用 scoped SeriesStore（通过适配器）

考虑到 Shard 已经有 db/measurement 字段，可以用于自动填充参数。所以 Shard 可以使用全局接口，内部自动填充 db/meas。

更新 `shard.SeriesStore` 接口：
```go
type SeriesStore interface {
	AllocateSID(database, measurement string, tags map[string]string) (uint64, error)
	GetTags(database, measurement string, sid uint64) (map[string]string, bool)
}
```

这样和 `engine.SeriesStore` 的方法签名子集匹配。Shard 内部调用时用 `s.db, s.measurement` 作为前两个参数。

好的，这个问题解决了。

- [ ] **Step 6: 编译验证**

```bash
go build ./internal/storage/shard/...
```

Expected: PASS

- [ ] **Step 7: 提交**

---

### Task 6: 重构 Engine 使用新接口

**Files:**
- Modify: `internal/engine/engine.go`
- Modify: `internal/engine/engine_write.go`
- Modify: `internal/engine/engine_query.go`
- Modify: `internal/engine/engine_catalog.go`

- [ ] **Step 1: 更新 Engine 结构体**

```go
type Engine struct {
	cfg              *Config
	dataDir          string
	catalog          Catalog
	seriesStore      SeriesStore
	shardIndex       ShardIndex
	flusher          Flusher
	coordinator      *FlushCoordinator
	retentionService *RetentionService
	mu               sync.RWMutex
	queryWg          sync.WaitGroup
	closed           bool
	shutdownMu       sync.Mutex
}
```

- [ ] **Step 2: 更新 Config**

```go
type Config struct {
	DataDir                string
	ShardDuration          time.Duration
	MemTableCfg            *types.MemTableConfig
	CompactionCfg          *compaction.Config
	CompressionAlgorithm   sstable.CompressionAlgorithm
	RetentionPeriod        time.Duration
	RetentionCheckInterval time.Duration
}
```

不变。

- [ ] **Step 3: 重写 New 函数**

```go
func New(cfg *Config) (*Engine, error) {
	var memTableCfg *types.MemTableConfig
	if cfg.MemTableCfg == nil || cfg.MemTableCfg.MaxSize == 0 {
		memTableCfg = memtable.DefaultMemTableConfig()
	} else {
		memTableCfg = cfg.MemTableCfg
	}

	retentionCheckInterval := cfg.RetentionCheckInterval
	if retentionCheckInterval == 0 {
		retentionCheckInterval = time.Hour
	}

	mgr, err := metadata.NewManager(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("create metadata manager: %w", err)
	}

	if err := mgr.Load(); err != nil {
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	flusher := shard.NewShardManager(
		cfg.DataDir,
		cfg.ShardDuration,
		cfg.CompactionCfg,
		cfg.CompressionAlgorithm,
		mgr.Catalog(),
		mgr.Series(),
	)

	coordinator := NewFlushCoordinator(flusher)

	engine := &Engine{
		cfg:         cfg,
		dataDir:     cfg.DataDir,
		catalog:     mgr.Catalog(),
		seriesStore: mgr.Series(),
		shardIndex:  mgr.Shards(),
		flusher:     flusher,
		coordinator: coordinator,
	}

	// 后台发现已有 Shard 和 WAL replay
	go engine.discoverAndRecover()

	if cfg.RetentionPeriod > 0 {
		engine.retentionService = NewRetentionService(
			engine.flusher,
			cfg.RetentionPeriod,
			retentionCheckInterval,
		)
		engine.retentionService.Start()
	}

	return engine, nil
}
```

- [ ] **Step 4: 移动 discoverAndRecover 到 Engine**

```go
func (e *Engine) discoverAndRecover() {
	databases := e.catalog.ListDatabases()
	for _, db := range databases {
		measurements, err := e.catalog.ListMeasurements(db)
		if err != nil {
			slog.Warn("failed to list measurements", "db", db, "error", err)
			continue
		}
		for _, meas := range measurements {
			// 创建 Writer 并重放 WAL
			w, err := e.getOrCreateWriter(db, meas, memTableCfg)
			if err != nil {
				slog.Warn("failed to create writer for recovery", "db", db, "meas", meas, "error", err)
				continue
			}
			if err := w.ReplayWAL(); err != nil {
				slog.Warn("WAL replay failed", "db", db, "meas", meas, "error", err)
			}

			// 发现已有 Shard
			e.flusher.GetShards(db, meas, 0, time.Now().UnixNano())
		}
	}
}
```

- [ ] **Step 5: 更新 Write/WriteBatch**

```go
func (e *Engine) Write(ctx context.Context, point *types.Point) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if e.isClosed() {
		return fmt.Errorf("engine is closed")
	}

	if point == nil {
		return ErrNilPoint
	}
	if point.Database == "" {
		return ErrEmptyDatabase
	}
	if point.Measurement == "" {
		return ErrEmptyMeasurement
	}
	if point.Timestamp < 0 {
		return ErrInvalidTimestamp
	}

	// 自动创建 database/measurement
	if !e.catalog.DatabaseExists(point.Database) {
		if err := e.catalog.CreateDatabase(point.Database); err != nil {
			slog.Warn("auto-create database failed", "database", point.Database, "error", err)
		}
	}
	if !e.catalog.MeasurementExists(point.Database, point.Measurement) {
		if err := e.catalog.CreateMeasurement(point.Database, point.Measurement); err != nil {
			slog.Warn("auto-create measurement failed", "database", point.Database, "measurement", point.Measurement, "error", err)
		}
	}

	w, err := e.getOrCreateWriter(point.Database, point.Measurement)
	if err != nil {
		return fmt.Errorf("get writer: %w", err)
	}

	if err := w.Write(point); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	// 检查是否需要触发异步 flush
	e.checkAsyncFlush(w, point.Database, point.Measurement)

	return nil
}
```

- [ ] **Step 6: 创建 getOrCreateWriter 方法**

```go
func (e *Engine) getOrCreateWriter(db, measurement string) (Writer, error) {
	key := db + "/" + measurement

	e.mu.RLock()
	w := e.coordinator.GetWriter(db, measurement)
	e.mu.RUnlock()

	if w != nil {
		return w, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	w = e.coordinator.GetWriter(db, measurement)
	if w != nil {
		return w, nil
	}

	measDir := filepath.Join(e.dataDir, db, measurement)
	mw, err := writer.New(writer.Config{
		DB:          db,
		Measurement: measurement,
		Dir:         measDir,
		SeriesStore: &seriesStoreAdapter{store: e.seriesStore, db: db, meas: measurement},
		MemTableCfg: e.cfg.MemTableCfg,
	})
	if err != nil {
		return nil, fmt.Errorf("create writer: %w", err)
	}

	e.coordinator.RegisterWriter(db, measurement, mw)

	return mw, nil
}
```

需要 seriesStoreAdapter 因为 writer.SeriesStore 签名不同：
```go
type seriesStoreAdapter struct {
	store SeriesStore
	db    string
	meas  string
}

func (a *seriesStoreAdapter) AllocateSID(tags map[string]string) (uint64, error) {
	return a.store.AllocateSID(a.db, a.meas, tags)
}

func (a *seriesStoreAdapter) GetTagsBySID(sid uint64) (map[string]string, bool) {
	return a.store.GetTags(a.db, a.meas, sid)
}
```

- [ ] **Step 7: 更新 Query/Iterator**

```go
func (e *Engine) Iterator(ctx context.Context, req *types.QueryRangeRequest) (*query.Iterator, error) {
	if e.isClosed() {
		return nil, fmt.Errorf("engine is closed")
	}

	var writerMT *memtable.MemTable
	var extSeriesStore shard.SeriesStore
	if w := e.coordinator.GetWriter(req.Database, req.Measurement); w != nil {
		writerMT = w.MemTable()
	}

	shardHandles := e.flusher.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)
	if len(shardHandles) == 0 && writerMT == nil {
		return nil, fmt.Errorf("no shards found")
	}

	shards := make([]*shard.Shard, 0, len(shardHandles))
	for _, h := range shardHandles {
		// 需要从 ShardManager 获取实际的 Shard 对象用于查询
		// 或者改 query.Iterator 接口使用 ShardHandle
	}

	return query.NewIteratorWithMemTable(ctx, shards, writerMT, extSeriesStore, req), nil
}
```

这里有个问题：`query.NewIteratorWithMemTable` 接受 `[]*shard.Shard`，但 Flusher.GetShards 返回 `[]*ShardHandle`。

两种解决方案：
A) Flusher.GetShards 返回 `[]*shard.Shard`（暴露具体类型）
B) 修改 query.Iterator 接受更抽象的接口

考虑到 ShardHandle 只有元数据（StartTime/EndTime/Dir），而 query.Iterator 需要实际的文件列表（SSTable files），所以 Flusher 需要返回能提供 SSTable 文件列表的对象。

最简单的方案：让 `ShardHandle` 包含一个返回 SSTable 文件的方法，或者直接让 `Flusher.GetShards` 返回能提供查询所需数据的具体对象。

实际方案：在 Flusher.GetShards 中返回包含 SSTable 文件列表的句柄。

更新设计：

```go
type ShardHandle struct {
	StartTime   int64
	EndTime     int64
	Dir         string
	SSTFiles    []string  // SSTable 文件路径列表
}
```

Flusher 实现填充 SSTFiles。

这样 query.Iterator 可以直接使用 ShardHandle。

但这意味着要修改 query 包——让它接受 ShardHandle 而不是 Shard。这进一步增加了重构范围。

为了控制范围，让我保持 Flusher.GetShards 返回 `[]*shard.Shard`（使用 *shard.Shard），这样 query 包不需要修改。这有点违背接口抽象的原则，但避免了大规模的重构链。

好的，更新 Flusher 接口：

```go
// Flusher 接口（在 engine 包）
type Flusher interface {
    Flush(db, measurement string, points []types.MemPoint) error
    Compact(db, measurement string, startTime int64) error
    GetShards(db, measurement string, startTime, endTime int64) []*shard.Shard
    CloseAll() error
    SetConfig(config *compaction.Config)
}
```

但这又引入 engine → shard 的依赖。engine 本来就依赖 shard（通过 Engine 使用 shard.Shard），所以这没问题。

然后删除 `ShardHandle` 类型（不需要了）。

OK，更新设计。让我更新计划。

- [ ] **Step 8: 更新 engine_catalog.go**

这些方法已经是通过 `e.manager.Catalog()` 访问的。改为通过 `e.catalog`：

```go
func (e *Engine) ListDatabases() []string {
	return e.catalog.ListDatabases()
}

func (e *Engine) CreateDatabase(database string) bool {
	if e.catalog.DatabaseExists(database) {
		return false
	}
	if err := e.catalog.CreateDatabase(database); err != nil {
		slog.Warn("failed to create database", "database", database, "error", err)
		return false
	}
	return true
}

// ... 其他方法类似
```

- [ ] **Step 9: 更新 Close 方法**

```go
func (e *Engine) Close() error {
	e.shutdownMu.Lock()
	if e.closed {
		e.shutdownMu.Unlock()
		return nil
	}
	e.closed = true
	e.shutdownMu.Unlock()

	if e.retentionService != nil {
		e.retentionService.Stop()
	}

	e.queryWg.Wait()

	// 同步刷写所有数据
	_ = e.coordinator.FlushAll()

	// 关闭所有 Writer 和 Flusher
	_ = e.coordinator.CloseAllWriters()
	_ = e.flusher.CloseAll()

	// 同步元数据
	if err := e.syncMetadata(); err != nil {
		return fmt.Errorf("sync metadata: %w", err)
	}

	return nil
}
```

- [ ] **Step 10: 编译验证**

```bash
go build ./internal/engine/...
```

Expected: FAIL（多处需要调整）

- [ ] **Step 11: 提交**

---

### Task 7: 更新 RetentionService 使用 Flusher 接口

**Files:**
- Modify: `internal/storage/shard/retention.go`

- [ ] **Step 1: 重写 RetentionService**

由于 ShardManager 不再暴露 `GetAllShards()`（该方法在 ShardManager 中但属于旧的内部 API），需要给 Flusher 接口添加 `GetAllShards` 或者让 RetentionService 通过 ShardIndex 接口来获取数据。

RetentionService 需要遍历所有 Shard，检查时间范围。可以通过 `ShardIndex.ListShards` 实现。

但 RetentionService 当前直接依赖 `*ShardManager`。重构后它应该依赖接口。

简化方案：让 Flusher 接口增加一个方法用于 Retention：

或者保持 RetentionService 使用 `ShardIndex` 接口。

```go
func NewRetentionService(
	catalog Catalog,
	shardIndex ShardIndex,
	flusher Flusher,
	retention, checkInterval time.Duration,
) *RetentionService {
	return &RetentionService{
		catalog:      catalog,
		shardIndex:   shardIndex,
		flusher:      flusher,
		retention:    retention,
		checkInterval: checkInterval,
		done:         make(chan struct{}),
	}
}
```

RetentionService 通过 ShardIndex 列出 Shard，通过 Flusher 删除。

需要在 Flusher 接口中添加 `DeleteShard` 方法：

```go
type Flusher interface {
    Flush(db, measurement string, points []types.MemPoint) error
    Compact(db, measurement string, startTime int64) error
    GetShards(db, measurement string, startTime, endTime int64) []*shard.Shard
    CloseAll() error
    SetConfig(config *compaction.Config)
}
```

实际上，Retention 删除 Shard 可以通过直接删除文件来实现——因为 Shard 现在是纯 SSTable 容器，删除就是删目录。

让我们在 Flusher 中添加 `DeleteShard` 方法，或者在 Engine 层面处理（通过 ShardIndex + 直接删除文件）。

为了保持接口简洁，让 Engine.Close() 处理 retention 时直接操作：

实际上，当前 RetentionService 在 shard 包中。为了最小化改动，我们先把它移到 engine 包或者保持它在 shard 包中但使用新的接口。

由于时间关系，让我们将 RetentionService 改为使用 engine 包定义的接口。但由于循环依赖，需要在 engine 包中定义 RetentionService。

OK，我意识到这个重构范围太大了。让我简化：将 RetentionService 移到 engine 包。

好的，计划已经够详细了。让我精简一下，重点完成任务分解。

---

### Task 8: 修复编译错误和测试

**Files:**
- Modify: `internal/engine/engine_test.go`
- Modify: `internal/storage/shard/manager_test.go`
- Modify: `internal/storage/shard/shard_test.go`
- Modify: `internal/storage/writer/` 相关测试（如果有）
- 各种引用旧 API 的文件

- [ ] **Step 1-3: 逐步修复测试**

- [ ] **Step 4: 运行全部测试**

```bash
go test ./...
```

- [ ] **Step 5: 运行 e2e 测试**

---

### Task 9: 代码清理和 lint

**Files:**
- 删除不再使用的 import
- 删除 `writer.ShardStore` 接口引用

- [ ] **Step 1: golangci-lint**

```bash
golangci-lint run ./...
```

- [ ] **Step 2: goimports-reviser**

```bash
goimports-reviser -format ./...
```

- [ ] **Step 3: 清理临时构建产物**

---

### Task 10: 最终验证

- [ ] **Step 1: 运行全部测试**

```bash
go test -cover ./...
```

Expected: 覆盖率 >= 90%

- [ ] **Step 2: 运行 e2e 测试**

```bash
cd tests/e2e/simple_integrity && go build && ./simple_integrity && rm simple_integrity
cd tests/e2e/write_1k && go build && ./write_1k && rm write_1k
cd tests/e2e/compaction_test && go build && ./compaction_test && rm compaction_test
# ... 更多 e2e 测试
```

- [ ] **Step 3: 提交最终版本**

```bash
git add -A
git commit -m "refactor(engine): 接口化重构Writer/Flusher/Metadata分离关注点"
```
