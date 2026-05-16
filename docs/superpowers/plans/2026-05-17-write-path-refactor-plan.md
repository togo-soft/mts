# 写入路径重构实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 移除写入路径的分批逻辑，数据直接写入全局 WAL + MemTable；MemTable flush 到 unordered/ 目录（未排序列式格式）；500ms 定时 compaction 从 unordered 分拣排序写入 stable/ 目录。

**Architecture:** 全局 WAL + 全局 MemTable → unordered/（immutable memtable）→ compaction 分拣排序 → stable/{db}/{meas}/{shard}/L0 和 L1。SSTable 列式格式通过 header flag 区分有序/无序。查询合并 MemTable + unordered + L0 + L1 四层数据。

**Tech Stack:** Go 1.26, gRPC, Protobuf, bbolt, Snappy/LZ4

**Spec:** `docs/superpowers/specs/2026-05-17-write-path-refactor-design.md`

---

### Task 1: SSTable Header 增加 Unordered Flag

**Files:**
- Modify: `internal/storage/shard/sstable/format.go`

在 `FileHeader` 的预留字段中增加 `Flags` 字段，用于区分 unordered/sorted。

- [ ] **Step 1: 修改 FileHeader 结构体**

`internal/storage/shard/sstable/format.go` —— 将 `FileHeader` 中的 `_ [8]byte // reserved` 拆分为 `Flags uint32` + `_ [4]byte`:

```go
// FileHeader SSTable 文件头 (64 bytes)
type FileHeader struct {
	Magic              [8]byte
	Version            uint32
	RowCount           uint32
	FieldCount         uint16
	BlockCount         uint16
	BlockSize          uint16
	Flags              uint16 // 0=有序, 1=无序
	_                  uint16 // padding
	TimestampsOffset   uint64
	SidsOffset         uint64
	BlockIndexOffset   uint64
	SectionTableOffset uint64
	_                  [8]byte // reserved
}
```

在文件顶部添加 flag 常量:

```go
const (
	FlagSorted     uint16 = 0x0000
	FlagUnordered  uint16 = 0x0001
)
```

- [ ] **Step 2: 修改 Marshal/Unmarshal**

修改 `FileHeader.Marshal()` —— 偏移 18 处写入 Flags:

```go
func (h *FileHeader) Marshal() [HeaderSize]byte {
	var buf [HeaderSize]byte
	copy(buf[0:8], h.Magic[:])
	binary.LittleEndian.PutUint32(buf[8:12], h.Version)
	binary.LittleEndian.PutUint32(buf[12:16], h.RowCount)
	binary.LittleEndian.PutUint16(buf[16:18], h.FieldCount)
	binary.LittleEndian.PutUint16(buf[18:20], h.BlockCount)
	binary.LittleEndian.PutUint16(buf[20:22], h.BlockSize)
	binary.LittleEndian.PutUint16(buf[22:24], h.Flags)
	// padding at 24:26 is zero
	binary.LittleEndian.PutUint64(buf[26:34], h.TimestampsOffset)
	binary.LittleEndian.PutUint64(buf[34:42], h.SidsOffset)
	binary.LittleEndian.PutUint64(buf[42:50], h.BlockIndexOffset)
	binary.LittleEndian.PutUint64(buf[50:58], h.SectionTableOffset)
	return buf
}
```

修改 `UnmarshalFileHeader()`:

```go
func UnmarshalFileHeader(data [HeaderSize]byte) (FileHeader, error) {
	if !bytes.Equal(data[0:8], Magic[:]) {
		return FileHeader{}, fmt.Errorf("invalid magic number")
	}
	return FileHeader{
		Magic:              Magic,
		Version:            binary.LittleEndian.Uint32(data[8:12]),
		RowCount:           binary.LittleEndian.Uint32(data[12:16]),
		FieldCount:         binary.LittleEndian.Uint16(data[16:18]),
		BlockCount:         binary.LittleEndian.Uint16(data[18:20]),
		BlockSize:          binary.LittleEndian.Uint16(data[20:22]),
		Flags:              binary.LittleEndian.Uint16(data[22:24]),
		TimestampsOffset:   binary.LittleEndian.Uint64(data[26:34]),
		SidsOffset:         binary.LittleEndian.Uint64(data[34:42]),
		BlockIndexOffset:   binary.LittleEndian.Uint64(data[42:50]),
		SectionTableOffset: binary.LittleEndian.Uint64(data[50:58]),
	}, nil
}
```

- [ ] **Step 3: 给 Writer 增加 Flags 参数**

`internal/storage/shard/sstable/writer.go` —— 给 `Writer` 结构体增加 `flags` 字段，在 `NewWriter` 签名中增加 `flags uint16` 参数:

```go
type Writer struct {
	// ... existing fields ...
	flags uint16
}

func NewWriter(shardDir string, seq uint64, blockSize int, compressAlgo CompressionAlgorithm, flags uint16) (*Writer, error) {
	w := &Writer{
		// ... existing init ...
		flags: flags,
	}
	// ... rest ...
}
```

在 `Close()` 方法中写 header 时传入 `w.flags`。找到 `writer_close.go` 中构建 `FileHeader` 的位置，设置 `Flags: w.flags`。

- [ ] **Step 4: 给 Reader 增加 Flags 读取**

`internal/storage/shard/sstable/reader.go` —— `FileHeader` 已被解析，`Reader` 结构体增加 `Flags` 字段:

```go
type Reader struct {
	// ... existing ...
	Flags uint16
}

func NewReader(filePath string, schema Schema) (*Reader, error) {
	// ... existing parse ...
	r.Flags = header.Flags
	// ...
}
```

- [ ] **Step 5: 更新所有 NewWriter 调用点**

搜索项目中所有 `sstable.NewWriter(` 调用，添加最后一个参数 `sstable.FlagSorted`:

```bash
grep -rn "sstable.NewWriter(" internal/
```

更新以下文件中的调用:
- `internal/storage/shard/shard.go` —— `writeSSTableWithTimeout`
- `internal/storage/compaction/merge.go` —— compaction 输出
- `internal/storage/compaction/level.go` —— level compaction 输出

- [ ] **Step 6: 更新 format_test.go 相关测试**

确保 `TestMarshalUnmarshalFileHeader` 测试验证 Flags 字段正确读写。

- [ ] **Step 7: 运行现有测试确保不引入回归**

```bash
cd internal/storage/shard/sstable && go test ./... -count=1 -timeout 60s
```

- [ ] **Step 8: Commit**

```bash
git add internal/storage/shard/sstable/
git commit -m "feat(sstable): 增加 Header Flags 字段支持有序/无序标记"
```

---

### Task 2: 新建 unordered 管理模块

**Files:**
- Create: `internal/storage/unordered/unordered.go`
- Create: `internal/storage/unordered/unordered_test.go`

管理 `{dataDir}/unordered/` 目录下的 SSTable 文件。

- [ ] **Step 1: 创建 unordered.go**

```go
// Package unordered 管理未排序 SSTable 文件（immutable memtable 集合）
package unordered

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
)

const (
	dirName   = "unordered"
	filePerm  = 0600
)

var globalSeq atomic.Uint64

// Dir 返回 unordered 目录路径
func Dir(dataDir string) string {
	return filepath.Join(dataDir, dirName)
}

// EnsureDir 确保 unordered 目录存在（权限 0700）
func EnsureDir(dataDir string) error {
	return os.MkdirAll(Dir(dataDir), 0700)
}

// NextSeq 获取全局自增序列号
func NextSeq() uint64 {
	return globalSeq.Add(1)
}

// SetSeq 从已有文件中恢复最大序列号（启动时调用）
func SetSeq(maxSeq uint64) {
	for {
		current := globalSeq.Load()
		if maxSeq <= current || globalSeq.CompareAndSwap(current, maxSeq) {
			break
		}
	}
}

// FilePath 返回指定 seq 的文件路径
func FilePath(dataDir string, seq uint64) string {
	return filepath.Join(Dir(dataDir), fmt.Sprintf("sst_%06d.bin", seq))
}

// ListFiles 列出 unordered 目录下所有 sst_*.bin 文件，按 seq 排序
func ListFiles(dataDir string) ([]string, error) {
	dir := Dir(dataDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "sst_") || !strings.HasSuffix(e.Name(), ".bin") {
			continue
		}
		files = append(files, filepath.Join(dir, e.Name()))
	}
	sort.Slice(files, func(i, j int) bool {
		return parseSeq(files[i]) < parseSeq(files[j])
	})
	return files, nil
}

// Write 将 MemPoint 切片写入 unordered SSTable 文件，返回文件路径
func Write(dataDir string, points []types.MemPoint, compressionAlgo sstable.CompressionAlgorithm) (string, error) {
	seq := NextSeq()
	path := FilePath(dataDir, seq)

	w, err := sstable.NewWriter(filepath.Dir(path), seq, sstable.BlockSize, compressionAlgo, sstable.FlagUnordered)
	if err != nil {
		return "", fmt.Errorf("unordered write: %w", err)
	}
	if err := w.WriteMemPoints(points); err != nil {
		w.Close()
		os.Remove(path)
		return "", fmt.Errorf("unordered write points: %w", err)
	}
	if err := w.Close(); err != nil {
		os.Remove(path)
		return "", fmt.Errorf("unordered close: %w", err)
	}
	return path, nil
}

// Remove 删除指定的 unordered 文件
func Remove(path string) error {
	return os.Remove(path)
}

func parseSeq(path string) uint64 {
	base := filepath.Base(path)
	numStr := strings.TrimPrefix(base, "sst_")
	numStr = strings.TrimSuffix(numStr, ".bin")
	n, _ := strconv.ParseUint(numStr, 10, 64)
	return n
}

// RecoverSeq 从 unordered 目录恢复最大 seq（启动时调用）
func RecoverSeq(dataDir string) error {
	files, err := ListFiles(dataDir)
	if err != nil {
		return err
	}
	var maxSeq uint64
	for _, f := range files {
		seq := parseSeq(f)
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	SetSeq(maxSeq)
	return nil
}
```

- [ ] **Step 2: 创建 unordered_test.go**

```go
package unordered

import (
	"os"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

func TestEnsureDir(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	if err := EnsureDir(dataDir); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(Dir(dataDir))
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Error("expected directory")
	}
}

func TestNextSeq(t *testing.T) {
	// reset
	globalSeq.Store(0)
	s1 := NextSeq()
	s2 := NextSeq()
	if s1 != 1 {
		t.Errorf("expected 1, got %d", s1)
	}
	if s2 != 2 {
		t.Errorf("expected 2, got %d", s2)
	}
}

func TestSetSeq(t *testing.T) {
	globalSeq.Store(0)
	SetSeq(100)
	if NextSeq() != 101 {
		t.Error("expected 101 after SetSeq(100)")
	}
}

func TestListFiles_Empty(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureDir(dir); err != nil {
		t.Fatal(err)
	}
	files, err := ListFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Errorf("expected 0 files, got %d", len(files))
	}
}

func TestWriteAndList(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureDir(dir); err != nil {
		t.Fatal(err)
	}
	globalSeq.Store(0)

	mp := types.PointToMemPoint(&types.Point{
		Database:    "db1",
		Measurement: "meas1",
		Timestamp:   100,
		Fields:      map[string]*types.FieldValue{"v": types.NewFieldValue(float64(1.0))},
	}, 1)

	path, err := Write(dir, []types.MemPoint{mp}, sstable.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if path == "" {
		t.Fatal("expected non-empty path")
	}

	files, err := ListFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}

	// verify it can be read
	reader, err := sstable.NewReader(path, sstable.Schema{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	rows, err := reader.ReadAll(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestRemove(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureDir(dir); err != nil {
		t.Fatal(err)
	}
	globalSeq.Store(0)

	mp := types.PointToMemPoint(&types.Point{
		Database:    "db1",
		Measurement: "meas1",
		Timestamp:   100,
		Fields:      map[string]*types.FieldValue{},
	}, 1)

	path, err := Write(dir, []types.MemPoint{mp}, sstable.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if err := Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("file should have been removed")
	}
}

func TestRecoverSeq(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureDir(dir); err != nil {
		t.Fatal(err)
	}
	globalSeq.Store(0)

	mp := types.PointToMemPoint(&types.Point{
		Database:    "db1",
		Measurement: "meas1",
		Timestamp:   100,
		Fields:      map[string]*types.FieldValue{},
	}, 1)

	Write(dir, []types.MemPoint{mp}, sstable.CompressionNone)
	Write(dir, []types.MemPoint{mp}, sstable.CompressionNone)

	globalSeq.Store(0)
	if err := RecoverSeq(dir); err != nil {
		t.Fatal(err)
	}
	if NextSeq() != 3 {
		t.Errorf("expected seq 3 after recovery, got %d", NextSeq())
	}
}
```

- [ ] **Step 3: 运行测试**

```bash
cd internal/storage/unordered && go test -v -count=1
```

- [ ] **Step 4: Commit**

```bash
git add internal/storage/unordered/
git commit -m "feat(unordered): 新增 unordered 目录管理模块"
```

---

### Task 3: 重写全局 WAL

**Files:**
- Modify: `internal/storage/wal/wal.go`
- Modify: `internal/storage/wal/format.go`
- Create: `internal/storage/wal/global.go`

WAL 段文件改为 `wal_{seq}.wal` 格式，存放在 `{dataDir}/wal/`，支持按段销毁。

- [ ] **Step 1: 新增 WAL 段文件命名和路径函数**

`internal/storage/wal/wal.go` 已有 `segmentPath`, `segmentName` 等函数。修改为全局路径格式 `wal_{seq}.wal`:

```go
// segmentName 返回段文件名（全局格式）
func segmentName(num uint64) string {
	return fmt.Sprintf("wal_%06d.wal", num)
}

// GlobalDir 返回全局 WAL 目录
func GlobalDir(dataDir string) string {
	return filepath.Join(dataDir, "wal")
}
```

- [ ] **Step 2: 修改 WAL.Open 为全局 WAL 打开方式**

保留现有 `WAL` 结构体和 `Open` 函数，但修改配置以支持全局模式。路径改为 `{dataDir}/wal/`。

- [ ] **Step 3: 新增 TruncateBefore 方法**

在 `wal.go` 中新增方法，用于删除指定 seq 之前的 WAL 段:

```go
// TruncateBefore 删除 seq 小于指定值的所有 WAL 段（flush 后调用）
func (w *WAL) TruncateBefore(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// 列出所有段，删除编号小于 seq 的
	entries, err := listSegments(w.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Num < seq {
			os.Remove(e.Path)
		}
	}
	return nil
}
```

- [ ] **Step 4: 运行现有 WAL 测试确保无回归**

```bash
cd internal/storage/wal && go test -v -count=1 -timeout 60s
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/wal/
git commit -m "feat(wal): 重写为全局 WAL 模式，wal_{seq}.wal 命名"
```

---

### Task 4: 简化 MemTable —— 保持现有双缓冲，适配全局模式

**Files:**
- Modify: `internal/storage/memtable/memtable.go`

MemTable 现有设计已经是独立的，不需要大的改动。只需要去掉 per-measurement 相关的假设，确保可以作为全局单实例使用。

- [ ] **Step 1: 运行 MemTable 现有测试确保无回归**

```bash
cd internal/storage/memtable && go test -v -count=1 -timeout 30s
```

---

### Task 5: 重写 Engine 写入路径

**Files:**
- Modify: `internal/engine/engine.go`
- Modify: `internal/engine/engine_write.go`
- Modify: `internal/engine/interfaces.go`
- Modify: `internal/engine/flush_coordinator.go`

移除 `Writer` 接口和 `getOrCreateWriter`。改为直接操作全局 WAL + MemTable。

- [ ] **Step 1: 修改 Engine 结构体**

`internal/engine/engine.go` —— 增加全局 WAL 和 MemTable 字段，移除 coordinator 中 writers map 的依赖:

```go
type Engine struct {
	cfg          *Config
	dataDir      string
	catalog      Catalog
	seriesStore  SeriesStore
	shardIndex   ShardIndex
	flusher      Flusher
	coordinator  *FlushCoordinator
	memTable     *memtable.MemTable   // 全局 MemTable
	wal          *wal.WAL             // 全局 WAL
	memTableCfg  *types.MemTableConfig
	metaManager  *metadata.Manager
	shardMgr     *shard.ShardManager
	retentionSvc *shard.RetentionService
	mu           sync.RWMutex
	queryWg      sync.WaitGroup
	closed       bool
	shutdownMu   sync.Mutex
}
```

- [ ] **Step 2: 修改 New() —— 初始化全局 WAL 和 MemTable**

在 `New()` 函数末尾，初始化全局 WAL 和 MemTable:

```go
// 全局 WAL
walDir := wal.GlobalDir(cfg.DataDir)
if err := os.MkdirAll(walDir, 0700); err != nil {
	return nil, fmt.Errorf("create wal dir: %w", err)
}
walCfg := wal.Config{
	Dir:          walDir,
	SegmentSize:  64 * 1024 * 1024,
	MaxSegments:  5,
	SyncMode:     wal.SyncPeriodic,
	SyncInterval: time.Minute,
	Compressed:   true,
}
globalWAL, err := wal.Open(walCfg)
if err != nil {
	return nil, fmt.Errorf("open wal: %w", err)
}

// 全局 MemTable
globalMT := memtable.NewMemTable(memTableCfg)

// unordered 目录
if err := unordered.EnsureDir(cfg.DataDir); err != nil {
	return nil, fmt.Errorf("create unordered dir: %w", err)
}
if err := unordered.RecoverSeq(cfg.DataDir); err != nil {
	return nil, fmt.Errorf("recover unordered seq: %w", err)
}

e.wal = globalWAL
e.memTable = globalMT
```

- [ ] **Step 3: 重写 engine_write.go —— 移除 getOrCreateWriter，直接写全局 WAL + MemTable**

```go
func (e *Engine) Write(ctx context.Context, point *types.Point) error {
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

	// 自动创建 db/measurement 元数据
	if ok := e.catalog.DatabaseExists(point.Database); !ok {
		if _ = e.catalog.CreateDatabase(point.Database); true {}
	}
	if ok := e.catalog.MeasurementExists(point.Database, point.Measurement); !ok {
		if _, err := e.catalog.CreateMeasurement(point.Database, point.Measurement); err != nil {
			return err
		}
	}

	// 分配 SID
	sid, err := e.seriesStore.AllocateSID(point.Database, point.Measurement, point.Tags)
	if err != nil {
		return fmt.Errorf("allocate sid: %w", err)
	}

	// 序列化
	mp := types.PointToMemPoint(point, sid)
	// 序列化 WAL 格式（复用 writer 包中的序列化函数，移入 types 或 wal 包）
	walPayload := serializePointForWAL(point.Timestamp, sid, mp.FieldData)

	// 写 WAL
	if _, err := e.wal.Write(walPayload); err != nil {
		return fmt.Errorf("wal write: %w", err)
	}

	// 写 MemTable（背压检查）
	if e.memTable.ActiveFull() {
		return fmt.Errorf("memtable full, backpressure")
	}

	if err := e.memTable.Write(mp); err != nil {
		return fmt.Errorf("memtable write: %w", err)
	}
	return nil
}

func (e *Engine) WriteBatch(ctx context.Context, points []*types.Point) error {
	if len(points) == 0 {
		return nil
	}

	// 收集 WAL 数据和 MemPoints
	walData := make([][]byte, 0, len(points))
	memPoints := make([]types.MemPoint, 0, len(points))

	for _, point := range points {
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

		if ok := e.catalog.DatabaseExists(point.Database); !ok {
			_ = e.catalog.CreateDatabase(point.Database)
		}
		if ok := e.catalog.MeasurementExists(point.Database, point.Measurement); !ok {
			if _, err := e.catalog.CreateMeasurement(point.Database, point.Measurement); err != nil {
				return err
			}
		}

		sid, err := e.seriesStore.AllocateSID(point.Database, point.Measurement, point.Tags)
		if err != nil {
			return fmt.Errorf("allocate sid: %w", err)
		}

		mp := types.PointToMemPoint(point, sid)
		memPoints = append(memPoints, mp)
		walData = append(walData, serializePointForWAL(point.Timestamp, sid, mp.FieldData))
	}

	// 批量写 WAL
	if _, err := e.wal.WriteBatch(walData); err != nil {
		return fmt.Errorf("wal write batch: %w", err)
	}

	// 背压检查
	if e.memTable.ActiveFull() {
		return fmt.Errorf("memtable full")
	}

	// 批量写 MemTable
	for _, mp := range memPoints {
		if err := e.memTable.Write(mp); err != nil {
			return fmt.Errorf("memtable write: %w", err)
		}
	}
	return nil
}
```

- [ ] **Step 4: 移动序列化函数**

将 `internal/storage/writer/wal_serialize.go` 中的 `serializePointForWALPooled`、`serializePointDirect`、`deserializeFromWAL` 函数移到 `internal/storage/wal/` 包中，新建 `wal/serialize.go`:

```go
// serialize.go —— WAL 格式序列化/反序列化

var walBufPool = sync.Pool{New: func() any { return make([]byte, 0, 256) }}

func SerializePoint(ts int64, sid uint64, fieldData []byte) ([]byte, func()) {
	buf := walBufPool.Get().([]byte)[:0]
	buf = append(buf, 2) // version
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0) // ts placeholder
	binary.LittleEndian.PutUint64(buf[1:9], uint64(ts))
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0) // sid placeholder
	binary.LittleEndian.PutUint64(buf[9:17], sid)
	buf = append(buf, fieldData...)
	return buf, func() { walBufPool.Put(buf[:0]) }
}

func DeserializePoint(data []byte) (types.MemPoint, error) {
	if len(data) < 17 {
		return types.MemPoint{}, fmt.Errorf("data too short: %d", len(data))
	}
	if data[0] != 2 {
		return types.MemPoint{}, fmt.Errorf("unsupported version: %d", data[0])
	}
	ts := int64(binary.LittleEndian.Uint64(data[1:9]))
	sid := binary.LittleEndian.Uint64(data[9:17])
	fieldData := data[17:]
	return types.MemPoint{Timestamp: ts, Sid: sid, FieldData: fieldData}, nil
}
```

- [ ] **Step 5: 修改 interfaces.go —— 移除 Writer 接口**

`internal/engine/interfaces.go` 删除 `Writer` 接口定义。修改 `Flusher` 接口，`Flush` 签名改为不需要 db/measurement 参数:

```go
type Flusher interface {
	Flush(points []types.MemPoint) error
	Compact(startTime int64) error
	GetShards(db, measurement string, startTime, endTime int64) []*shard.Shard
	CloseAll() error
	SetConfig(config *compaction.Config)
}
```

- [ ] **Step 6: 运行 engine 测试查看需要调整的部分**

```bash
cd internal/engine && go test -v -count=1 -timeout 30s 2>&1 | head -100
```

- [ ] **Step 7: Commit**

```bash
git add internal/engine/ internal/storage/wal/serialize.go
git commit -m "feat(engine): 重写写入路径，移除 Writer，直接写全局 WAL + MemTable"
```

---

### Task 6: 重写 FlushCoordinator —— 输出到 unordered

**Files:**
- Modify: `internal/engine/flush_coordinator.go`

FlushCoordinator 不再管理 per-measurement writers，改为直接管理全局 MemTable 的 swap 和写入 unordered 目录。

- [ ] **Step 1: 重写 FlushCoordinator**

```go
type FlushCoordinator struct {
	memTable    *memtable.MemTable
	w           *wal.WAL
	flusher     Flusher
	dataDir     string
	compression sstable.CompressionAlgorithm
	mu          sync.Mutex
	closed      bool
	stopCh      chan struct{}
	stopOnce    sync.Once
}

func NewFlushCoordinator(mt *memtable.MemTable, w *wal.WAL, flusher Flusher, dataDir string, compression sstable.CompressionAlgorithm) *FlushCoordinator {
	return &FlushCoordinator{
		memTable:    mt,
		w:           w,
		flusher:     flusher,
		dataDir:     dataDir,
		compression: compression,
		stopCh:      make(chan struct{}),
	}
}

func (fc *FlushCoordinator) StartPeriodicCheck(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fc.checkAndFlush()
			case <-fc.stopCh:
				return
			}
		}
	}()
}

func (fc *FlushCoordinator) checkAndFlush() {
	// 不重复 flush
	if fc.memTable.IsFlushing() {
		return
	}
	// 触发条件：NearFull 或 IdleExceeded
	if !fc.memTable.NearFull() && !fc.memTable.IdleExceeded() {
		return
	}
	_ = fc.doFlush()
}

func (fc *FlushCoordinator) doFlush() error {
	// TrySetFlushing 防并发
	if !fc.memTable.TrySetFlushing() {
		return nil
	}
	defer fc.memTable.ClearFlushing()

	passive := fc.memTable.Swap()
	if len(passive) == 0 {
		return nil
	}

	// 写入 unordered 目录，不是 SSTable
	path, err := unordered.Write(fc.dataDir, passive, fc.compression)
	if err != nil {
		// 恢复数据
		fc.memTable.MergePassiveBack()
		return err
	}

	fc.memTable.ClearPassive()

	// 销毁已 flush 的 WAL 段
	seq := unordered.NextSeq() - 1
	_ = fc.w.TruncateBefore(seq)

	_ = path // path 仅用于日志
	return nil
}

func (fc *FlushCoordinator) FlushAll() error {
	return fc.doFlush()
}

func (fc *FlushCoordinator) Stop() {
	fc.stopOnce.Do(func() { close(fc.stopCh) })
}
```

- [ ] **Step 2: 运行测试**

```bash
cd internal/engine && go test -v -count=1 -timeout 30s
```

- [ ] **Step 3: Commit**

```bash
git add internal/engine/flush_coordinator.go
git commit -m "feat(flush): FlushCoordinator 改为输出到 unordered 目录"
```

---

### Task 7: 重写 Compaction —— 新增 unordered→L0 分拣排序阶段

**Files:**
- Create: `internal/storage/compaction/unordered_compactor.go`
- Modify: `internal/storage/shard/manager.go`

新增从 `unordered/` 到 `stable/` 的分拣排序 compaction。

- [ ] **Step 1: 创建 unordered_compactor.go**

```go
package compaction

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/internal/storage/unordered"
	"codeberg.org/micro-ts/mts/types"
)

// UnorderedCompactor 将 unordered 数据分拣排序写入 stable 目录
type UnorderedCompactor struct {
	dataDir     string
	shardMgr    ShardManager // 接口，提供 GetOrCreateL0Dir
	compression sstable.CompressionAlgorithm
}

// ShardManager 接口（compaction 需要的最小 shard 操作）
type ShardManager interface {
	L0Dir(db, measurement string, shardStart int64) (string, error)
	ShardDuration() int64
}

func NewUnorderedCompactor(dataDir string, shardMgr ShardManager, compression sstable.CompressionAlgorithm) *UnorderedCompactor {
	return &UnorderedCompactor{
		dataDir:     dataDir,
		shardMgr:    shardMgr,
		compression: compression,
	}
}

// Compact 扫描 unordered 下所有文件，按 (db, measurement, shard) 分拣排序，写入 stable L0
func (uc *UnorderedCompactor) Compact() error {
	files, err := unordered.ListFiles(uc.dataDir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}

	// 分组: key = "db/meas/shardStart"
	groups := make(map[string][]types.MemPoint)

	for _, file := range files {
		reader, err := sstable.NewReader(file, sstable.Schema{})
		if err != nil {
			return fmt.Errorf("open unordered %s: %w", file, err)
		}
		rows, err := reader.ReadAll(nil)
		reader.Close()
		if err != nil {
			return fmt.Errorf("read unordered %s: %w", file, err)
		}

		// 从 PointRow 重建 MemPoint
		for _, row := range rows {
			shardStart := uc.calcShardStart(row.Timestamp)
			key := fmt.Sprintf("%s/%s/%d", row.Database, row.Measurement, shardStart)
			mp := rowToMemPoint(row)
			groups[key] = append(groups[key], mp)
		}
	}

	// 对每组排序并写入 L0
	for key, points := range groups {
		sort.Slice(points, func(i, j int) bool {
			if points[i].Timestamp != points[j].Timestamp {
				return points[i].Timestamp < points[j].Timestamp
			}
			return points[i].Sid < points[j].Sid
		})

		// 解析 key: "db/meas/shardStart"
		var db, meas string
		var shardStart int64
		fmt.Sscanf(key, "%s/%s/%d", &db, &meas, &shardStart) // simplified

		l0Dir, err := uc.shardMgr.L0Dir(db, meas, shardStart)
		if err != nil {
			return err
		}

		seq := unordered.NextSeq()
		path := filepath.Join(l0Dir, fmt.Sprintf("sst_%06d.bin", seq))
		w, err := sstable.NewWriter(l0Dir, seq, sstable.BlockSize, uc.compression, sstable.FlagSorted)
		if err != nil {
			return err
		}
		if err := w.WriteMemPoints(points); err != nil {
			w.Close()
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		_ = path
	}

	// 删除已处理的 unordered 文件
	for _, file := range files {
		os.Remove(file)
	}

	return nil
}

func (uc *UnorderedCompactor) calcShardStart(ts int64) int64 {
	dur := uc.shardMgr.ShardDuration()
	return (ts / dur) * dur
}

func rowToMemPoint(row *types.PointRow) types.MemPoint {
	fieldData := types.AppendFieldData(nil, row.Fields)
	return types.MemPoint{
		Timestamp: row.Timestamp,
		Sid:       row.Sid,
		FieldData: fieldData,
	}
}
```

- [ ] **Step 2: 在 ShardManager 中添加 L0Dir 方法**

`internal/storage/shard/manager.go`:

```go
func (m *ShardManager) L0Dir(db, measurement string, shardStart int64) (string, error) {
	shard, err := m.GetShard(db, measurement, shardStart)
	if err != nil {
		return "", err
	}
	dir := filepath.Join(shard.DataDir(), "L0")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	return dir, nil
}

func (m *ShardManager) ShardDuration() int64 {
	return int64(m.shardDuration)
}
```

- [ ] **Step 3: 在 Engine 中启动 500ms 定时 UnorderedCompaction**

`internal/engine/engine.go` —— 在 `New()` 末尾添加:

```go
uc := compaction.NewUnorderedCompactor(cfg.DataDir, e.shardMgr, cfg.CompressionAlgorithm)
go func() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = uc.Compact()
		case <-shutdownCh:
			return
		}
	}
}()
```

- [ ] **Step 4: Commit**

```bash
git add internal/storage/compaction/ internal/storage/shard/manager.go internal/engine/engine.go
git commit -m "feat(compaction): 新增 unordered→L0 分拣排序压缩阶段"
```

---

### Task 8: 更新查询路径

**Files:**
- Modify: `internal/query/iterator.go`
- Modify: `internal/engine/engine_query.go`
- Modify: `internal/storage/shard/iterator.go`

查询需要合并 MemTable + unordered + L0 + L1 四层数据。

- [ ] **Step 1: 创建 unordered 层迭代器包装**

`internal/storage/unordered/iterator.go`:

```go
package unordered

import (
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// Iterator 遍历 unordered 目录下所有文件
type Iterator struct {
	readers    []*sstable.Reader
	merger     *sstable.MergeIterator
	currentRow *types.PointRow
	err        error
}

func NewIterator(dataDir string, startTime, endTime int64, fields []string) (*Iterator, error) {
	files, err := ListFiles(dataDir)
	if err != nil {
		return nil, err
	}
	var readers []*sstable.Reader
	var filePaths []string
	for _, f := range files {
		r, err := sstable.NewReader(f, sstable.Schema{})
		if err != nil {
			// 跳过损坏文件
			continue
		}
		readers = append(readers, r)
		filePaths = append(filePaths, f)
	}
	merger, err := sstable.NewMergeIterator(filePaths, startTime, endTime, sstable.Schema{}, nil, fields)
	if err != nil {
		return nil, err
	}
	return &Iterator{
		readers: readers,
		merger:  merger,
	}, nil
}

func (it *Iterator) Next() bool {
	if !it.merger.Next() {
		return false
	}
	it.currentRow = it.merger.Point()
	return true
}

func (it *Iterator) Point() *types.PointRow {
	return it.currentRow
}

func (it *Iterator) Close() error {
	return it.merger.Close()
}
```

- [ ] **Step 2: 修改 query.Iterator 合并 unordered 层**

`internal/query/iterator.go` —— 在 `NewIteratorWithMemTable` 中增加 unordered 迭代器参数，合并到 heap 中:

```go
func NewIteratorWithMemTable(ctx context.Context, shards []*shard.Shard, writerMT *memtable.MemTable, unorderedIter *unordered.Iterator, extSeriesStore shard.SeriesStore, req *types.QueryRangeRequest) *Iterator {
	q := &Iterator{
		req: req,
	}
	// ... existing shard iterators ...
	// 添加 unordered 迭代器到 shard heap
	if unorderedIter != nil {
		// 将 unorderedIter 包装为 ShardIterator 或单独的 wrapper
	}
	return q
}
```

- [ ] **Step 3: 修改 Engine.Iterator 传入 unordered 迭代器**

`internal/engine/engine_query.go`:

```go
func (e *Engine) Iterator(ctx context.Context, req *types.QueryRangeRequest) (*query.Iterator, error) {
	// ... existing shard query ...
	unorderedIter, _ := unordered.NewIterator(e.dataDir, req.StartTime, req.EndTime, req.Fields)
	return query.NewIteratorWithMemTable(ctx, shards, e.memTable, unorderedIter, extSeriesStore, req)
}
```

- [ ] **Step 4: Commit**

```bash
git add internal/query/ internal/engine/engine_query.go internal/storage/unordered/iterator.go
git commit -m "feat(query): 查询合并 unordered 层数据"
```

---

### Task 9: 崩溃恢复

**Files:**
- Modify: `internal/engine/engine.go`

WAL replay 在启动时执行，将数据恢复到 MemTable，满则 flush 到 unordered。

- [ ] **Step 1: 实现 discoverAndRecover**

`internal/engine/engine.go` —— 重写 `discoverAndRecover()`:

```go
func (e *Engine) discoverAndRecover() {
	// 扫描 wal/ 目录
	entries, err := wal.ListSegments(e.wal.Dir())
	if err != nil {
		return
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Num < entries[j].Num })

	for _, entry := range entries {
		// Replay 每个段
		err := e.wal.ReplaySegment(entry, func(payload []byte) error {
			mp, err := wal.DeserializePoint(payload)
			if err != nil {
				return err
			}
			if e.memTable.NearFull() {
				// Swap & flush to unordered
				_ = e.coordinator.doFlush()
			}
			return e.memTable.Write(mp)
		})
		if err != nil {
			// log error, continue
		}
	}

	// 恢复完成后删除已 replay 的 WAL 段
	e.wal.TruncateBefore(0) // 全部清理
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/engine/engine.go
git commit -m "feat(recovery): 实现 WAL replay 崩溃恢复"
```

---

### Task 10: 更新公共 API（mts.go）

**Files:**
- Modify: `mts.go`

适配新的 Engine 接口。大部分接口保持不变，主要是内部调用链变化。

- [ ] **Step 1: 检查 mts.go 编译通过**

`mts.go` 中 `Write`, `WriteBatch`, `QueryRange` 等方法的签名不变，但依赖的 Engine 内部实现已变化。确保编译通过:

```bash
go build ./...
```

- [ ] **Step 2: Commit**

```bash
git add mts.go
git commit -m "refactor(mts): 适配新的 Engine 写入和查询接口"
```

---

### Task 11: 移除 writer 包并清理导入

**Files:**
- Remove: `internal/storage/writer/` (整个包)

- [ ] **Step 1: 检查所有对 writer 包的引用**

```bash
grep -rn "storage/writer" --include="*.go" .
```

- [ ] **Step 2: 更新或移除引用**

将所有 `writer` 包的引用替换为对 `wal` 包或直接使用序列化函数。

- [ ] **Step 3: 删除 writer 目录**

```bash
rm -rf internal/storage/writer/
```

- [ ] **Step 4: 运行 go mod tidy**

```bash
go mod tidy
```

- [ ] **Step 5: Commit**

```bash
git add -A internal/storage/writer/ go.mod go.sum
git commit -m "refactor: 移除 writer 包，清理导入"
```

---

### Task 12: 更新 Shard 和 ShardManager —— 适配无 Flush 直接写入路径

**Files:**
- Modify: `internal/storage/shard/shard.go`
- Modify: `internal/storage/shard/manager.go`

移除 `WriteSSTable`、`Flush` 等不再需要的方法，简化 ShardManager 为仅管理 stable 数据。

- [ ] **Step 1: 简化 ShardManager.Flush 方法**

移除 `groupByShard`、`Flush` 中的直接 SSTable 写入逻辑。保留 shard 发现和 GetShard。

- [ ] **Step 2: 移除 Shard.WriteSSTable**

删除 `WriteSSTable` 和 `writeSSTableWithTimeout` 方法（它们已在 flusher 中不再被调用）。

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/
git commit -m "refactor(shard): 简化 ShardManager，移除直接 SSTable 写入"
```

---

### Task 13: 运行完整测试套件与修复

- [ ] **Step 1: 运行所有单元测试**

```bash
go test ./internal/... -count=1 -timeout 120s
```

- [ ] **Step 2: 逐修复编译错误和测试失败**

- [ ] **Step 3: 运行 golangci-lint**

```bash
golangci-lint run ./...
```

- [ ] **Step 4: 运行 goimports-reviser**

```bash
goimports-reviser -project-name codeberg.org/micro-ts/mts ./...
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "fix: 修复重构后的编译错误和测试失败"
```

---

### Task 14: E2E 测试

- [ ] **Step 1: 运行所有 E2E 测试**

按 CLAUDE.md 中的测试清单逐一执行:

```bash
# 核心功能测试
cd tests/e2e/integrity && go build && ./integrity && rm -f integrity
cd tests/e2e/simple_integrity && go build && ./simple_integrity && rm -f simple_integrity
cd tests/e2e/persistence_test && go build && ./persistence_test && rm -f persistence_test
cd tests/e2e/wal_test && go build && ./wal_test && rm -f wal_test
cd tests/e2e/restart_recovery && go build && ./restart_recovery && rm -f restart_recovery
cd tests/e2e/compaction_test && go build && ./compaction_test && rm -f compaction_test
cd tests/e2e/retention_test && go build && ./retention_test && rm -f retention_test
cd tests/e2e/grpc_write_query && go build && ./grpc_write_query && rm -f grpc_write_query

# 写入性能测试
cd tests/e2e/write_1k && go build && ./write_1k && rm -f write_1k
cd tests/e2e/write_10k && go build && ./write_10k && rm -f write_10k
cd tests/e2e/write_100k && go build && ./write_100k && rm -f write_100k

# 查询性能测试
cd tests/e2e/query_1k && go build && ./query_1k && rm -f query_1k
cd tests/e2e/query_10k && go build && ./query_10k && rm -f query_10k

# Schema 测试
cd tests/e2e/check_fields && go build && ./check_fields && rm -f check_fields
cd tests/e2e/check_schema && go build && ./check_schema && rm -f check_schema
```

- [ ] **Step 2: 修复 E2E 测试发现的问题**

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "fix: 修复 E2E 测试适配"
```

---

### Task 15: 最终验证与清理

- [ ] **Step 1: 代码覆盖率检查**

```bash
go test ./internal/... -coverprofile=coverage.out -count=1
go tool cover -func=coverage.out | grep total
```

确保覆盖率 ≥ 90%。

- [ ] **Step 2: 最终 lint 检查**

```bash
golangci-lint run ./...
```

- [ ] **Step 3: goimports-reviser 格式化**

```bash
goimports-reviser -project-name codeberg.org/micro-ts/mts ./...
```

- [ ] **Step 4: 最终编译验证**

```bash
go build ./...
go vet ./...
```

- [ ] **Step 5: 清理临时产物**

```bash
find . -name "*.test" -delete
find . -name "coverage.out" -delete
```

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: 最终验证与清理，确保覆盖率≥90%"
```

---

## 实现顺序总结

```
Task 1  (SSTable Flags)        ──┐
Task 2  (Unordered 模块)        ──┤ 基础层，可并行
Task 3  (全局 WAL)             ──┤
Task 4  (MemTable 验证)        ──┘
Task 5  (Engine 写入重写)       ── 依赖 1-4
Task 6  (FlushCoordinator)      ── 依赖 5
Task 7  (Compaction unordered)  ── 依赖 2,5
Task 8  (查询路径)              ── 依赖 2,5
Task 9  (崩溃恢复)              ── 依赖 3,5,6
Task 10 (公共 API)              ── 依赖 5-9
Task 11 (移除 writer)           ── 依赖 10
Task 12 (Shard 简化)            ── 依赖 11
Task 13 (测试修复)              ── 依赖 1-12
Task 14 (E2E 测试)              ── 依赖 13
Task 15 (最终验证)              ── 依赖 14
```
