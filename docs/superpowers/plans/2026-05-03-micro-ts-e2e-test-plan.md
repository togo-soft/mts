# micro-ts 端到端测试套件实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 micro-ts 端到端测试套件，包括存储引擎连接、数据完整性测试和性能测试

**Architecture:** 存储引擎连接 ShardManager、MemTable、WAL、SSTable 和 MemoryMetaStore；测试套件使用 t.TempDir() 隔离

**Tech Stack:** Go 1.26, testing, math/rand, runtime MemStats

---

## File Structure

```
micro-ts/
├── internal/storage/
│   ├── engine.go           # 存储引擎 (新建)
│   └── engine_test.go      # 存储引擎测试 (新建)
├── microts.go              # Library API (修改：连接 Engine)
├── tests/
│   ├── integrity_test.go   # 数据完整性测试 (新建)
│   ├── bench_test.go       # 性能测试 (新建)
│   └── data_gen.go         # 数据生成器 (新建)
└── docs/superpowers/specs/2026-05-03-micro-ts-e2e-test-design.md
```

---

## Phase 1: 存储引擎实现

### Task 1.1: Engine 结构定义

**Files:**
- Create: `internal/storage/engine.go`
- Create: `internal/storage/engine_test.go`

- [ ] **Step 1: 编写 Engine 初始化测试**

```go
// internal/storage/engine_test.go
package storage

import (
    "testing"
    "time"
)

func TestEngine_Open(t *testing.T) {
    cfg := &Config{
        DataDir:        t.TempDir(),
        ShardDuration: time.Hour,
    }

    engine, err := NewEngine(cfg)
    if err != nil {
        t.Fatalf("NewEngine failed: %v", err)
    }
    defer engine.Close()

    if engine == nil {
        t.Errorf("expected non-nil engine")
    }
}

func TestEngine_Close(t *testing.T) {
    cfg := &Config{
        DataDir:        t.TempDir(),
        ShardDuration: time.Hour,
    }

    engine, _ := NewEngine(cfg)
    err := engine.Close()
    if err != nil {
        t.Fatalf("Close failed: %v", err)
    }
}
```

- [ ] **Step 2: 运行测试验证（预期失败）**

```bash
go test ./internal/storage/... -v -run TestEngine
```
Expected: FAIL - "NewEngine not defined"

- [ ] **Step 3: 编写 Engine 结构**

```go
// internal/storage/engine.go
package storage

import (
    "sync"
    "time"

    "micro-ts/internal/measurement"
    "micro-ts/internal/shard"
    "micro-ts/internal/types"
)

// Config 存储引擎配置
type Config struct {
    DataDir        string
    ShardDuration time.Duration
}

// Engine 存储引擎
type Engine struct {
    cfg          *Config
    shardManager *shard.Manager
    metaStores   map[string]*measurement.MemoryMetaStore
    mu           sync.RWMutex
}

// NewEngine 创建引擎
func NewEngine(cfg *Config) (*Engine, error) {
    return &Engine{
        cfg:          cfg,
        shardManager: shard.NewShardManager(cfg.DataDir, cfg.ShardDuration),
        metaStores:   make(map[string]*measurement.MemoryMetaStore),
    }, nil
}

// Close 关闭引擎
func (e *Engine) Close() error {
    e.mu.Lock()
    defer e.mu.Unlock()
    e.metaStores = nil
    return nil
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/... -v -run TestEngine
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/engine.go internal/storage/engine_test.go
git commit -m "feat: add Engine struct"
```

---

### Task 1.2: Engine Write 方法

**Files:**
- Modify: `internal/storage/engine.go`
- Modify: `internal/storage/engine_test.go`

- [ ] **Step 1: 编写 Write 测试**

```go
// internal/storage/engine_test.go
func TestEngine_Write(t *testing.T) {
    cfg := &Config{
        DataDir:        t.TempDir(),
        ShardDuration: time.Hour,
    }

    engine, _ := NewEngine(cfg)
    defer engine.Close()

    point := &types.Point{
        Database:    "db1",
        Measurement: "cpu",
        Tags:        map[string]string{"host": "server1"},
        Timestamp:   time.Now().UnixNano(),
        Fields:      map[string]any{"usage": 85.5},
    }

    err := engine.Write(point)
    if err != nil {
        t.Fatalf("Write failed: %v", err)
    }
}

func TestEngine_WriteBatch(t *testing.T) {
    cfg := &Config{
        DataDir:        t.TempDir(),
        ShardDuration: time.Hour,
    }

    engine, _ := NewEngine(cfg)
    defer engine.Close()

    points := []*types.Point{
        {
            Database:    "db1",
            Measurement: "cpu",
            Tags:        map[string]string{"host": "server1"},
            Timestamp:   time.Now().UnixNano(),
            Fields:      map[string]any{"usage": 85.5},
        },
        {
            Database:    "db1",
            Measurement: "cpu",
            Tags:        map[string]string{"host": "server2"},
            Timestamp:   time.Now().UnixNano() + 1e9,
            Fields:      map[string]any{"usage": 90.0},
        },
    }

    err := engine.WriteBatch(points)
    if err != nil {
        t.Fatalf("WriteBatch failed: %v", err)
    }
}
```

- [ ] **Step 2: 运行测试验证（预期失败）**

```bash
go test ./internal/storage/... -v -run TestEngine_Write
```
Expected: FAIL - "Write not defined"

- [ ] **Step 3: 实现 Write 方法**

```go
// internal/storage/engine.go

// metaKey 生成 metaStore 的 key
func (e *Engine) metaKey(db, measurement string) string {
    return db + "/" + measurement
}

// getMetaStore 获取或创建 MetaStore
func (e *Engine) getMetaStore(db, measurement string) *measurement.MemoryMetaStore {
    e.mu.Lock()
    defer e.mu.Unlock()

    key := e.metaKey(db, measurement)
    if ms, ok := e.metaStores[key]; ok {
        return ms
    }

    ms := measurement.NewMemoryMetaStore()
    e.metaStores[key] = ms
    return ms
}

// Write 写入单个点
func (e *Engine) Write(point *types.Point) error {
    // 获取或创建 Shard
    s, err := e.shardManager.GetShard(point.Database, point.Measurement, point.Timestamp)
    if err != nil {
        return err
    }

    // 写入 Shard
    return s.Write(point)
}

// WriteBatch 批量写入
func (e *Engine) WriteBatch(points []*types.Point) error {
    for _, p := range points {
        if err := e.Write(p); err != nil {
            return err
        }
    }
    return nil
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/... -v -run TestEngine_Write
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/engine.go internal/storage/engine_test.go
git commit -m "feat: add Engine Write methods"
```

---

### Task 1.3: Shard.Write 方法

**Files:**
- Modify: `internal/storage/shard/shard.go`

- [ ] **Step 1: 添加 Write 方法到 Shard**

```go
// internal/storage/shard/shard.go

// Shard 新增字段和方法
type Shard struct {
    db          string
    measurement string
    startTime  int64
    endTime    int64
    dir        string
    memTable   *MemTable
    wal        *WAL
}

// NewShard 修改为初始化 memTable 和 wal
func NewShard(db, measurement string, startTime, endTime int64, dir string) (*Shard, error) {
    // 创建目录
    if err := SafeMkdirAll(dir, 0700); err != nil {
        return nil, err
    }

    walDir := filepath.Join(dir, "wal")
    if err := SafeMkdirAll(walDir, 0700); err != nil {
        return nil, err
    }

    wal, err := NewWAL(walDir, 0)
    if err != nil {
        return nil, err
    }

    return &Shard{
        db:          db,
        measurement: measurement,
        startTime:   startTime,
        endTime:     endTime,
        dir:         dir,
        memTable:    NewMemTable(64 * 1024 * 1024), // 64MB
        wal:         wal,
    }, nil
}

// Write 写入数据
func (s *Shard) Write(point *types.Point) error {
    // 写入 WAL
    // TODO: 序列化 point

    // 写入 MemTable
    if err := s.memTable.Write(point); err != nil {
        return err
    }

    // 检查是否需要刷盘
    if s.memTable.ShouldFlush() {
        return s.flush()
    }

    return nil
}

// flush 刷盘到 SSTable
func (s *Shard) flush() error {
    points := s.memTable.Flush()
    if len(points) == 0 {
        return nil
    }

    // TODO: 写入 SSTable
    _ = points

    return nil
}

// Close 关闭
func (s *Shard) Close() error {
    if s.wal != nil {
        s.wal.Close()
    }
    return nil
}
```

- [ ] **Step 2: 运行测试验证**

```bash
go build ./internal/storage/shard/...
```
Expected: 可能需要修复编译错误

- [ ] **Step 3: Commit**

---

### Task 1.4: Engine Query 方法

**Files:**
- Modify: `internal/storage/engine.go`
- Modify: `internal/storage/engine_test.go`

- [ ] **Step 1: 编写 Query 测试**

```go
// internal/storage/engine_test.go
func TestEngine_Query(t *testing.T) {
    cfg := &Config{
        DataDir:        t.TempDir(),
        ShardDuration: time.Hour,
    }

    engine, _ := NewEngine(cfg)
    defer engine.Close()

    now := time.Now().UnixNano()

    // 写入测试数据
    points := []*types.Point{
        {
            Database:    "db1",
            Measurement: "cpu",
            Tags:        map[string]string{"host": "server1"},
            Timestamp:   now,
            Fields:      map[string]any{"usage": 85.5},
        },
        {
            Database:    "db1",
            Measurement: "cpu",
            Tags:        map[string]string{"host": "server1"},
            Timestamp:   now + 1e9,
            Fields:      map[string]any{"usage": 90.0},
        },
    }

    engine.WriteBatch(points)

    // 查询
    req := &types.QueryRangeRequest{
        Database:    "db1",
        Measurement: "cpu",
        StartTime:   now,
        EndTime:     now + 2e9,
    }

    resp, err := engine.Query(req)
    if err != nil {
        t.Fatalf("Query failed: %v", err)
    }

    if len(resp.Rows) != 2 {
        t.Errorf("expected 2 rows, got %d", len(resp.Rows))
    }
}
```

- [ ] **Step 2: 运行测试验证（预期失败）**

```bash
go test ./internal/storage/... -v -run TestEngine_Query
```

- [ ] **Step 3: 实现 Query 方法**

```go
// internal/storage/engine.go

// Query 范围查询
func (e *Engine) Query(req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
    // 获取相交的 Shard
    shards := e.shardManager.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)

    var rows []types.PointRow
    for _, s := range shards {
        r, err := s.Read(req.StartTime, req.EndTime)
        if err != nil {
            return nil, err
        }
        rows = append(rows, r...)
    }

    return &types.QueryRangeResponse{
        Database:    req.Database,
        Measurement: req.Measurement,
        StartTime:   req.StartTime,
        EndTime:     req.EndTime,
        TotalCount:  int64(len(rows)),
        Rows:        rows,
    }, nil
}
```

- [ ] **Step 4: 运行测试验证**

- [ ] **Step 5: Commit**

---

### Task 1.5: microts.go 连接 Engine

**Files:**
- Modify: `microts.go`

- [ ] **Step 1: 更新 microts.go 连接 Engine**

```go
// microts.go
package microts

import (
    "context"

    "micro-ts/internal/storage"
    "micro-ts/internal/types"
)

// DB 数据库
type DB struct {
    engine *storage.Engine
}

// Open 打开数据库
func Open(cfg Config) (*DB, error) {
    engine, err := storage.NewEngine(&storage.Config{
        DataDir:        cfg.DataDir,
        ShardDuration:  cfg.ShardDuration,
    })
    if err != nil {
        return nil, err
    }
    return &DB{engine: engine}, nil
}

// Write 写入单个点
func (db *DB) Write(ctx context.Context, point *types.Point) error {
    return db.engine.Write(point)
}

// WriteBatch 批量写入
func (db *DB) WriteBatch(ctx context.Context, points []*types.Point) error {
    return db.engine.WriteBatch(points)
}

// QueryRange 范围查询
func (db *DB) QueryRange(ctx context.Context, req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
    return db.engine.Query(req)
}

// ListMeasurements 列出 Measurement
func (db *DB) ListMeasurements(ctx context.Context, database string) ([]string, error) {
    return db.engine.ListMeasurements(database)
}

// Close 关闭数据库
func (db *DB) Close() error {
    return db.engine.Close()
}
```

- [ ] **Step 2: 运行测试验证**

```bash
go build ./...
```

- [ ] **Step 3: Commit**

```bash
git add microts.go
git commit -m "feat: connect Library API to Engine"
```

---

## Phase 2: 数据完整性测试

### Task 2.1: 数据生成器

**Files:**
- Create: `tests/data_gen.go`

- [ ] **Step 1: 实现数据生成器**

```go
// tests/data_gen.go
package tests

import (
    "math/rand"
    "time"
)

// DataGenerator 数据生成器
type DataGenerator struct {
    seed     int64
    rand     *rand.Rand
    baseTime int64
}

// NewDataGenerator 创建生成器
func NewDataGenerator(seed int64) *DataGenerator {
    return &DataGenerator{
        seed:     seed,
        rand:     rand.New(rand.NewSource(seed)),
        baseTime: time.Now().UnixNano(),
    }
}

// Field 定义
type Field struct {
    Name string
    Type string // "float64", "int64", "string", "bool"
}

// GeneratePoint 生成单个数据点
func (g *DataGenerator) GeneratePoint(db, measurement string, timestamp int64) *types.Point {
    return &types.Point{
        Database:    db,
        Measurement: measurement,
        Tags:        map[string]string{"host": "server1"},
        Timestamp:   timestamp,
        Fields:      g.generateFields(),
    }
}

// generateFields 生成 10 个字段
func (g *DataGenerator) generateFields() map[string]any {
    fields := make(map[string]any)

    // 5 个浮点数
    for i := 1; i <= 5; i++ {
        fields["field_float_"+string(rune('0'+i))] = g.rand.Float64() * 1000
    }

    // 3 个整数
    for i := 1; i <= 3; i++ {
        fields["field_int_"+string(rune('0'+i))] = int64(g.rand.Intn(100000))
    }

    // 1 个字符串
    fields["field_string_1"] = g.randomString(8 + g.rand.Intn(9))

    // 1 个布尔
    fields["field_bool_1"] = g.rand.Intn(2) == 1

    return fields
}

// randomString 生成随机字符串
func (g *DataGenerator) randomString(length int) string {
    const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    b := make([]byte, length)
    for i := range b {
        b[i] = chars[g.rand.Intn(len(chars))]
    }
    return string(b)
}
```

- [ ] **Step 2: Commit**

---

### Task 2.2: 数据完整性测试

**Files:**
- Create: `tests/integrity_test.go`

- [ ] **Step 1: 编写完整性测试**

```go
// tests/integrity_test.go
package tests

import (
    "testing"
    "time"

    "micro-ts"
)

func TestIntegrity_100K_Points(t *testing.T) {
    // 使用固定种子保证可复现
    gen := NewDataGenerator(42)

    // 创建数据库
    db, err := microts.Open(microts.Config{
        DataDir:       t.TempDir(),
        ShardDuration: time.Hour,
    })
    if err != nil {
        t.Fatalf("Open failed: %v", err)
    }
    defer db.Close()

    const count = 100000
    baseTime := time.Now().UnixNano()
    expectedPoints := make([]*types.Point, count)

    // 生成并写入数据
    t.Log("Generating and writing 100K points...")
    for i := 0; i < count; i++ {
        ts := baseTime + int64(i)*int64(time.Second)
        p := gen.GeneratePoint("db1", "cpu", ts)
        expectedPoints[i] = p

        if err := db.Write(nil, p); err != nil {
            t.Fatalf("Write failed at %d: %v", i, err)
        }
    }

    // 查询所有数据
    t.Log("Reading back data...")
    resp, err := db.QueryRange(nil, &types.QueryRangeRequest{
        Database:    "db1",
        Measurement: "cpu",
        StartTime:   baseTime,
        EndTime:     baseTime + int64(count)*int64(time.Second),
        Limit:       0, // 不限制
    })
    if err != nil {
        t.Fatalf("Query failed: %v", err)
    }

    // 验证数量
    if int(resp.TotalCount) != count {
        t.Errorf("expected %d points, got %d", count, resp.TotalCount)
    }

    // 逐字段验证
    t.Log("Verifying data integrity...")
    for i, row := range resp.Rows {
        expected := expectedPoints[i]

        // 验证 timestamp
        if row.Timestamp != expected.Timestamp {
            t.Errorf("row %d: timestamp mismatch: expected %d, got %d", i, expected.Timestamp, row.Timestamp)
        }

        // 验证 fields
        for name, expectedVal := range expected.Fields {
            actualVal, ok := row.Fields[name]
            if !ok {
                t.Errorf("row %d: missing field %s", i, name)
                continue
            }
            if actualVal != expectedVal {
                t.Errorf("row %d: field %s mismatch: expected %v, got %v", i, name, expectedVal, actualVal)
            }
        }
    }

    t.Log("Data integrity verified successfully")
}
```

- [ ] **Step 2: 运行测试**

```bash
go test ./tests/... -v -run TestIntegrity_100K
```

- [ ] **Step 3: Commit**

---

## Phase 3: 性能测试

### Task 3.1: 性能测试实现

**Files:**
- Create: `tests/bench_test.go`

- [ ] **Step 1: 编写性能测试**

```go
// tests/bench_test.go
package tests

import (
    "runtime"
    "testing"
    "time"

    "micro-ts"
    "micro-ts/internal/types"
)

func runGC() {
    runtime.GC()
}

// memStats 内存统计
type memStats struct {
    alloc uint64
}

func getMemStats() memStats {
    var stats runtime.MemStats
    runtime.ReadMemStats(&stats)
    return memStats{alloc: stats.Alloc}
}

func BenchmarkWrite_1K(b *testing.B) {
    benchmarkWrite(b, 1000)
}

func BenchmarkWrite_10K(b *testing.B) {
    benchmarkWrite(b, 10000)
}

func BenchmarkWrite_100K(b *testing.B) {
    benchmarkWrite(b, 100000)
}

func BenchmarkWrite_1M(b *testing.B) {
    benchmarkWrite(b, 1000000)
}

func benchmarkWrite(b *testing.B, count int) {
    runGC()
    memBefore := getMemStats()
    timeBefore := time.Now()

    db, _ := microts.Open(microts.Config{
        DataDir:       b.TempDir(),
        ShardDuration: time.Hour,
    })
    defer db.Close()

    gen := NewDataGenerator(42)
    baseTime := time.Now().UnixNano()

    for i := 0; i < count; i++ {
        ts := baseTime + int64(i)*int64(time.Second)
        p := gen.GeneratePoint("db1", "cpu", ts)
        db.Write(nil, p)
    }

    timeAfter := time.Now()
    memAfter := getMemStats()
    runGC()

    elapsed := timeAfter.Sub(timeBefore)
    tps := float64(count) / elapsed.Seconds()
    memDelta := memAfter.alloc - memBefore.alloc

    b.ReportMetric(tps, "writes/sec")
    b.ReportMetric(float64(memDelta)/1024/1024, "MB_allocated")
    b.SetBytesProcessed(int64(count * 100)) // 估算
}
```

- [ ] **Step 2: 运行性能测试**

```bash
go test ./tests/... -v -bench=BenchmarkWrite -benchmem
```

- [ ] **Step 3: Commit**

---

## Self-Review 检查清单

- [ ] Spec 覆盖：每个章节都有对应实现
- [ ] 无占位符：代码完整，无 TBD/TODO
- [ ] 类型一致：接口方法签名一致
- [ ] 权限正确：目录 0700，文件 0600
- [ ] 错误处理：所有错误都被处理

---

## Plan complete

**Implementation approach:** Subagent-Driven (recommended) or Inline Execution

**Execution options:**
1. **Subagent-Driven** - I dispatch a fresh subagent per task, review between tasks
2. **Inline Execution** - Execute tasks in this session using executing-plans

Which approach?