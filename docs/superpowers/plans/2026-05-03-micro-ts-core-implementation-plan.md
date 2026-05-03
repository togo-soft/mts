# micro-ts 核心功能实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 WAL 写入集成、MemTable 自动 Flush、MetaStore SID 分配、Tag 索引构建、分层元数据架构

**Architecture:** 采用 Database → Measurement 分层元数据设计，WAL periodic sync (3s)，MemTable 阈值触发 flush，并发查询

**Tech Stack:** Go 1.26, sync, channel, time.Ticker

---

## 文件结构

```
internal/storage/
├── measurement/
│   ├── store.go           # MetaStore 接口定义（已有）
│   ├── meta.go            # 废弃，重构为下面两个文件
│   ├── db_meta.go         # 新增: DatabaseMetaStore
│   └── meas_meta.go       # 新增: MeasurementMetaStore
└── shard/
    ├── shard.go           # 修改: 新增 wal、metaStore、flushMu
    ├── wal.go             # 修改: 新增 StartPeriodicSync
    └── manager.go         # 修改: 注入 metaStore

internal/engine/
└── engine.go              # 修改: 并发查询、分层 metaStore

internal/types/
└── types.go               # 修改: PointRow 追加 SID 字段
```

---

## Task 1: 辅助函数 - tagsHash 和 tagsEqual

**Files:**
- Create: `internal/storage/measurement/meas_meta.go`

- [ ] **Step 1: 编写测试**

```go
// internal/storage/measurement/meas_meta_test.go
package measurement

import "testing"

func TestTagsHash(t *testing.T) {
    tests := []struct {
        name string
        tags map[string]string
    }{
        {"empty", map[string]string{}},
        {"single", map[string]string{"host": "server1"}},
        {"multiple", map[string]string{"host": "server1", "region": "us"}},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            h := tagsHash(tt.tags)
            if h == 0 && len(tt.tags) > 0 {
                t.Error("hash should not be 0 for non-empty tags")
            }
        })
    }
}

func TestTagsEqual(t *testing.T) {
    tags1 := map[string]string{"host": "server1", "region": "us"}
    tags2 := map[string]string{"region": "us", "host": "server1"}
    tags3 := map[string]string{"host": "server2"}

    if !tagsEqual(tags1, tags2) {
        t.Error("tags with same content but different order should be equal")
    }
    if tagsEqual(tags1, tags3) {
        t.Error("tags with different content should not be equal")
    }
}
```

- [ ] **Step 2: 运行测试**

```bash
go test ./internal/storage/measurement/... -v -run "TestTagsHash|TestTagsEqual"
```

- [ ] **Step 3: 实现辅助函数**

```go
// internal/storage/measurement/meas_meta.go

// tagsHash 计算 tags 的哈希值（与顺序无关）
func tagsHash(tags map[string]string) uint64 {
    if len(tags) == 0 {
        return 0
    }
    var h uint64 = 14695981039346656037 // FNV offset basis
    for k, v := range tags {
        h ^= uint64(len(k))
        h *= 1099511628211
        h ^= uint64(len(v))
        h *= 1099511628211
    }
    return h
}

// tagsEqual 比较两个 tags 是否相等（与顺序无关）
func tagsEqual(a, b map[string]string) bool {
    if len(a) != len(b) {
        return false
    }
    for k, v := range a {
        if b[k] != v {
            return false
        }
    }
    return true
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/measurement/... -v -run "TestTagsHash|TestTagsEqual"
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/measurement/meas_meta.go internal/storage/measurement/meas_meta_test.go
git commit -m "feat: add tagsHash and tagsEqual helper functions"
```

---

## Task 2: MeasurementMetaStore 实现

**Files:**
- Create: `internal/storage/measurement/meas_meta.go` (追加)

- [ ] **Step 1: 编写测试**

```go
// internal/storage/measurement/meas_meta_test.go (追加)

func TestMeasurementMetaStore_AllocateSID(t *testing.T) {
    m := NewMeasurementMetaStore()

    tags1 := map[string]string{"host": "server1"}
    sid1 := m.AllocateSID(tags1)
    if sid1 != 0 {
        t.Errorf("first SID should be 0, got %d", sid1)
    }

    // 相同 tags 应该返回相同 SID
    sid1Again := m.AllocateSID(tags1)
    if sid1Again != sid1 {
        t.Errorf("same tags should return same SID, got %d vs %d", sid1Again, sid1)
    }

    // 不同 tags 应该返回新 SID
    tags2 := map[string]string{"host": "server2"}
    sid2 := m.AllocateSID(tags2)
    if sid2 != 1 {
        t.Errorf("second SID should be 1, got %d", sid2)
    }
}

func TestMeasurementMetaStore_GetTagsBySID(t *testing.T) {
    m := NewMeasurementMetaStore()

    tags := map[string]string{"host": "server1", "region": "us"}
    sid := m.AllocateSID(tags)

    retrieved, ok := m.GetTagsBySID(sid)
    if !ok {
        t.Error("GetTagsBySID should return true")
    }
    if !tagsEqual(retrieved, tags) {
        t.Error("retrieved tags should match original")
    }

    // 不存在的 SID
    _, ok = m.GetTagsBySID(999)
    if ok {
        t.Error("GetTagsBySID for non-existent SID should return false")
    }
}

func TestMeasurementMetaStore_GetSidsByTag(t *testing.T) {
    m := NewMeasurementMetaStore()

    tags1 := map[string]string{"host": "server1", "region": "us"}
    tags2 := map[string]string{"host": "server2", "region": "us"}
    tags3 := map[string]string{"host": "server3", "region": "eu"}

    m.AllocateSID(tags1)
    m.AllocateSID(tags2)
    m.AllocateSID(tags3)

    sids := m.GetSidsByTag("region", "us")
    if len(sids) != 2 {
        t.Errorf("region=us should have 2 sids, got %d", len(sids))
    }
}
```

- [ ] **Step 2: 运行测试**

```bash
go test ./internal/storage/measurement/... -v -run "TestMeasurementMetaStore_"
```

- [ ] **Step 3: 实现 MeasurementMetaStore**

```go
// internal/storage/measurement/meas_meta.go

// MeasurementMetaStore Measurement 级元数据
type MeasurementMetaStore struct {
    mu       sync.RWMutex
    series   map[uint64]map[string]string // sid → tags
    tagIndex map[string][]uint64          // tagKey\0tagValue → sids
    nextSID  uint64
    dirty    bool
}

// NewMeasurementMetaStore 创建 MeasurementMetaStore
func NewMeasurementMetaStore() *MeasurementMetaStore {
    return &MeasurementMetaStore{
        series:   make(map[uint64]map[string]string),
        tagIndex: make(map[string][]uint64),
        nextSID:  0,
    }
}

// AllocateSID 分配或查找 SID
func (m *MeasurementMetaStore) AllocateSID(tags map[string]string) uint64 {
    m.mu.Lock()
    defer m.mu.Unlock()

    // 查找已存在的 SID
    for sid, t := range m.series {
        if tagsEqual(t, tags) {
            return sid
        }
    }

    // 分配新 SID
    sid := m.nextSID
    m.nextSID++
    m.series[sid] = copyTags(tags)
    m.dirty = true

    // 更新 tagIndex
    for k, v := range tags {
        indexKey := k + "\x00" + v
        m.tagIndex[indexKey] = append(m.tagIndex[indexKey], sid)
    }

    return sid
}

// GetTagsBySID 根据 SID 获取 tags
func (m *MeasurementMetaStore) GetTagsBySID(sid uint64) (map[string]string, bool) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    tags, ok := m.series[sid]
    if !ok {
        return nil, false
    }
    return copyTags(tags), true
}

// GetSidsByTag 根据 tag 条件获取 sids
func (m *MeasurementMetaStore) GetSidsByTag(tagKey, tagValue string) []uint64 {
    m.mu.RLock()
    defer m.mu.RUnlock()
    sids := m.tagIndex[tagKey+"\x00"+tagValue]
    result := make([]uint64, len(sids))
    copy(result, sids)
    return result
}

// copyTags 复制 tags map
func copyTags(tags map[string]string) map[string]string {
    result := make(map[string]string, len(tags))
    for k, v := range tags {
        result[k] = v
    }
    return result
}

// Close 关闭
func (m *MeasurementMetaStore) Close() error {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.series = nil
    m.tagIndex = nil
    return nil
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/measurement/... -v -run "TestMeasurementMetaStore_"
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/measurement/meas_meta.go internal/storage/measurement/meas_meta_test.go
git commit -m "feat: implement MeasurementMetaStore with AllocateSID and tagIndex"
```

---

## Task 3: DatabaseMetaStore 实现

**Files:**
- Create: `internal/storage/measurement/db_meta.go`

- [ ] **Step 1: 编写测试**

```go
// internal/storage/measurement/db_meta_test.go
package measurement

import "testing"

func TestDatabaseMetaStore_GetOrCreate(t *testing.T) {
    db := NewDatabaseMetaStore()

    m1 := db.GetOrCreate("cpu")
    m2 := db.GetOrCreate("cpu")
    m3 := db.GetOrCreate("memory")

    if m1 != m2 {
        t.Error("GetOrCreate with same name should return same instance")
    }
    if m1 == m3 {
        t.Error("GetOrCreate with different name should return different instance")
    }
}
```

- [ ] **Step 2: 运行测试**

```bash
go test ./internal/storage/measurement/... -v -run "TestDatabaseMetaStore_"
```

- [ ] **Step 3: 实现 DatabaseMetaStore**

```go
// internal/storage/measurement/db_meta.go

// DatabaseMetaStore 数据库级元数据容器
type DatabaseMetaStore struct {
    mu            sync.RWMutex
    measurements  map[string]*MeasurementMetaStore
}

// NewDatabaseMetaStore 创建 DatabaseMetaStore
func NewDatabaseMetaStore() *DatabaseMetaStore {
    return &DatabaseMetaStore{
        measurements: make(map[string]*MeasurementMetaStore),
    }
}

// GetOrCreate 获取或创建 MeasurementMetaStore
func (d *DatabaseMetaStore) GetOrCreate(name string) *MeasurementMetaStore {
    d.mu.RLock()
    m, ok := d.measurements[name]
    d.mu.RUnlock()
    if ok {
        return m
    }

    d.mu.Lock()
    defer d.mu.Unlock()
    // 双重检查
    if m, ok = d.measurements[name]; ok {
        return m
    }
    m = NewMeasurementMetaStore()
    d.measurements[name] = m
    return m
}

// Close 关闭所有 MeasurementMetaStore
func (d *DatabaseMetaStore) Close() error {
    d.mu.Lock()
    defer d.mu.Unlock()
    for _, m := range d.measurements {
        m.Close()
    }
    d.measurements = nil
    return nil
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/measurement/... -v -run "TestDatabaseMetaStore_"
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/measurement/db_meta.go internal/storage/measurement/db_meta_test.go
git commit -m "feat: implement DatabaseMetaStore for layered metadata"
```

---

## Task 4: WAL Periodic Sync

**Files:**
- Modify: `internal/storage/shard/wal.go`

- [ ] **Step 1: 编写测试**

```go
// internal/storage/shard/wal_test.go (追加)

func TestWAL_StartPeriodicSync(t *testing.T) {
    tmpDir := t.TempDir()

    wal, err := NewWAL(tmpDir, 0)
    if err != nil {
        t.Fatalf("NewWAL failed: %v", err)
    }

    // 写入数据
    data := []byte("test data")
    if _, err := wal.Write(data); err != nil {
        t.Fatalf("Write failed: %v", err)
    }

    // 启动 periodic sync，间隔 100ms
    done := make(chan struct{})
    wal.StartPeriodicSync(100*time.Millisecond, done)

    // 等待 250ms，确保至少 sync 了 2 次
    time.Sleep(250 * time.Millisecond)
    close(done)

    // 验证数据已刷盘（通过重新打开 WAL）
    wal2, err := NewWAL(tmpDir, 0)
    if err != nil {
        t.Fatalf("NewWAL for verify failed: %v", err)
    }
    defer wal2.Close()

    points, err := ReplayWAL(tmpDir)
    if err != nil {
        t.Fatalf("ReplayWAL failed: %v", err)
    }
    if len(points) != 1 {
        t.Errorf("expected 1 point, got %d", len(points))
    }
}
```

- [ ] **Step 2: 运行测试**

```bash
go test ./internal/storage/shard/... -v -run "TestWAL_StartPeriodicSync"
```

- [ ] **Step 3: 添加 StartPeriodicSync 方法**

```go
// internal/storage/shard/wal.go (追加)

// StartPeriodicSync 启动定期 Sync goroutine
func (w *WAL) StartPeriodicSync(interval time.Duration, done <-chan struct{}) {
    go func() {
        ticker := time.NewTicker(interval)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                if err := w.Sync(); err != nil {
                    // log error but don't stop
                    fmt.Printf("WAL Sync error: %v\n", err)
                }
            case <-done:
                return
            }
        }
    }()
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/shard/... -v -run "TestWAL_StartPeriodicSync"
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/shard/wal.go internal/storage/shard/wal_test.go
git commit -m "feat: add WAL periodic sync with StartPeriodicSync"
```

---

## Task 5: Shard 新增 wal、metaStore 字段并重构 Write

**Files:**
- Modify: `internal/storage/shard/shard.go`

- [ ] **Step 1: 编写测试**

```go
// internal/storage/shard/shard_test.go (追加)

func TestShard_WriteWithWAL(t *testing.T) {
    tmpDir := t.TempDir()

    // 创建 mock MetaStore
    metaStore := measurement.NewMeasurementMetaStore()

    s := NewShard("db1", "cpu", 0, time.Hour.Nanoseconds(), tmpDir, metaStore)
    defer s.Close()

    // 写入数据
    p := &types.Point{
        Timestamp: 1000000000,
        Tags:      map[string]string{"host": "server1"},
        Fields:   map[string]any{"usage": float64(85.5)},
    }
    if err := s.Write(p); err != nil {
        t.Fatalf("Write failed: %v", err)
    }

    // 验证 WAL 写入
    points, err := ReplayWAL(filepath.Join(tmpDir, "wal"))
    if err != nil {
        t.Fatalf("ReplayWAL failed: %v", err)
    }
    if len(points) != 1 {
        t.Errorf("expected 1 point in WAL, got %d", len(points))
    }
}
```

- [ ] **Step 2: 运行测试**

```bash
go test ./internal/storage/shard/... -v -run "TestShard_WriteWithWAL"
```

- [ ] **Step 3: 修改 Shard 结构**

```go
// internal/storage/shard/shard.go

type Shard struct {
    db          string
    measurement string
    startTime   int64
    endTime     int64
    dir         string
    memTable    *MemTable
    wal         *WAL                    // 新增
    metaStore   *measurement.MeasurementMetaStore // 新增
    flushMu     sync.Mutex              // 保护 flush
}
```

- [ ] **Step 4: 修改 NewShard 签名**

```go
// NewShard 创建 Shard
func NewShard(db, measurement string, startTime, endTime int64, dir string, metaStore *measurement.MeasurementMetaStore) *Shard {
    // 创建 WAL
    walDir := filepath.Join(dir, "wal")
    wal, err := NewWAL(walDir, 0)
    if err != nil {
        // 如果 WAL 创建失败，使用 nil wal
        wal = nil
    }

    return &Shard{
        db:          db,
        measurement: measurement,
        startTime:   startTime,
        endTime:     endTime,
        dir:         dir,
        memTable:    NewMemTable(64*1024*1024), // 64MB
        wal:         wal,
        metaStore:   metaStore,
    }
}
```

- [ ] **Step 5: 修改 Write 方法**

```go
// Write 写入点
func (s *Shard) Write(point *types.Point) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 1. 写入 WAL
    if s.wal != nil {
        data, err := serializePoint(point)
        if err != nil {
            return err
        }
        if _, err := s.wal.Write(data); err != nil {
            return err
        }
    }

    // 2. 分配 SID
    sid := s.metaStore.AllocateSID(point.Tags)

    // 3. 写入 MemTable
    if err := s.memTable.Write(point); err != nil {
        return err
    }

    // 4. 检查是否需要 flush
    if s.memTable.ShouldFlush() {
        if err := s.flushLocked(); err != nil {
            return err
        }
    }

    return nil
}
```

- [ ] **Step 6: 修改 flushLocked 方法**

```go
// flushLocked 执行 flush（假设锁已持有）
func (s *Shard) flushLocked() error {
    points := s.memTable.Flush()
    if len(points) == 0 {
        return nil
    }

    // 创建 SSTable Writer
    w, err := sstable.NewWriter(s.dir, 0)
    if err != nil {
        return err
    }

    if err := w.WritePoints(points); err != nil {
        _ = w.Close()
        return err
    }

    if err := w.Close(); err != nil {
        return err
    }

    return nil
}
```

- [ ] **Step 7: 更新 Close 方法**

```go
// Close 关闭 Shard
func (s *Shard) Close() error {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 停止 WAL
    if s.wal != nil {
        if err := s.wal.Close(); err != nil {
            return err
        }
    }

    // flush 剩余数据
    points := s.memTable.Flush()
    if len(points) > 0 {
        w, err := sstable.NewWriter(s.dir, 0)
        if err != nil {
            return err
        }

        if err := w.WritePoints(points); err != nil {
            _ = w.Close()
            return err
        }

        if err := w.Close(); err != nil {
            return err
        }
    }

    return nil
}
```

- [ ] **Step 8: 运行测试验证**

```bash
go test ./internal/storage/shard/... -v -run "TestShard_WriteWithWAL"
```

- [ ] **Step 9: Commit**

```bash
git add internal/storage/shard/shard.go internal/storage/shard/shard_test.go
git commit -m "feat: integrate WAL and MetaStore into Shard.Write"
```

---

## Task 6: ShardManager 注入 MetaStore

**Files:**
- Modify: `internal/storage/shard/manager.go`

- [ ] **Step 1: 修改 ShardManager 结构**

```go
// internal/storage/shard/manager.go

type ShardManager struct {
    dir           string
    shardDuration time.Duration
    shards        map[string]*Shard
    mu            sync.RWMutex
    dbMetaStores  map[string]*measurement.DatabaseMetaStore // 新增
}
```

- [ ] **Step 2: 修改 NewShardManager**

```go
func NewShardManager(dir string, shardDuration time.Duration, dbMetaStores map[string]*measurement.DatabaseMetaStore) *ShardManager {
    return &ShardManager{
        dir:           dir,
        shardDuration: shardDuration,
        shards:        make(map[string]*Shard),
        dbMetaStores:  dbMetaStores,
    }
}
```

- [ ] **Step 3: 修改 GetShard**

```go
func (m *ShardManager) GetShard(db, measurement string, timestamp int64) (*Shard, error) {
    startTime := m.calcShardStart(timestamp)
    endTime := startTime + int64(m.shardDuration)
    key := m.makeKey(db, measurement, startTime)

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

    // 创建新 Shard
    shardDir := filepath.Join(m.dir, db, measurement, formatTimeRange(startTime, endTime))
    if err := storage.SafeMkdirAll(shardDir, 0700); err != nil {
        return nil, err
    }

    // 获取或创建 MeasurementMetaStore
    dbMeta := m.dbMetaStores[db]
    if dbMeta == nil {
        dbMeta = measurement.NewDatabaseMetaStore()
        m.dbMetaStores[db] = dbMeta
    }
    measMeta := dbMeta.GetOrCreate(measurement)

    s = NewShard(db, measurement, startTime, endTime, shardDir, measMeta)
    m.shards[key] = s
    return s, nil
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/shard/... -v
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/shard/manager.go
git commit -m "feat: inject MetaStore into ShardManager"
```

---

## Task 7: Engine 使用分层 MetaStore

**Files:**
- Modify: `internal/engine/engine.go`

- [ ] **Step 1: 修改 Engine 结构**

```go
type Engine struct {
    cfg           *Config
    shardManager  *shard.ShardManager
    dbMetaStores  map[string]*measurement.DatabaseMetaStore // 替换 metaStores
    mu            sync.RWMutex
}
```

- [ ] **Step 2: 修改 NewEngine**

```go
func NewEngine(cfg *Config) (*Engine, error) {
    dbMetaStores := make(map[string]*measurement.DatabaseMetaStore)
    return &Engine{
        cfg:          cfg,
        shardManager: shard.NewShardManager(cfg.DataDir, cfg.ShardDuration, dbMetaStores),
        dbMetaStores: dbMetaStores,
    }, nil
}
```

- [ ] **Step 3: 并发查询实现**

```go
func (e *Engine) Query(req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
    shards := e.shardManager.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)

    if len(shards) == 0 {
        return &types.QueryRangeResponse{
            Database:    req.Database,
            Measurement: req.Measurement,
            StartTime:   req.StartTime,
            EndTime:     req.EndTime,
            TotalCount:  0,
            Rows:        []types.PointRow{},
        }, nil
    }

    // 并发读取所有 Shard
    rowsCh := make(chan []types.PointRow, len(shards))
    var wg sync.WaitGroup

    for _, s := range shards {
        wg.Add(1)
        go func(s *shard.Shard) {
            defer wg.Done()
            rows, err := s.Read(req.StartTime, req.EndTime)
            if err != nil {
                return
            }
            rowsCh <- rows
        }(s)
    }

    go func() {
        wg.Wait()
        close(rowsCh)
    }()

    // 合并结果
    var allRows []types.PointRow
    for rows := range rowsCh {
        allRows = append(allRows, rows...)
    }

    // 按时间排序
    sort.Slice(allRows, func(i, j int) bool {
        return allRows[i].Timestamp < allRows[j].Timestamp
    })

    // Tag 过滤（使用 tagIndex）
    if len(req.Tags) > 0 {
        allRows = e.filterTags(allRows, req.Tags)
    }

    // 字段过滤
    if len(req.Fields) > 0 {
        allRows = e.filterFields(allRows, req.Fields)
    }

    // 分页
    totalCount := int64(len(allRows))
    if req.Offset > 0 {
        if req.Offset < int64(len(allRows)) {
            allRows = allRows[req.Offset:]
        } else {
            allRows = nil
        }
    }
    if req.Limit > 0 && int64(len(allRows)) > req.Limit {
        allRows = allRows[:req.Limit]
    }

    return &types.QueryRangeResponse{
        Database:    req.Database,
        Measurement: req.Measurement,
        StartTime:   req.StartTime,
        EndTime:     req.EndTime,
        TotalCount:  totalCount,
        HasMore:     req.Limit > 0 && int64(len(allRows)) >= req.Limit,
        Rows:        allRows,
    }, nil
}
```

- [ ] **Step 4: 添加 sort import**

```go
import (
    "sort"  // 新增
    "sync"
    "time"

    "micro-ts/internal/storage/measurement"
    "micro-ts/internal/storage/shard"
    "micro-ts/internal/types"
)
```

- [ ] **Step 5: 修改 filterTags 使用 tagIndex**

```go
func (e *Engine) filterTags(rows []types.PointRow, tags map[string]string) []types.PointRow {
    // 获取 db 的 metaStore
    dbMeta := e.dbMetaStores[rows[0].Database] // rows[0] 存在说明至少有一个 shard
    if dbMeta == nil {
        return rows
    }

    measMeta := dbMeta.GetOrCreate(rows[0].Measurement)
    if measMeta == nil {
        return rows
    }

    // 获取所有满足 tag 条件的 sids
    var sids []uint64
    for k, v := range tags {
        tagSids := measMeta.GetSidsByTag(k, v)
        if len(sids) == 0 {
            sids = tagSids
        } else {
            // 取交集
            sidSet := make(map[uint64]bool)
            for _, sid := range sids {
                sidSet[sid] = true
            }
            var intersect []uint64
            for _, sid := range tagSids {
                if sidSet[sid] {
                    intersect = append(intersect, sid)
                }
            }
            sids = intersect
        }
    }

    if len(sids) == 0 {
        return nil
    }

    // 过滤
    sidSet := make(map[uint64]bool)
    for _, sid := range sids {
        sidSet[sid] = true
    }

    result := make([]types.PointRow, 0, len(rows))
    for _, row := range rows {
        if sidSet[row.SID] {
            result = append(result, row)
        }
    }
    return result
}
```

- [ ] **Step 6: 运行测试验证**

```bash
go test ./internal/engine/... -v
```

- [ ] **Step 7: Commit**

```bash
git add internal/engine/engine.go
git commit -m "feat: use layered MetaStore and concurrent query"
```

---

## Task 8: PointRow 添加 SID 字段

**Files:**
- Modify: `internal/types/types.go`

- [ ] **Step 1: 修改 PointRow**

```go
// PointRow 是查询结果的一行
type PointRow struct {
    SID       uint64            // Series ID，新增
    Timestamp int64
    Tags      map[string]string
    Fields    map[string]any
}
```

- [ ] **Step 2: 运行测试验证**

```bash
go test ./... -v 2>&1 | head -100
```

- [ ] **Step 3: Commit**

```bash
git add internal/types/types.go
git commit -m "feat: add SID field to PointRow"
```

---

## Task 9: 运行完整 E2E 测试

- [ ] **Step 1: 运行所有单元测试**

```bash
go test ./... -v 2>&1 | tail -50
```

- [ ] **Step 2: 运行 E2E 测试**

```bash
go run ./tests/e2e/write_1k/
go run ./tests/e2e/query_1k/
go run ./tests/e2e/integrity/
```

- [ ] **Step 3: Commit 所有变更**

```bash
git status
git add -A
git commit -m "feat: complete core implementation with WAL, MetaStore and concurrent query"
```

---

## Task 10: 代码格式化和 Lint

- [ ] **Step 1: golangci-lint**

```bash
golangci-lint run ./...
```

- [ ] **Step 2: goimports-reviser**

```bash
goimports-reviser -local micro-ts -w .
```

- [ ] **Step 3: 最终测试**

```bash
go test ./... -cover
```

---

## Self-Review 检查清单

- [ ] Spec 覆盖：WAL 集成 ✓, MetaStore 分层 ✓, Flush ✓, 并发查询 ✓
- [ ] 无占位符：所有 TODO/FIXME 已实现
- [ ] 类型一致：Shard.Write 签名匹配 ShardManager.GetShard
- [ ] 权限正确：目录 0700，文件 0600
- [ ] 错误处理：flush 失败返回错误，persist 失败仅 log

---

## Plan complete

**Implementation approach:** Subagent-Driven (recommended)

Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?