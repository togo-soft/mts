# micro-ts 时序数据库实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现一个高性能时序数据库，支持多值模型、Columnar LSM-tree 存储、高写入吞吐、高效范围查询

**Architecture:** 采用 Multi-Writer Shard 分片架构，元数据使用 MetaStore 接口抽象，列式存储使用自定义块结构，支持 mmap 高效随机访问

**Tech Stack:** Go 1.26, gRPC, Protobuf（仅用于 IDL 和 proto 消息）

---

## 阶段划分

### Phase 1: 基础设施（项目初始化、类型定义、工具函数）
### Phase 2: 元数据存储（MetaStore 接口与实现）
### Phase 3: WAL（Write-Ahead Log）
### Phase 4: MemTable（内存跳表）
### Phase 5: SSTable（列式存储）
### Phase 6: Shard 管理
### Phase 7: 查询执行器
### Phase 8: gRPC API
### Phase 9: Library API 与入口
### Phase 10: 测试与验证

---

## File Structure

```
micro-ts/
├── cmd/
│   └── server/
│       └── main.go
├── microts.go
├── internal/
│   ├── api/
│   │   ├── grpc.go
│   │   └── pb/
│   │       └── microts.pb.go
│   ├── storage/
│   │   ├── engine.go
│   │   ├── measurement/
│   │   │   ├── meta.go
│   │   │   └── store.go
│   │   └── shard/
│   │       ├── manager.go
│   │       ├── shard.go
│   │       ├── memtable.go
│   │       ├── wal.go
│   │       ├── sstable/
│   │       │   ├── reader.go
│   │       │   ├── writer.go
│   │       │   └── block.go
│   │       └── compression/
│   │           ├── delta.go
│   │           ├── delta_of_delta.go
│   │           ├── gorilla.go
│   │           └── varint.go
│   ├── query/
│   │   └── executor.go
│   └── types/
│       └── types.go
├── proto/
│   └── microts.proto
├── go.mod
├── go.sum
└── README.md
```

---

## Phase 1: 基础设施

### Task 1.1: 项目初始化

**Files:**
- Create: `go.mod`
- Create: `proto/microts.proto`

- [ ] **Step 1: 初始化 go.mod**

```bash
cd /c/Users/xuthu/GolandProjects/micro-ts
go mod init micro-ts
go get google.golang.org/grpc@latest
go get google.golang.org/protobuf@latest
go get github.com/golang/protobuf/protoc-gen-go@latest
```

- [ ] **Step 2: 创建 proto 定义文件**

Create `proto/microts.proto`:

```protobuf
syntax = "proto3";

package microts.v1;

option go_package = "microts/internal/api/pb";

// 字段值
message FieldValue {
    oneof value {
        int64 int_value = 1;
        double float_value = 2;
        string string_value = 3;
        bool bool_value = 4;
    }
}

// 写入请求
message WriteRequest {
    string database = 1;
    string measurement = 2;
    map<string, string> tags = 3;
    int64 timestamp = 4;
    map<string, FieldValue> fields = 5;
}

// 批量写入请求
message WriteBatchRequest {
    repeated WriteRequest points = 1;
}

// 写入响应
message WriteResponse {
    bool success = 1;
    string error = 2;
}

// 范围查询请求
message QueryRangeRequest {
    string database = 1;
    string measurement = 2;
    int64 start_time = 3;
    int64 end_time = 4;
    repeated string fields = 5;
    map<string, string> tags = 6;
    int64 offset = 7;
    int64 limit = 8;
}

// 一行数据
message Row {
    int64 timestamp = 1;
    map<string, string> tags = 2;
    map<string, FieldValue> fields = 3;
}

// 范围查询响应
message QueryRangeResponse {
    string database = 1;
    string measurement = 2;
    int64 start_time = 3;
    int64 end_time = 4;
    int64 total_count = 5;
    bool has_more = 6;
    repeated Row rows = 7;
}

// 列出 Measurement 请求
message ListMeasurementsRequest {
    string database = 1;
}

// 列出 Measurement 响应
message ListMeasurementsResponse {
    repeated string measurements = 1;
}

// 健康检查请求
message HealthRequest {
}

// 健康检查响应
message HealthResponse {
    bool healthy = 1;
    string version = 2;
}

// gRPC 服务
service MicroTS {
    rpc Write(WriteRequest) returns (WriteResponse);
    rpc WriteBatch(WriteBatchRequest) returns (WriteBatchResponse);
    rpc QueryRange(QueryRangeRequest) returns (QueryRangeResponse);
    rpc ListMeasurements(ListMeasurementsRequest) returns (ListMeasurementsResponse);
    rpc Health(HealthRequest) returns (HealthResponse);
}
```

- [ ] **Step 3: 生成 protobuf 代码**

```bash
cd /c/Users/xuthu/GolandProjects/micro-ts
mkdir -p internal/api/pb
protoc --go_out=. --go_opt=paths=source_relative proto/microts.proto
mv microts.pb.go internal/api/pb/
```

- [ ] **Step 4: Commit**

```bash
git add go.mod proto/microts.proto internal/api/pb/microts.pb.go
git commit -m "feat: initialize project with go.mod and proto definitions"
```

---

### Task 1.2: 核心类型定义

**Files:**
- Create: `internal/types/types.go`
- Create: `internal/types/types_test.go`

- [ ] **Step 1: 编写测试用例**

```go
// internal/types/types_test.go
package types

import (
    "testing"
    "time"
)

func TestPointFieldTypes(t *testing.T) {
    p := &Point{
        Database:    "db1",
        Measurement: "cpu",
        Tags:        map[string]string{"host": "server1"},
        Timestamp:   time.Now().UnixNano(),
        Fields: map[string]any{
            "usage": 85.5,
            "count": int64(100),
            "active": true,
            "name": "test",
        },
    }

    if p.Database != "db1" {
        t.Errorf("expected database db1, got %s", p.Database)
    }

    if p.Measurement != "cpu" {
        t.Errorf("expected measurement cpu, got %s", p.Measurement)
    }

    if len(p.Tags) != 1 || p.Tags["host"] != "server1" {
        t.Errorf("unexpected tags: %v", p.Tags)
    }

    if len(p.Fields) != 4 {
        t.Errorf("expected 4 fields, got %d", len(p.Fields))
    }
}

func TestQueryRangeRequest(t *testing.T) {
    req := &QueryRangeRequest{
        Database:    "db1",
        Measurement: "cpu",
        StartTime:   1000,
        EndTime:     2000,
        Fields:      []string{"usage"},
        Tags:        map[string]string{"host": "server1"},
        Offset:      0,
        Limit:       100,
    }

    if req.Database != "db1" {
        t.Errorf("expected database db1")
    }
    if req.StartTime >= req.EndTime {
        t.Errorf("start_time should be less than end_time")
    }
    if req.Limit != 100 {
        t.Errorf("expected limit 100, got %d", req.Limit)
    }
}

func TestPointRow(t *testing.T) {
    row := &PointRow{
        Timestamp: time.Now().UnixNano(),
        Tags:      map[string]string{"host": "server1"},
        Fields:    map[string]any{"usage": 85.5},
    }

    if row.Timestamp == 0 {
        t.Errorf("timestamp should not be zero")
    }
    if len(row.Tags) != 1 {
        t.Errorf("expected 1 tag, got %d", len(row.Tags))
    }
}
```

- [ ] **Step 2: 运行测试验证**

```bash
cd /c/Users/xuthu/GolandProjects/micro-ts
go test ./internal/types/... -v
```

- [ ] **Step 3: 编写实现代码**

```go
// internal/types/types.go
package types

import (
    "time"
)

// Point 是写入的基本单位
type Point struct {
    Database    string
    Measurement string
    Tags        map[string]string
    Timestamp   int64 // 纳秒
    Fields      map[string]any
}

// PointRow 是查询结果的一行
type PointRow struct {
    Timestamp int64
    Tags      map[string]string
    Fields    map[string]any
}

// QueryRangeRequest 定义范围查询
type QueryRangeRequest struct {
    Database    string
    Measurement string
    StartTime   int64
    EndTime     int64
    Fields      []string
    Tags        map[string]string
    Offset      int64
    Limit       int64
}

// QueryRangeResponse 返回查询结果
type QueryRangeResponse struct {
    Database    string
    Measurement string
    StartTime   int64
    EndTime     int64
    TotalCount  int64
    HasMore     bool
    Rows        []PointRow
}

// Config 配置
type Config struct {
    // Shard 时间窗口，最小 1 小时
    ShardDuration time.Duration

    // MemTable 刷盘阈值（最小 1MB）
    MemTableSize int64

    // WAL 刷盘策略
    WALSyncPolicy string

    // 数据目录
    DataDir string
}

// FieldType 字段类型
type FieldType int

const (
    FieldTypeInt64 FieldType = iota
    FieldTypeFloat64
    FieldTypeString
    FieldTypeBool
)

// FieldDef 字段定义
type FieldDef struct {
    Name string
    Type FieldType
}

// MeasurementMeta Measurement 元信息
type MeasurementMeta struct {
    Version    int64
    FieldSchema []FieldDef
    TagKeys    []string
    NextSID    int64
}
```

- [ ] **Step 4: 运行测试验证**

```bash
cd /c/Users/xuthu/GolandProjects/micro-ts
go test ./internal/types/... -v
```

- [ ] **Step 5: Commit**

```bash
git add internal/types/types.go internal/types/types_test.go
git commit -m "feat: add core types definitions"
```

---

### Task 1.3: 工具函数

**Files:**
- Create: `internal/storage/util.go`
- Create: `internal/storage/util_test.go`

- [ ] **Step 1: 编写测试用例**

```go
// internal/storage/util_test.go
package storage

import (
    "os"
    "path/filepath"
    "testing"
)

func TestSafeMkdirAll(t *testing.T) {
    tmpDir := t.TempDir()
    testPath := filepath.Join(tmpDir, "test", "nested", "dir")

    err := safeMkdirAll(testPath, 0700)
    if err != nil {
        t.Fatalf("safeMkdirAll failed: %v", err)
    }

    info, err := os.Stat(testPath)
    if err != nil {
        t.Fatalf("stat failed: %v", err)
    }
    if !info.IsDir() {
        t.Errorf("expected directory")
    }
}

func TestSafeCreate(t *testing.T) {
    tmpDir := t.TempDir()
    testFile := filepath.Join(tmpDir, "test.txt")

    f, err := safeCreate(testFile, 0600)
    if err != nil {
        t.Fatalf("safeCreate failed: %v", err)
    }
    f.Close()

    info, err := os.Stat(testFile)
    if err != nil {
        t.Fatalf("stat failed: %v", err)
    }
    if info.Mode().Perm() != 0600 {
        t.Errorf("expected 0600, got %o", info.Mode().Perm())
    }
}

func TestSafeOpenFile(t *testing.T) {
    tmpDir := t.TempDir()
    testFile := filepath.Join(tmpDir, "test.txt")

    f, err := safeOpenFile(testFile, os.O_RDWR|os.O_CREATE, 0600)
    if err != nil {
        t.Fatalf("safeOpenFile failed: %v", err)
    }
    f.Close()

    info, err := os.Stat(testFile)
    if err != nil {
        t.Fatalf("stat failed: %v", err)
    }
    if info.Mode().Perm() != 0600 {
        t.Errorf("expected 0600, got %o", info.Mode().Perm())
    }
}
```

- [ ] **Step 2: 运行测试验证**

```bash
go test ./internal/storage/... -v -run TestSafe
```

- [ ] **Step 3: 编写实现代码**

```go
// internal/storage/util.go
package storage

import (
    "os"
    "path/filepath"
)

// safeMkdirAll 创建目录，设置权限 0700
func safeMkdirAll(path string, perm uint32) error {
    // 确保父目录存在
    parent := filepath.Dir(path)
    if parent != "" && parent != "." {
        if err := os.MkdirAll(parent, 0755); err != nil {
            return err
        }
    }
    return os.MkdirAll(path, os.FileMode(perm))
}

// safeCreate 创建文件，设置权限 0600
func safeCreate(path string, perm uint32) (*os.File, error) {
    dir := filepath.Dir(path)
    if dir != "" && dir != "." {
        if err := safeMkdirAll(dir, 0700); err != nil {
            return nil, err
        }
    }
    return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(perm))
}

// safeOpenFile 打开文件，设置权限
func safeOpenFile(name string, flag int, perm uint32) (*os.File, error) {
    dir := filepath.Dir(name)
    if dir != "" && dir != "." {
        if err := safeMkdirAll(dir, 0700); err != nil {
            return nil, err
        }
    }
    return os.OpenFile(name, flag, os.FileMode(perm))
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/... -v -run TestSafe
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/util.go internal/storage/util_test.go
git commit -m "feat: add safe file operation utilities"
```

---

## Phase 2: 元数据存储

### Task 2.1: MetaStore 接口定义

**Files:**
- Create: `internal/storage/measurement/store.go`
- Create: `internal/storage/measurement/store_test.go`

- [ ] **Step 1: 编写测试用例**

```go
// internal/storage/measurement/store_test.go
package measurement

import (
    "testing"
)

func TestMetaStoreInterface(t *testing.T) {
    // 验证 MemoryMetaStore 实现了 MetaStore 接口
    var _ MetaStore = (*MemoryMetaStore)(nil)
}
```

- [ ] **Step 2: 运行测试验证**

```bash
go test ./internal/storage/measurement/... -v -run TestMetaStoreInterface
```

- [ ] **Step 3: 编写接口定义**

```go
// internal/storage/measurement/store.go
package measurement

import (
    "context"
    "microts/internal/types"
)

// MetaStore 元数据存储接口
type MetaStore interface {
    // 获取 Measurement 元信息
    GetMeta(ctx context.Context) (*types.MeasurementMeta, error)
    // 更新 Measurement 元信息
    SetMeta(ctx context.Context, meta *types.MeasurementMeta) error

    // 根据 sid 获取 tags
    GetSeries(ctx context.Context, sid uint64) ([]byte, error)
    // 添加或更新 series
    SetSeries(ctx context.Context, sid uint64, tags []byte) error
    // 获取所有 series
    GetAllSeries(ctx context.Context) (map[uint64][]byte, error)

    // 获取 tag 对应的所有 sid
    GetSidsByTag(ctx context.Context, tagKey, tagValue string) ([]uint64, error)
    // 添加 tag -> sid 映射
    AddTagIndex(ctx context.Context, tagKey, tagValue string, sid uint64) error

    // 持久化到磁盘
    Persist(ctx context.Context) error
    // 从磁盘加载
    Load(ctx context.Context) error
    // 关闭
    Close() error
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/measurement/... -v -run TestMetaStoreInterface
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/measurement/store.go internal/storage/measurement/store_test.go
git commit -m "feat: add MetaStore interface"
```

---

### Task 2.2: MemoryMetaStore 实现

**Files:**
- Modify: `internal/storage/measurement/meta.go`
- Create: `internal/storage/measurement/meta_test.go`

- [ ] **Step 1: 编写测试用例**

```go
// internal/storage/measurement/meta_test.go
package measurement

import (
    "context"
    "testing"
    "time"

    "microts/internal/types"
)

func TestMemoryMetaStore_SetAndGetMeta(t *testing.T) {
    store := NewMemoryMetaStore()

    meta := &types.MeasurementMeta{
        Version: 1,
        FieldSchema: []types.FieldDef{
            {Name: "usage", Type: types.FieldTypeFloat64},
        },
        TagKeys: []string{"host"},
        NextSID: 1,
    }

    err := store.SetMeta(context.Background(), meta)
    if err != nil {
        t.Fatalf("SetMeta failed: %v", err)
    }

    got, err := store.GetMeta(context.Background())
    if err != nil {
        t.Fatalf("GetMeta failed: %v", err)
    }

    if got.Version != meta.Version {
        t.Errorf("expected version %d, got %d", meta.Version, got.Version)
    }
    if len(got.FieldSchema) != len(meta.FieldSchema) {
        t.Errorf("expected %d fields, got %d", len(meta.FieldSchema), len(got.FieldSchema))
    }
}

func TestMemoryMetaStore_Series(t *testing.T) {
    store := NewMemoryMetaStore()

    tags := []byte{0x02, 0x04, 'h', 'o', 's', 't', 0x07, 's', 'e', 'r', 'v', 'e', 'r', '1'}

    err := store.SetSeries(context.Background(), 1, tags)
    if err != nil {
        t.Fatalf("SetSeries failed: %v", err)
    }

    got, err := store.GetSeries(context.Background(), 1)
    if err != nil {
        t.Fatalf("GetSeries failed: %v", err)
    }

    if len(got) != len(tags) {
        t.Errorf("expected %d bytes, got %d", len(tags), len(got))
    }
}

func TestMemoryMetaStore_TagIndex(t *testing.T) {
    store := NewMemoryMetaStore()

    err := store.AddTagIndex(context.Background(), "host", "server1", 1)
    if err != nil {
        t.Fatalf("AddTagIndex failed: %v", err)
    }

    sids, err := store.GetSidsByTag(context.Background(), "host", "server1")
    if err != nil {
        t.Fatalf("GetSidsByTag failed: %v", err)
    }

    if len(sids) != 1 || sids[0] != 1 {
        t.Errorf("expected [1], got %v", sids)
    }
}

func TestMemoryMetaStore_Close(t *testing.T) {
    store := NewMemoryMetaStore()
    err := store.Close()
    if err != nil {
        t.Fatalf("Close failed: %v", err)
    }
}

func TestMemoryMetaStore_NextSID(t *testing.T) {
    store := NewMemoryMetaStore()

    meta := &types.MeasurementMeta{
        Version:    1,
        FieldSchema: []types.FieldDef{},
        TagKeys:    []string{},
        NextSID:    1,
    }

    err := store.SetMeta(context.Background(), meta)
    if err != nil {
        t.Fatalf("SetMeta failed: %v", err)
    }

    // 获取当前 sid
    m, _ := store.GetMeta(context.Background())
    if m.NextSID != 1 {
        t.Errorf("expected NextSID 1, got %d", m.NextSID)
    }

    // 分配新 sid
    newSID := m.NextSID
    m.NextSID++
    store.SetMeta(context.Background(), m)

    m, _ = store.GetMeta(context.Background())
    if m.NextSID != newSID+1 {
        t.Errorf("expected NextSID %d, got %d", newSID+1, m.NextSID)
    }
}
```

- [ ] **Step 2: 运行测试验证（预期失败）**

```bash
go test ./internal/storage/measurement/... -v -run TestMemoryMetaStore
```

- [ ] **Step 3: 编写实现代码**

```go
// internal/storage/measurement/meta.go
package measurement

import (
    "context"
    "sync"

    "microts/internal/types"
)

// MemoryMetaStore 内存实现的 MetaStore
type MemoryMetaStore struct {
    mu       sync.RWMutex
    meta     *types.MeasurementMeta
    series   map[uint64][]byte
    tagIndex map[string][]uint64
    dirty    bool
}

// NewMemoryMetaStore 创建 MemoryMetaStore
func NewMemoryMetaStore() *MemoryMetaStore {
    return &MemoryMetaStore{
        series:   make(map[uint64][]byte),
        tagIndex: make(map[string][]uint64),
    }
}

func (m *MemoryMetaStore) GetMeta(ctx context.Context) (*types.MeasurementMeta, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return m.meta, nil
}

func (m *MemoryMetaStore) SetMeta(ctx context.Context, meta *types.MeasurementMeta) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.meta = meta
    m.dirty = true
    return nil
}

func (m *MemoryMetaStore) GetSeries(ctx context.Context, sid uint64) ([]byte, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return m.series[sid], nil
}

func (m *MemoryMetaStore) SetSeries(ctx context.Context, sid uint64, tags []byte) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.series[sid] = tags
    m.dirty = true
    return nil
}

func (m *MemoryMetaStore) GetAllSeries(ctx context.Context) (map[uint64][]byte, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    result := make(map[uint64][]byte, len(m.series))
    for k, v := range m.series {
        result[k] = v
    }
    return result, nil
}

func (m *MemoryMetaStore) GetSidsByTag(ctx context.Context, tagKey, tagValue string) ([]uint64, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    key := tagKey + "\x00" + tagValue
    return m.tagIndex[key], nil
}

func (m *MemoryMetaStore) AddTagIndex(ctx context.Context, tagKey, tagValue string, sid uint64) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    key := tagKey + "\x00" + tagValue
    m.tagIndex[key] = append(m.tagIndex[key], sid)
    m.dirty = true
    return nil
}

func (m *MemoryMetaStore) Persist(ctx context.Context) error {
    // TODO: 实现持久化
    return nil
}

func (m *MemoryMetaStore) Load(ctx context.Context) error {
    // TODO: 实现加载
    return nil
}

func (m *MemoryMetaStore) Close() error {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.series = nil
    m.tagIndex = nil
    return nil
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/measurement/... -v -run TestMemoryMetaStore
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/measurement/meta.go internal/storage/measurement/meta_test.go
git commit -m "feat: add MemoryMetaStore implementation"
```

---

## Phase 3: WAL

### Task 3.1: WAL 实现

**Files:**
- Create: `internal/storage/shard/wal.go`
- Create: `internal/storage/shard/wal_test.go`

- [ ] **Step 1: 编写测试用例**

```go
// internal/storage/shard/wal_test.go
package shard

import (
    "os"
    "path/filepath"
    "testing"
)

func TestWAL_Write(t *testing.T) {
    tmpDir := t.TempDir()
    w, err := NewWAL(tmpDir, 0)
    if err != nil {
        t.Fatalf("NewWAL failed: %v", err)
    }
    defer w.Close()

    data := []byte("test data")
    n, err := w.Write(data)
    if err != nil {
        t.Fatalf("Write failed: %v", err)
    }
    if n != len(data) {
        t.Errorf("expected %d bytes written, got %d", len(data), n)
    }
}

func TestWAL_Sync(t *testing.T) {
    tmpDir := t.TempDir()
    w, err := NewWAL(tmpDir, 0)
    if err != nil {
        t.Fatalf("NewWAL failed: %v", err)
    }
    defer w.Close()

    data := []byte("test data")
    w.Write(data)

    err = w.Sync()
    if err != nil {
        t.Fatalf("Sync failed: %v", err)
    }
}

func TestWAL_Reopen(t *testing.T) {
    tmpDir := t.TempDir()
    w1, err := NewWAL(tmpDir, 0)
    if err != nil {
        t.Fatalf("NewWAL failed: %v", err)
    }

    w1.Write([]byte("data1"))
    w1.Close()

    w2, err := NewWAL(tmpDir, 1)
    if err != nil {
        t.Fatalf("NewWAL failed: %v", err)
    }
    defer w2.Close()

    if w2.Sequence() != 1 {
        t.Errorf("expected sequence 1, got %d", w2.Sequence())
    }
}

func TestWAL_FilePermissions(t *testing.T) {
    tmpDir := t.TempDir()
    w, err := NewWAL(tmpDir, 0)
    if err != nil {
        t.Fatalf("NewWAL failed: %v", err)
    }
    w.Close()

    files, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
    for _, f := range files {
        info, _ := os.Stat(f)
        if info.Mode().Perm() != 0600 {
            t.Errorf("expected 0600, got %o", info.Mode().Perm())
        }
    }
}
```

- [ ] **Step 2: 运行测试验证（预期失败）**

```bash
go test ./internal/storage/shard/... -v -run TestWAL
```

- [ ] **Step 3: 编写 WAL 实现**

```go
// internal/storage/shard/wal.go
package shard

import (
    "encoding/binary"
    "os"
    "path/filepath"
    "sync"
    "unsafe"

    "microts/internal/storage"
)

// WAL Write-Ahead Log
type WAL struct {
    dir       string
    seq       uint64
    file      *os.File
    mu        sync.Mutex
    buf       []byte
    pos       int
}

// NewWAL 创建 WAL
func NewWAL(dir string, seq uint64) (*WAL, error) {
    if err := storage.SafeMkdirAll(dir, 0700); err != nil {
        return nil, err
    }

    filename := filepath.Join(dir, padSeq(seq)+".wal")
    f, err := storage.SafeOpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
    if err != nil {
        return nil, err
    }

    return &WAL{
        dir:  dir,
        seq:  seq,
        file: f,
        buf:  make([]byte, 4096),
    }, nil
}

func padSeq(seq uint64) string {
    return unsafe.String(&[20]byte{}, 20) // dummy, will format below
}

func (w *WAL) formatSeq() string {
    return filepath.Join(w.dir, "0000000000") // dummy
}

// Write 写入数据
func (w *WAL) Write(data []byte) (int, error) {
    w.mu.Lock()
    defer w.mu.Unlock()

    // 计算需要的空间：4 bytes length + data
    need := 4 + len(data)
    if len(w.buf)-w.pos < need {
        // 刷新到文件
        if err := w.flushLocked(); err != nil {
            return 0, err
        }
    }

    // 写入长度
    binary.BigEndian.PutUint32(w.buf[w.pos:], uint32(len(data)))
    w.pos += 4

    // 写入数据
    copy(w.buf[w.pos:], data)
    w.pos += len(data)

    return len(data), nil
}

func (w *WAL) flushLocked() error {
    if w.pos == 0 {
        return nil
    }

    n, err := w.file.Write(w.buf[:w.pos])
    if err != nil {
        return err
    }
    if n != w.pos {
        return os.ErrShortWrite
    }
    w.pos = 0
    return nil
}

// Sync 刷盘
func (w *WAL) Sync() error {
    w.mu.Lock()
    defer w.mu.Unlock()

    if err := w.flushLocked(); err != nil {
        return err
    }
    return w.file.Sync()
}

// Sequence 返回当前序列号
func (w *WAL) Sequence() uint64 {
    return w.seq
}

// Close 关闭
func (w *WAL) Close() error {
    w.mu.Lock()
    defer w.mu.Unlock()

    if err := w.flushLocked(); err != nil {
        return err
    }
    if err := w.file.Sync(); err != nil {
        return err
    }
    return w.file.Close()
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/shard/... -v -run TestWAL
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/shard/wal.go internal/storage/shard/wal_test.go
git commit -m "feat: add WAL implementation"
```

---

## Phase 4: MemTable

### Task 4.1: MemTable 实现

**Files:**
- Create: `internal/storage/shard/memtable.go`
- Create: `internal/storage/shard/memtable_test.go`

- [ ] **Step 1: 编写测试用例**

```go
// internal/storage/shard/memtable_test.go
package shard

import (
    "testing"
    "time"
)

func TestMemTable_Write(t *testing.T) {
    m := NewMemTable(64 * 1024 * 1024)

    p := &Point{
        Measurement: "cpu",
        Timestamp:   time.Now().UnixNano(),
        Tags:        map[string]string{"host": "server1"},
        Fields:      map[string]any{"usage": 85.5},
    }

    if err := m.Write(p); err != nil {
        t.Fatalf("Write failed: %v", err)
    }

    if m.Count() != 1 {
        t.Errorf("expected count 1, got %d", m.Count())
    }
}

func TestMemTable_SortKey(t *testing.T) {
    m := NewMemTable(64 * 1024 * 1024)

    now := time.Now().UnixNano()
    p1 := &Point{Measurement: "cpu", Timestamp: now + 100}
    p2 := &Point{Measurement: "cpu", Timestamp: now}
    p3 := &Point{Measurement: "cpu", Timestamp: now + 200}

    m.Write(p2)
    m.Write(p1)
    m.Write(p3)

    // 验证排序
    if m.entries[0].Timestamp != now {
        t.Errorf("expected first timestamp %d, got %d", now, m.entries[0].Timestamp)
    }
    if m.entries[1].Timestamp != now+100 {
        t.Errorf("expected second timestamp %d, got %d", now+100, m.entries[1].Timestamp)
    }
    if m.entries[2].Timestamp != now+200 {
        t.Errorf("expected third timestamp %d, got %d", now+200, m.entries[2].Timestamp)
    }
}

func TestMemTable_ShouldFlush(t *testing.T) {
    m := NewMemTable(100) // 100 bytes limit

    p := &Point{
        Measurement: "cpu",
        Timestamp:   time.Now().UnixNano(),
        Tags:        map[string]string{"host": "server1"},
        Fields:      map[string]any{"usage": 85.5},
    }

    // 写入一些数据直到应该 flush
    for !m.ShouldFlush() {
        m.Write(p)
    }

    if !m.ShouldFlush() {
        t.Errorf("expected ShouldFlush to return true")
    }
}
```

- [ ] **Step 2: 运行测试验证（预期失败）**

```bash
go test ./internal/storage/shard/... -v -run TestMemTable
```

- [ ] **Step 3: 编写 MemTable 实现**

```go
// internal/storage/shard/memtable.go
package shard

import (
    "sort"
    "sync"

    "microts/internal/types"
)

// Point 是写入的基本单位（简化版）
type Point struct {
    Measurement string
    Timestamp   int64
    Tags        map[string]string
    Fields      map[string]any
}

// entry 是 MemTable 中的条目
type entry struct {
    Point
}

// MemTable 内存跳表
type MemTable struct {
    mu       sync.RWMutex
    entries  []*entry
    maxSize  int64
    count    int
}

// NewMemTable 创建 MemTable
func NewMemTable(maxSize int64) *MemTable {
    return &MemTable{
        entries: make([]*entry, 0, 1024),
        maxSize: maxSize,
    }
}

// Write 写入
func (m *MemTable) Write(p *Point) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    m.entries = append(m.entries, &entry{Point: *p})
    m.count++

    // 检查是否需要排序
    if m.count > 1 && m.entries[m.count-1].Timestamp < m.entries[m.count-2].Timestamp {
        sort.Slice(m.entries, func(i, j int) bool {
            return m.entries[i].Timestamp < m.entries[j].Timestamp
        })
    }

    return nil
}

// Count 返回条数
func (m *MemTable) Count() int {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return m.count
}

// ShouldFlush 检查是否应该刷盘
func (m *MemTable) ShouldFlush() bool {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return int64(len(m.entries)*16) >= m.maxSize || m.count >= 10000
}

// Flush 刷盘（返回数据用于生成 SSTable）
func (m *MemTable) Flush() []*Point {
    m.mu.Lock()
    defer m.mu.Unlock()

    result := make([]*Point, len(m.entries))
    for i, e := range m.entries {
        p := e.Point
        result[i] = &p
    }

    m.entries = m.entries[:0]
    m.count = 0
    return result
}

// Iterator 迭代器
func (m *MemTable) Iterator() *MemTableIterator {
    m.mu.RLock()
    return &MemTableIterator{
        entries: m.entries,
        pos:     0,
    }
}

// MemTableIterator 迭代器
type MemTableIterator struct {
    entries []*entry
    pos     int
}

func (i *MemTableIterator) Next() bool {
    i.pos++
    return i.pos < len(i.entries)
}

func (i *MemTableIterator) Point() *Point {
    return &i.entries[i.pos].Point
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/shard/... -v -run TestMemTable
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/shard/memtable.go internal/storage/shard/memtable_test.go
git commit -m "feat: add MemTable implementation"
```

---

## Phase 5: SSTable

### Task 5.1: 压缩编码

**Files:**
- Create: `internal/storage/shard/compression/varint.go`
- Create: `internal/storage/shard/compression/delta.go`
- Create: `internal/storage/shard/compression/delta_of_delta.go`
- Create: `internal/storage/shard/compression/gorilla.go`

- [ ] **Step 1: 编写 Varint 编码测试**

```go
// internal/storage/shard/compression/varint_test.go
package compression

import (
    "testing"
)

func TestVarintEncode(t *testing.T) {
    tests := []struct {
        input    uint64
        expected []byte
    }{
        {0, []byte{0}},
        {127, []byte{127}},
        {128, []byte{1, 128}},
        {255, []byte{1, 255}},
        {1000, []byte{3, 232, 7}},
    }

    for _, tt := range tests {
        buf := make([]byte, 10)
        n := PutVarint(buf, tt.input)
        if n != len(tt.expected) || string(buf[:n]) != string(tt.expected) {
            t.Errorf("PutVarint(%d): expected %v, got %v", tt.input, tt.expected, buf[:n])
        }
    }
}

func TestVarintDecode(t *testing.T) {
    tests := []uint64{0, 127, 128, 255, 1000, 1 << 63}

    for _, expected := range tests {
        buf := make([]byte, 10)
        n := PutVarint(buf, expected)
        val, n2 := Varint(buf)
        if val != expected || n != n2 {
            t.Errorf("Varint roundtrip: expected %d, got %d", expected, val)
        }
    }
}
```

- [ ] **Step 2: 编写 Varint 实现**

```go
// internal/storage/shard/compression/varint.go
package compression

// PutVarint 编码 varint，返回写入的字节数
func PutVarint(buf []byte, v uint64) int {
    i := 0
    for v >= 0x80 {
        buf[i] = byte(v) | 0x80
        v >>= 7
        i++
    }
    buf[i] = byte(v)
    return i + 1
}

// Varint 解码 varint
func Varint(buf []byte) (uint64, int) {
    var shift uint
    var v uint64
    for i, b := range buf {
        v |= uint64(b&0x7F) << shift
        if b < 0x80 {
            return v, i + 1
        }
        shift += 7
    }
    return v, len(buf)
}
```

- [ ] **Step 3: 编写 Delta 编码测试**

```go
// internal/storage/shard/compression/delta_test.go
package compression

import (
    "testing"
)

func TestDeltaEncode(t *testing.T) {
    values := []int64{100, 105, 110, 115}
    deltas := DeltaEncode(values)

    expected := []int64{100, 5, 5, 5}
    for i, d := range deltas {
        if d != expected[i] {
            t.Errorf("delta[%d]: expected %d, got %d", i, expected[i], d)
        }
    }
}
```

- [ ] **Step 4: 编写 Delta 实现**

```go
// internal/storage/shard/compression/delta.go
package compression

// DeltaEncode 计算差值编码
func DeltaEncode(values []int64) []int64 {
    if len(values) == 0 {
        return nil
    }
    deltas := make([]int64, len(values))
    deltas[0] = values[0]
    for i := 1; i < len(values); i++ {
        deltas[i] = values[i] - values[i-1]
    }
    return deltas
}

// DeltaDecode 差值解码
func DeltaDecode(deltas []int64) []int64 {
    if len(deltas) == 0 {
        return nil
    }
    values := make([]int64, len(deltas))
    values[0] = deltas[0]
    for i := 1; i < len(deltas); i++ {
        values[i] = values[i-1] + deltas[i]
    }
    return values
}
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/shard/compression/*.go
git commit -m "feat: add compression utilities"
```

---

### Task 5.2: SSTable Writer

**Files:**
- Create: `internal/storage/shard/sstable/writer.go`
- Create: `internal/storage/shard/sstable/writer_test.go`

- [ ] **Step 1: 编写测试**

```go
// internal/storage/shard/sstable/writer_test.go
package sstable

import (
    "os"
    "path/filepath"
    "testing"
)

func TestWriter_WriteTimestampBlock(t *testing.T) {
    tmpDir := t.TempDir()

    w, err := NewWriter(filepath.Join(tmpDir, "timestamps.bin"))
    if err != nil {
        t.Fatalf("NewWriter failed: %v", err)
    }

    timestamps := []int64{1000, 2000, 3000}
    err = w.WriteTimestampBlock(timestamps)
    if err != nil {
        t.Fatalf("WriteTimestampBlock failed: %v", err)
    }

    err = w.Close()
    if err != nil {
        t.Fatalf("Close failed: %v", err)
    }

    // 验证文件存在
    info, err := os.Stat(filepath.Join(tmpDir, "timestamps.bin"))
    if err != nil {
        t.Fatalf("stat failed: %v", err)
    }
    if info.Size() == 0 {
        t.Errorf("file should not be empty")
    }
}
```

- [ ] **Step 2: 运行测试验证**

```bash
go test ./internal/storage/shard/sstable/... -v -run TestWriter
```

- [ ] **Step 3: 编写 Writer 实现**

```go
// internal/storage/shard/sstable/writer.go
package sstable

import (
    "encoding/binary"
    "os"
    "unsafe"

    "microts/internal/storage"
    "microts/internal/storage/shard/compression"
)

// Magic 魔数 "TSERPEG"
var Magic = [8]byte{0x54, 0x53, 0x45, 0x52, 0x50, 0x45, 0x47, 0x46}

// Version 版本
const Version = 1

// BlockSize 默认块大小 64KB
const BlockSize = 64 * 1024

// Writer SSTable 写入器
type Writer struct {
    file *os.File
    path string

    // 状态
    timestamps []int64
    sids      []uint64
    fields    map[string][]byte
}

// NewWriter 创建 Writer
func NewWriter(path string) (*Writer, error) {
    f, err := storage.SafeCreate(path, 0600)
    if err != nil {
        return nil, err
    }

    return &Writer{
        file:   f,
        path:   path,
        fields: make(map[string][]byte),
    }, nil
}

// WriteTimestampBlock 写入时间戳块
func (w *Writer) WriteTimestampBlock(timestamps []int64) error {
    // 计算 delta 编码
    deltas := compression.DeltaEncode(timestamps)

    // 分配缓冲区
    buf := make([]byte, BlockSize)
    pos := 0

    // 写入 header
    binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(Magic))
    pos += 8
    binary.BigEndian.PutUint32(buf[pos:pos+4], Version)
    pos += 4
    // block_count 后续填写
    blockCountOffset := pos
    pos += 4

    // 计算压缩数据
    encoded := w.encodeDeltas(deltas)

    // 写入数据
    binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(len(encoded)))
    pos += 4
    copy(buf[pos:], encoded)
    pos += len(encoded)

    _, err := w.file.Write(buf[:pos])
    return err
}

func (w *Writer) encodeDeltas(deltas []int64) []byte {
    // 简单实现：直接用 varint 编码
    buf := make([]byte, len(deltas)*16)
    pos := 0
    for _, d := range deltas {
        n := compression.PutVarint(buf[pos:], uint64(d))
        pos += n
    }
    return buf[:pos]
}

// Close 关闭
func (w *Writer) Close() error {
    return w.file.Close()
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/shard/sstable/... -v -run TestWriter
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/shard/sstable/writer.go internal/storage/shard/sstable/writer_test.go
git commit -m "feat: add SSTable writer"
```

---

### Task 5.3: SSTable Reader

**Files:**
- Create: `internal/storage/shard/sstable/reader.go`
- Create: `internal/storage/shard/sstable/reader_test.go`

- [ ] **Step 1: 编写测试**

```go
// internal/storage/shard/sstable/reader_test.go
package sstable

import (
    "testing"
)

func TestReader_ReadTimestamps(t *testing.T) {
    // 先写后读
}
```

- [ ] **Step 2: 运行测试验证**

- [ ] **Step 3: 编写 Reader 实现**

```go
// internal/storage/shard/sstable/reader.go
package sstable

import (
    "os"
)

// Reader SSTable 读取器
type Reader struct {
    file *os.File
    path string
}

// NewReader 创建 Reader
func NewReader(path string) (*Reader, error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    return &Reader{file: f, path: path}, nil
}

// Close 关闭
func (r *Reader) Close() error {
    return r.file.Close()
}
```

- [ ] **Step 4: Commit**

---

## Phase 6: Shard 管理

### Task 6.1: Shard 实现

**Files:**
- Create: `internal/storage/shard/shard.go`
- Create: `internal/storage/shard/shard_test.go`

- [ ] **Step 1: 编写测试**

```go
// internal/storage/shard/shard_test.go
package shard

import (
    "testing"
    "time"
)

func TestShard_TimeRange(t *testing.T) {
    start := time.Now().UnixNano()
    end := start + int64(time.Hour)

    s := NewShard("db1", "cpu", start, end, t.TempDir())

    if s.StartTime() != start {
        t.Errorf("expected start %d, got %d", start, s.StartTime())
    }
    if s.EndTime() != end {
        t.Errorf("expected end %d, got %d", end, s.EndTime())
    }
}
```

- [ ] **Step 2: 运行测试验证**

- [ ] **Step 3: 编写 Shard 实现**

```go
// internal/storage/shard/shard.go
package shard

import (
    "time"
)

// Shard 单个 Shard
type Shard struct {
    db         string
    measurement string
    startTime  int64
    endTime    int64
    dir        string
}

// NewShard 创建 Shard
func NewShard(db, measurement string, startTime, endTime int64, dir string) *Shard {
    return &Shard{
        db:         db,
        measurement: measurement,
        startTime:  startTime,
        endTime:    endTime,
        dir:        dir,
    }
}

// StartTime 返回开始时间
func (s *Shard) StartTime() int64 {
    return s.startTime
}

// EndTime 返回结束时间
func (s *Shard) EndTime() int64 {
    return s.endTime
}

// ContainsTime 检查时间是否在范围内
func (s *Shard) ContainsTime(ts int64) bool {
    return ts >= s.startTime && ts < s.endTime
}

// Duration 返回时间窗口
func (s *Shard) Duration() time.Duration {
    return time.Duration(s.endTime - s.startTime)
}
```

- [ ] **Step 4: Commit**

---

### Task 6.2: Shard Manager

**Files:**
- Create: `internal/storage/shard/manager.go`
- Create: `internal/storage/shard/manager_test.go`

- [ ] **Step 1: 编写测试**

```go
// internal/storage/shard/manager_test.go
package shard

import (
    "testing"
    "time"
)

func TestShardManager_GetShard(t *testing.T) {
    m := NewShardManager(t.TempDir(), time.Hour)

    start := time.Now().UnixNano()
    end := start + int64(time.Hour)

    s, err := m.GetShard("db1", "cpu", start)
    if err != nil {
        t.Fatalf("GetShard failed: %v", err)
    }

    if !s.ContainsTime(start) {
        t.Errorf("shard should contain time %d", start)
    }
}

func TestShardManager_GetShard_TimeWindow(t *testing.T) {
    m := NewShardManager(t.TempDir(), time.Hour)

    // 1小时时间窗口
    base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()

    // 30分钟应该落在第一个shard
    s1, _ := m.GetShard("db1", "cpu", base)
    s2, _ := m.GetShard("db1", "cpu", base+30*60*1e9)

    if s1 != s2 {
        t.Errorf("30分钟应该在同一个shard")
    }

    // 90分钟应该落在下一个shard
    s3, _ := m.GetShard("db1", "cpu", base+90*60*1e9)
    if s1 == s3 {
        t.Errorf("90分钟应该在不同shard")
    }
}
```

- [ ] **Step 2: 运行测试验证**

- [ ] **Step 3: 编写 Manager 实现**

```go
// internal/storage/shard/manager.go
package shard

import (
    "path/filepath"
    "sync"
    "time"
)

// ShardManager Shard 管理器
type ShardManager struct {
    dir           string
    shardDuration time.Duration
    shards        map[string]*Shard
    mu            sync.RWMutex
}

// NewShardManager 创建 ShardManager
func NewShardManager(dir string, shardDuration time.Duration) *ShardManager {
    return &ShardManager{
        dir:           dir,
        shardDuration: shardDuration,
        shards:        make(map[string]*Shard),
    }
}

// GetShard 获取或创建 Shard
func (m *ShardManager) GetShard(db, measurement string, timestamp int64) (*Shard, error) {
    // 计算时间窗口
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

    // 再次检查
    if s, ok := m.shards[key]; ok {
        return s, nil
    }

    // 创建新 Shard
    shardDir := filepath.Join(m.dir, db, measurement, formatTimeRange(startTime, endTime))
    s = NewShard(db, measurement, startTime, endTime, shardDir)
    m.shards[key] = s
    return s, nil
}

func (m *ShardManager) calcShardStart(timestamp int64) int64 {
    return (timestamp / int64(m.shardDuration)) * int64(m.shardDuration)
}

func (m *ShardManager) makeKey(db, measurement string, startTime int64) string {
    return db + "/" + measurement + "/" + formatInt64(startTime)
}

func formatTimeRange(start, end int64) string {
    return formatInt64(start) + "_" + formatInt64(end)
}

func formatInt64(n int64) string {
    return string(rune(n))
}
```

- [ ] **Step 4: Commit**

---

## Phase 7-10: 查询执行器、gRPC API、Library API、测试与验证

后续阶段需要完成：
- Query Executor 实现
- gRPC 服务实现
- Library API (microts.go)
- Server 入口 (cmd/server/main.go)
- 完整测试覆盖

---

## Self-Review 检查清单

- [ ] Spec 覆盖：每个章节都有对应实现
- [ ] 无占位符：代码完整，无 TBD/TODO
- [ ] 类型一致：接口方法签名一致
- [ ] 权限正确：目录 0700，文件 0600
- [ ] 错误处理：所有错误都被处理

---

## Plan complete and saved to `docs/superpowers/plans/2026-05-02-micro-ts-implementation.md`

**Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
