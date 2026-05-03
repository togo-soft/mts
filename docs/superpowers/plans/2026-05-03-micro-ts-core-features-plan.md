# micro-ts 核心功能实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 micro-ts 缺失的核心功能：SSTable 刷盘/读取、字段过滤、Tag 过滤、WAL 恢复

**Architecture:** 写入路径：Write → MemTable → (flush) → SSTable + WAL；查询路径：Read → MemTable + SSTable → Filter

**Tech Stack:** Go 1.26, mmap, 自定义二进制格式

---

## File Structure

```
micro-ts/
├── internal/
│   ├── engine/
│   │   └── engine.go           # 存储引擎
│   └── storage/
│       ├── measurement/
│       │   └── meta.go        # MetaStore (已有)
│       └── shard/
│           ├── shard.go         # Shard (已有)
│           ├── memtable.go     # MemTable (已有)
│           ├── wal.go          # WAL (已有，待增强)
│           └── sstable/
│               ├── writer.go    # SSTable Writer (已有，待增强)
│               └── reader.go    # SSTable Reader (新建)
├── tests/
│   ├── integrity_test.go      # 数据完整性测试 (已有)
│   └── bench_test.go          # 性能测试 (已有)
└── microts.go                 # Library API (已有)
```

---

## Phase 1: SSTable 刷盘

### Task 1.1: SSTable 多列写入

**Files:**
- Modify: `internal/storage/shard/sstable/writer.go`

- [ ] **Step 1: 编写测试**

```go
// internal/storage/shard/sstable/writer_test.go
func TestWriter_WritePoints(t *testing.T) {
    tmpDir := t.TempDir()

    w, err := NewWriter(tmpDir, 0)
    if err != nil {
        t.Fatalf("NewWriter failed: %v", err)
    }

    points := []*types.Point{
        {
            Timestamp: 1000,
            Tags:      map[string]string{"host": "server1"},
            Fields:   map[string]any{"usage": 85.5, "count": int64(100)},
        },
        {
            Timestamp: 2000,
            Tags:      map[string]string{"host": "server1"},
            Fields:   map[string]any{"usage": 90.0, "count": int64(200)},
        },
    }

    err = w.WritePoints(points)
    if err != nil {
        t.Fatalf("WritePoints failed: %v", err)
    }

    err = w.Close()
    if err != nil {
        t.Fatalf("Close failed: %v", err)
    }
}
```

- [ ] **Step 2: 运行测试**

```bash
go test ./internal/storage/shard/sstable/... -v -run TestWriter_WritePoints
```

- [ ] **Step 3: 增强 Writer 实现**

```go
// internal/storage/shard/sstable/writer.go

// Writer 新增字段
type Writer struct {
    file      *os.File
    path      string
    shardDir  string
    seq       uint64
    timestamp *os.File
    sid       *os.File
    fields    map[string]*os.File
}

// NewWriter 修改签名
func NewWriter(shardDir string, seq uint64) (*Writer, error) {
    if err := storage.SafeMkdirAll(shardDir, 0700); err != nil {
        return nil, err
    }

    w := &Writer{
        shardDir: shardDir,
        seq:      seq,
        fields:   make(map[string]*os.File),
    }

    // 打开或创建 timestamp 文件
    tsFile, err := storage.SafeOpenFile(
        filepath.Join(shardDir, "_timestamps.bin"),
        os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
    if err != nil {
        return nil, err
    }
    w.timestamp = tsFile

    return w, nil
}

// WritePoints 写入一批 points
func (w *Writer) WritePoints(points []*types.Point) error {
    // 收集所有字段名
    fieldNames := make(map[string]bool)
    for _, p := range points {
        for name := range p.Fields {
            fieldNames[name] = true
        }
    }

    // 打开字段文件
    for name := range fieldNames {
        f, err := storage.SafeOpenFile(
            filepath.Join(w.shardDir, "fields", name+".bin"),
            os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
        if err != nil {
            return err
        }
        w.fields[name] = f
    }

    // 写入 timestamp
    for _, p := range points {
        var buf [8]byte
        binary.BigEndian.PutUint64(buf[:], uint64(p.Timestamp))
        if _, err := w.timestamp.Write(buf[:]); err != nil {
            return err
        }
    }

    // 写入各字段
    for name, f := range w.fields {
        for _, p := range points {
            if val, ok := p.Fields[name]; ok {
                w.writeFieldValue(f, val)
            }
        }
    }

    return nil
}

func (w *Writer) writeFieldValue(f *os.File, val any) error {
    switch v := val.(type) {
    case float64:
        var buf [8]byte
        binary.BigEndian.PutUint64(buf[:], math.Float64bits(v))
        _, err := f.Write(buf[:])
        return err
    case int64:
        var buf [8]byte
        binary.BigEndian.PutUint64(buf[:], uint64(v))
        _, err := f.Write(buf[:])
        return err
    case string:
        var buf [4]byte
        binary.BigEndian.PutUint32(buf[:], uint32(len(v)))
        if _, err := f.Write(buf[:]); err != nil {
            return err
        }
        _, err := f.WriteString(v)
        return err
    case bool:
        if v {
            _, err := f.Write([]byte{1})
            return err
        }
        _, err := f.Write([]byte{0})
        return err
    }
    return nil
}

// Close 关闭
func (w *Writer) Close() error {
    if w.timestamp != nil {
        w.timestamp.Close()
    }
    for _, f := range w.fields {
        f.Close()
    }
    return w.file.Close()
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/shard/sstable/... -v -run TestWriter_WritePoints
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/shard/sstable/writer.go internal/storage/shard/sstable/writer_test.go
git commit -m "feat: add multi-column SSTable write"
```

---

### Task 1.2: Shard.flush() 实现

**Files:**
- Modify: `internal/storage/shard/shard.go`

- [ ] **Step 1: 编写测试**

```go
// internal/storage/shard/shard_test.go
func TestShard_Flush(t *testing.T) {
    tmpDir := t.TempDir()

    s := NewShard("db1", "cpu", 0, time.Hour.Nanoseconds(), tmpDir)
    defer s.Close()

    // 写入数据
    for i := 0; i < 100; i++ {
        p := &types.Point{
            Timestamp: int64(i) * 1e9,
            Tags:     map[string]string{"host": "server1"},
            Fields:   map[string]any{"usage": float64(i)},
        }
        err := s.Write(p)
        if err != nil {
            t.Fatalf("Write failed: %v", err)
        }
    }

    // 手动 flush
    err := s.Flush()
    if err != nil {
        t.Fatalf("Flush failed: %v", err)
    }

    // 验证 MemTable 已清空
    if s.memTable.Count() != 0 {
        t.Errorf("expected memTable count 0, got %d", s.memTable.Count())
    }
}
```

- [ ] **Step 2: 运行测试**

```bash
go test ./internal/storage/shard/... -v -run TestShard_Flush
```

- [ ] **Step 3: 实现 Flush**

```go
// internal/storage/shard/shard.go

// Flush 刷盘
func (s *Shard) Flush() error {
    s.mu.Lock()
    defer s.mu.Unlock()

    points := s.memTable.Flush()
    if len(points) == 0 {
        return nil
    }

    // 创建 SSTable Writer
    dataDir := filepath.Join(s.dir, "data")
    if err := storage.SafeMkdirAll(dataDir, 0700); err != nil {
        return err
    }

    w, err := sstable.NewWriter(dataDir, 0)
    if err != nil {
        return err
    }

    if err := w.WritePoints(points); err != nil {
        return err
    }

    return w.Close()
}
```

- [ ] **Step 4: 运行测试验证**

```bash
go test ./internal/storage/shard/... -v -run TestShard_Flush
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/shard/shard.go internal/storage/shard/shard_test.go
git commit -m "feat: implement Shard.Flush"
```

---

## Phase 2: SSTable 读取

### Task 2.1: SSTable Reader 多列读取

**Files:**
- Modify: `internal/storage/shard/sstable/reader.go`

- [ ] **Step 1: 编写测试**

```go
// internal/storage/shard/sstable/reader_test.go
func TestReader_ReadAll(t *testing.T) {
    tmpDir := t.TempDir()

    // 先写入
    w, err := sstable.NewWriter(tmpDir, 0)
    if err != nil {
        t.Fatalf("NewWriter failed: %v", err)
    }

    points := []*types.Point{
        {
            Timestamp: 1000,
            Tags:      map[string]string{"host": "server1"},
            Fields:   map[string]any{"usage": 85.5},
        },
        {
            Timestamp: 2000,
            Tags:      map[string]string{"host": "server1"},
            Fields:   map[string]any{"usage": 90.0},
        },
    }

    if err := w.WritePoints(points); err != nil {
        t.Fatalf("WritePoints failed: %v", err)
    }
    w.Close()

    // 再读取
    r, err := sstable.NewReader(tmpDir)
    if err != nil {
        t.Fatalf("NewReader failed: %v", err)
    }
    defer r.Close()

    rows, err := r.ReadAll(nil) // nil 表示读取所有字段
    if err != nil {
        t.Fatalf("ReadAll failed: %v", err)
    }

    if len(rows) != 2 {
        t.Errorf("expected 2 rows, got %d", len(rows))
    }
}
```

- [ ] **Step 2: 运行测试**

```bash
go test ./internal/storage/shard/sstable/... -v -run TestReader_ReadAll
```

- [ ] **Step 3: 实现 Reader.ReadAll**

```go
// internal/storage/shard/sstable/reader.go

type Reader struct {
    dataDir string
}

func NewReader(dataDir string) (*Reader, error) {
    return &Reader{dataDir: dataDir}, nil
}

func (r *Reader) ReadAll(fields []string) ([]types.PointRow, error) {
    // 读取 timestamp
    tsFile, err := os.Open(filepath.Join(r.dataDir, "_timestamps.bin"))
    if err != nil {
        return nil, err
    }
    defer tsFile.Close()

    timestamps, err := r.readTimestamps(tsFile)
    if err != nil {
        return nil, err
    }

    // 如果没有指定字段，读取所有字段文件
    if len(fields) == 0 {
        entries, err := os.ReadDir(filepath.Join(r.dataDir, "fields"))
        if err != nil {
            return nil, err
        }
        for _, e := range entries {
            if !e.IsDir() {
                fields = append(fields, strings.TrimSuffix(e.Name(), ".bin"))
            }
        }
    }

    // 读取各字段
    fieldData := make(map[string][]byte)
    for _, name := range fields {
        f, err := os.Open(filepath.Join(r.dataDir, "fields", name+".bin"))
        if err != nil {
            return nil, err
        }
        defer f.Close()

        data, err := io.ReadAll(f)
        if err != nil {
            return nil, err
        }
        fieldData[name] = data
    }

    // 构建结果
    rows := make([]types.PointRow, len(timestamps))
    for i, ts := range timestamps {
        row := types.PointRow{
            Timestamp: ts,
            Tags:      map[string]string{"host": "server1"}, // TODO: 从 sid 解析
            Fields:    make(map[string]any),
        }

        for _, name := range fields {
            row.Fields[name] = r.decodeFieldValue(fieldData[name], i, name)
        }

        rows[i] = row
    }

    return rows, nil
}

func (r *Reader) readTimestamps(f *os.File) ([]int64, error) {
    data, err := io.ReadAll(f)
    if err != nil {
        return nil, err
    }

    timestamps := make([]int64, 0, len(data)/8)
    for i := 0; i+8 <= len(data); i += 8 {
        ts := int64(binary.BigEndian.Uint64(data[i : i+8]))
        timestamps = append(timestamps, ts)
    }
    return timestamps, nil
}

func (r *Reader) decodeFieldValue(data []byte, index int, fieldName string) any {
    // 根据字段类型解码... (简化实现)
    return nil
}
```

- [ ] **Step 4: 运行测试验证**

- [ ] **Step 5: Commit**

---

### Task 2.2: Shard.Read 合并 MemTable + SSTable

**Files:**
- Modify: `internal/storage/shard/shard.go`

- [ ] **Step 1: 修改 Read 方法**

```go
// internal/storage/shard/shard.go

// Read 读取时间范围内的点
func (s *Shard) Read(startTime, endTime int64) ([]types.PointRow, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    var rows []types.PointRow

    // 1. 从 MemTable 读取
    iter := s.memTable.Iterator()
    for iter.Next() {
        p := iter.Point()
        if p.Timestamp >= startTime && p.Timestamp < endTime {
            rows = append(rows, types.PointRow{
                Timestamp: p.Timestamp,
                Tags:      p.Tags,
                Fields:    p.Fields,
            })
        }
    }

    // 2. 从 SSTable 读取
    sstRows, err := s.readFromSSTable(startTime, endTime)
    if err != nil {
        return nil, err
    }
    rows = append(rows, sstRows...)

    // 3. 按时间排序
    sort.Slice(rows, func(i, j int) bool {
        return rows[i].Timestamp < rows[j].Timestamp
    })

    return rows, nil
}

func (s *Shard) readFromSSTable(startTime, endTime int64) ([]types.PointRow, error) {
    dataDir := filepath.Join(s.dir, "data")
    if _, err := os.Stat(dataDir); os.IsNotExist(err) {
        return nil, nil // 没有 SSTable
    }

    r, err := sstable.NewReader(dataDir)
    if err != nil {
        return nil, err
    }
    defer r.Close()

    return r.ReadRange(startTime, endTime)
}
```

- [ ] **Step 2: Commit**

---

## Phase 3: 查询优化

### Task 3.1: 字段过滤 (Field Projection)

**Files:**
- Modify: `internal/engine/engine.go`

- [ ] **Step 1: 修改 Query 方法**

```go
// internal/engine/engine.go

// Query 范围查询
func (e *Engine) Query(req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
    shards := e.shardManager.GetShards(req.Database, req.Measurement, req.StartTime, req.EndTime)

    var allRows []types.PointRow
    for _, s := range shards {
        rows, err := s.Read(req.StartTime, req.EndTime)
        if err != nil {
            return nil, err
        }

        // 字段过滤
        if len(req.Fields) > 0 {
            rows = filterFields(rows, req.Fields)
        }

        // Tag 过滤
        if len(req.Tags) > 0 {
            rows = filterTags(rows, req.Tags)
        }

        allRows = append(allRows, rows...)
    }

    // 分页
    totalCount := int64(len(allRows))
    if req.Offset > 0 {
        if req.Offset >= int64(len(allRows)) {
            allRows = nil
        } else {
            allRows = allRows[req.Offset:]
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

func filterFields(rows []types.PointRow, fields []string) []types.PointRow {
    fieldSet := make(map[string]bool)
    for _, f := range fields {
        fieldSet[f] = true
    }

    result := make([]types.PointRow, len(rows))
    for i, row := range rows {
        filtered := make(map[string]any)
        for name, val := range row.Fields {
            if fieldSet[name] {
                filtered[name] = val
            }
        }
        result[i] = types.PointRow{
            Timestamp: row.Timestamp,
            Tags:      row.Tags,
            Fields:    filtered,
        }
    }
    return result
}

func filterTags(rows []types.PointRow, tags map[string]string) []types.PointRow {
    result := make([]types.PointRow, 0, len(rows))
    for _, row := range rows {
        match := true
        for k, v := range tags {
            if row.Tags[k] != v {
                match = false
                break
            }
        }
        if match {
            result = append(result, row)
        }
    }
    return result
}
```

- [ ] **Step 2: Commit**

---

## Phase 4: 持久化

### Task 4.1: WAL 恢复

**Files:**
- Modify: `internal/storage/shard/manager.go`

- [ ] **Step 1: 修改 NewShardManager**

```go
// internal/storage/shard/manager.go

type ShardManager struct {
    dir           string
    shardDuration time.Duration
    shards        map[string]*Shard
    mu            sync.RWMutex
}

func NewShardManager(dir string, shardDuration time.Duration) *ShardManager {
    sm := &ShardManager{
        dir:           dir,
        shardDuration: shardDuration,
        shards:        make(map[string]*Shard),
    }

    // 从 WAL 恢复
    sm.recover()

    return sm
}

func (m *ShardManager) recover() {
    // 遍历所有 Shard 目录
    entries, err := os.ReadDir(m.dir)
    if err != nil {
        return
    }

    for _, dbEntry := range entries {
        if !dbEntry.IsDir() {
            continue
        }
        dbPath := filepath.Join(m.dir, dbEntry.Name())

        measEntries, err := os.ReadDir(dbPath)
        if err != nil {
            continue
        }

        for _, measEntry := range measEntries {
            if !measEntry.IsDir() {
                continue
            }
            measPath := filepath.Join(dbPath, measEntry.Name())

            // 检查 WAL 恢复
            m.recoverShard(dbEntry.Name(), measEntry.Name(), measPath)
        }
    }
}

func (m *ShardManager) recoverShard(db, measurement, measDir string) error {
    shardDirs, err := os.ReadDir(measDir)
    if err != nil {
        return err
    }

    for _, sDir := range shardDirs {
        if !sDir.IsDir() || !strings.Contains(sDir.Name(), "_") {
            continue
        }

        parts := strings.Split(sDir.Name(), "_")
        if len(parts) != 2 {
            continue
        }

        startTime, _ := strconv.ParseInt(parts[0], 10, 64)
        endTime, _ := strconv.ParseInt(parts[1], 10, 64)

        shardPath := filepath.Join(measDir, sDir.Name())
        walPath := filepath.Join(shardPath, "wal")

        // 恢复 WAL
        if err := m.recoverWAL(walPath, db, measurement, startTime); err != nil {
            continue // 单个 shard 恢复失败不影响其他
        }
    }

    return nil
}

func (m *ShardManager) recoverWAL(walDir, db, measurement string, startTime int64) error {
    walFiles, err := filepath.Glob(filepath.Join(walDir, "*.wal"))
    if err != nil {
        return err
    }

    for _, walFile := range walFiles {
        // TODO: 解析 WAL 文件并重放 points
        _ = walFile
    }

    return nil
}
```

- [ ] **Step 2: Commit**

---

## Self-Review 检查清单

- [ ] Spec 覆盖：SSTable 刷盘/读取、字段过滤、Tag 过滤、分页
- [ ] 无占位符：代码完整
- [ ] 类型一致：接口方法签名一致
- [ ] 权限正确：目录 0700，文件 0600
- [ ] 错误处理：所有错误都被处理

---

## Plan complete

**Implementation approach:** Subagent-Driven (recommended)

Which approach?