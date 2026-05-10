# 移除 tsSidMap 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除 `Shard.tsSidMap`，将 SID 嵌入 MemTable entry，WritePoints 改为并行数组索引传递 SID。

**Architecture:** 将 SID 信息从 Shard 层的外挂 map 下沉到 MemTable entry 内部，使 SID 与数据点生命周期绑定。WritePoints 的 timestamp 查找改为 O(1) 索引对应。

**Tech Stack:** Go 1.x, 无外部依赖变更

---

### Task 1: MemTable — entry 加 Sid，改 Write/Flush 签名

**Files:**
- Modify: `internal/storage/memtable/memtable.go`

- [ ] **Step 1: entry 增加 Sid 字段**

修改 `entry` 结构体：
```go
type entry struct {
    Point types.Point
    Sid   uint64
}
```

- [ ] **Step 2: Write 签名加 sid 参数**

```go
// 旧
func (m *MemTable) Write(p *types.Point) error

// 新
func (m *MemTable) Write(p *types.Point, sid uint64) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    tags := make(map[string]string, len(p.Tags))
    for k, v := range p.Tags {
        tags[k] = v
    }

    fields := make(map[string]*types.FieldValue, len(p.Fields))
    for k, v := range p.Fields {
        fields[k] = v
    }

    m.entries = append(m.entries, &entry{
        Point: types.Point{
            Database:    p.Database,
            Measurement: p.Measurement,
            Tags:        tags,
            Timestamp:   p.Timestamp,
            Fields:      fields,
        },
        Sid: sid,
    })
    m.count++
    m.lastWrite = time.Now()

    if m.count > 1 && m.entries[m.count-1].Point.Timestamp < m.entries[m.count-2].Point.Timestamp {
        sort.Slice(m.entries, func(i, j int) bool {
            return m.entries[i].Point.Timestamp < m.entries[j].Point.Timestamp
        })
        m.sorted = true
    } else {
        m.sorted = true
    }

    return nil
}
```

- [ ] **Step 3: Flush 返回并行 slices**

```go
// 旧
func (m *MemTable) Flush() []*types.Point

// 新
func (m *MemTable) Flush() ([]*types.Point, []uint64) {
    m.mu.Lock()
    result := m.entries
    m.entries = nil
    m.count = 0
    m.sorted = false
    m.mu.Unlock()

    if len(result) == 0 {
        return nil, nil
    }

    points := make([]*types.Point, len(result))
    sids := make([]uint64, len(result))
    for i, e := range result {
        points[i] = &e.Point
        sids[i] = e.Sid
    }

    for i := range result {
        result[i] = nil
    }

    return points, sids
}
```

- [ ] **Step 4: 编译验证**

```bash
cd /root/projects/mts && go build ./internal/storage/memtable/...
```
Expected: 编译失败（调用方尚未适配）

---

### Task 2: MemTable 测试适配

**Files:**
- Modify: `internal/storage/memtable/memtable_test.go`
- Modify: `internal/storage/memtable/memtable_bench_test.go`

- [ ] **Step 1: 适配 memtable_test.go — 所有 Write 调用加 sid 参数**

将所有 `m.Write(p)` 改为 `m.Write(p, 0)`。涉及位置：L20, L37, L39, L41, L81, L106, L117, L145, L170。

搜索替换：`\.Write(p)` → `.Write(p, 0)`

- [ ] **Step 2: 适配 memtable_test.go — Flush 调用适配多返回值**

`FlushMultipleTimes` (L175-L178):
```go
// 旧
points := m.Flush()
if len(points) != 5 {

// 新
points, sids := m.Flush()
if len(points) != 5 || len(sids) != 5 {
```

- [ ] **Step 3: 适配 memtable_bench_test.go — Write 调用加 sid 参数**

所有 `mt.Write(p)` → `mt.Write(p, 0)`，`mts[i].Write(p)` → `mts[i].Write(p, 0)`。

L22, L40, L57, L77.

- [ ] **Step 4: 运行 MemTable 测试**

```bash
cd /root/projects/mts && go test ./internal/storage/memtable/... -v -count=1
```
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add internal/storage/memtable/memtable.go internal/storage/memtable/memtable_test.go internal/storage/memtable/memtable_bench_test.go
git commit -m "refactor(memtable): entry 携带 Sid，Write/Flush 签名变更"
```

---

### Task 3: SSTable Writer — WritePoints 签名改为并行数组

**Files:**
- Modify: `internal/storage/shard/sstable/writer_field.go`

- [ ] **Step 1: 改 WritePoints 签名和内部逻辑**

```go
// 旧
func (w *Writer) WritePoints(points []*types.Point, tsSidMap map[int64][]uint64) error {

// 新
// sids: 与 points 一一对应，len(sids)==0 时所有 sid 默认为 0。
func (w *Writer) WritePoints(points []*types.Point, sids []uint64) error {
    fieldNames := make(map[string]bool)
    for _, p := range points {
        for name, val := range p.Fields {
            fieldNames[name] = true
            if _, exists := w.schema.Fields[name]; !exists {
                w.schema.Fields[name] = detectFieldType(val)
            }
        }
    }

    for name := range fieldNames {
        f, err := storage.SafeOpenFile(
            filepath.Join(w.dataDir, "fields", name+".bin"),
            os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
        if err != nil {
            return fmt.Errorf("open field file %s: %w", name, err)
        }
        w.fields[name] = f

        w.fieldBufs[name] = make([]byte, 0, BlockSize)
        w.fieldSizes[name] = w.fieldTypeSize(w.schema.Fields[name])
    }

    for i, p := range points {
        var sid uint64
        if i < len(sids) {
            sid = sids[i]
        }
        if err := w.writePointWithSid(p, sid); err != nil {
            return fmt.Errorf("write point (timestamp=%d): %w", p.Timestamp, err)
        }
    }

    return nil
}
```

- [ ] **Step 2: 编译验证**

```bash
cd /root/projects/mts && go build ./internal/storage/shard/sstable/...
```
Expected: 编译成功（调用方尚未完全适配但 WritePoints 签名已更新）

---

### Task 4: SSTable Writer 测试适配

**Files:**
- Modify: `internal/storage/shard/sstable/writer_test.go`

- [ ] **Step 1: WritePoints 调用适配**

L32 `w.WritePoints(points, nil)` — nil 仍然兼容（len(nil)==0 → 所有 sid 为 0）。无需改动。

- [ ] **Step 2: 添加 sids 非零的测试用例**

在 `TestWriter_WritePoints` 末尾追加：
```go
func TestWriter_WritePointsWithSids(t *testing.T) {
    tmpDir := t.TempDir()
    w, err := NewWriter(tmpDir, 1, 0)
    if err != nil {
        t.Fatalf("NewWriter failed: %v", err)
    }

    points := []*types.Point{
        {
            Timestamp: 1000,
            Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(int64(1))},
        },
        {
            Timestamp: 2000,
            Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(int64(2))},
        },
    }
    sids := []uint64{42, 99}

    if err := w.WritePoints(points, sids); err != nil {
        t.Fatalf("WritePoints failed: %v", err)
    }
    if err := w.Close(); err != nil {
        t.Fatalf("Close failed: %v", err)
    }

    // 验证 _sids.bin 文件存在且非空
    sidPath := filepath.Join(tmpDir, "data", "sst_1", "_sids.bin")
    info, err := os.Stat(sidPath)
    if err != nil {
        t.Fatalf("stat sids file failed: %v", err)
    }
    if info.Size() < 16 {
        t.Errorf("sids file too small, expected at least 16 bytes, got %d", info.Size())
    }
}
```

- [ ] **Step 3: 运行测试**

```bash
cd /root/projects/mts && go test ./internal/storage/shard/sstable/... -v -count=1 -run TestWriter
```
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add internal/storage/shard/sstable/writer_field.go internal/storage/shard/sstable/writer_test.go
git commit -m "refactor(sstable): WritePoints 改为并行 sids 数组参数"
```

---

### Task 5: Shard — 删除 tsSidMap + 适配 Write 路径

**Files:**
- Modify: `internal/storage/shard/shard.go`
- Modify: `internal/storage/shard/shard_io.go`

- [ ] **Step 1: 删除 Shard 结构体中的 tsSidMap 字段**

`shard.go` L132 删除：
```go
tsSidMap        map[int64][]uint64          // timestamp → sid 列表映射（支持同一时间戳多个点）
```

- [ ] **Step 2: 删除 NewShard 中 tsSidMap 初始化**

`shard.go` L198 删除：
```go
tsSidMap:    make(map[int64][]uint64),
```

- [ ] **Step 3: 适配 shard_io.go Write 方法**

`shard_io.go` L56-62：
```go
// 旧
sid, err := s.seriesStore.AllocateSID(point.Tags)
if err != nil {
    return fmt.Errorf("allocate SID: %w", err)
}
s.sidCache[sid] = copyTagsMap(point.Tags)
s.tsSidMap[point.Timestamp] = append(s.tsSidMap[point.Timestamp], sid)

// ...字段验证...
if err := s.memTable.Write(point); err != nil {

// 新
sid, err := s.seriesStore.AllocateSID(point.Tags)
if err != nil {
    return fmt.Errorf("allocate SID: %w", err)
}
s.sidCache[sid] = copyTagsMap(point.Tags)

// ...字段验证...
if err := s.memTable.Write(point, sid); err != nil {
```

- [ ] **Step 4: 编译验证**

```bash
cd /root/projects/mts && go build ./internal/storage/shard/...
```
Expected: 编译失败（flush_lifecycle 尚未适配）

---

### Task 6: Shard — Flush / Close / ReplayWAL 适配

**Files:**
- Modify: `internal/storage/shard/shard_flush.go`
- Modify: `internal/storage/shard/shard_lifecycle.go`

- [ ] **Step 1: 适配 shard_flush.go — flushLocked**

`shard_flush.go` L25-87：
```go
// 旧
points := s.memTable.Flush()
...
w.WritePoints(points, s.tsSidMap)
...
for _, p := range points {
    delete(s.tsSidMap, p.Timestamp)
}

// 新
points, sids := s.memTable.Flush()
...
w.WritePoints(points, sids)
...
// 不再需要 delete tsSidMap
```

删除 L85-87 的 `for _, p := range points { delete(s.tsSidMap, p.Timestamp) }`

- [ ] **Step 2: 适配 shard_lifecycle.go — Close**

`shard_lifecycle.go` L66-84 平坦 compaction 路径：
```go
// 旧
points := s.memTable.Flush()
...
w.WritePoints(points, s.tsSidMap)
...
for _, p := range points {
    delete(s.tsSidMap, p.Timestamp)
}

// 新
points, sids := s.memTable.Flush()
...
w.WritePoints(points, sids)
...
// 不再需要 delete tsSidMap
```

删除 L107-109 和 L117-119 的 tsSidMap 清理代码。

- [ ] **Step 3: 适配 shard_lifecycle.go — ReplayWAL**

`shard.go`（或 `shard_lifecycle.go`）L565：
```go
// 旧
s.tsSidMap[point.Timestamp] = append(s.tsSidMap[point.Timestamp], sid)
if err := s.memTable.Write(point); err != nil {

// 新
if err := s.memTable.Write(point, sid); err != nil {
```

- [ ] **Step 4: 编译验证**

```bash
cd /root/projects/mts && go build ./internal/storage/shard/...
```
Expected: 编译成功

- [ ] **Step 5: 运行 shard 包全部测试**

```bash
cd /root/projects/mts && go test ./internal/storage/shard/... -v -count=1 -short -timeout 120s 2>&1 | head -200
```
若因调用方未适配导致部分测试编译失败，跳过此步。

- [ ] **Step 6: 提交**

```bash
git add internal/storage/shard/shard.go internal/storage/shard/shard_io.go internal/storage/shard/shard_flush.go internal/storage/shard/shard_lifecycle.go
git commit -m "refactor(shard): 删除 tsSidMap，SID 由 MemTable entry 携带"
```

---

### Task 7: Compaction merge.go — tsSidMap → []uint64

**Files:**
- Modify: `internal/storage/compaction/merge.go`

- [ ] **Step 1: 替换 tsSidMap 为 sids 切片**

L106-151 范围内：

删除 `var tsSidMap map[int64][]uint64`

`flushBatch` 闭包内：
```go
// 旧
tsSidMap = make(map[int64][]uint64)

// 新
sids = sids[:0]
```

L130-151 merge 主循环：
```go
// 旧
var tsSidMap map[int64][]uint64  // 删除

// flushBatch 中:
if err := w.WritePoints(pointsToWrite, tsSidMap); err != nil {
...
tsSidMap = make(map[int64][]uint64)

// 主循环中:
if tsSidMap == nil {
    tsSidMap = make(map[int64][]uint64)
}
tsSidMap[row.Timestamp] = append(tsSidMap[row.Timestamp], row.Sid)

// 新
var sids []uint64

// flushBatch 中:
if err := w.WritePoints(pointsToWrite, sids); err != nil {
...
sids = sids[:0]

// 主循环中:
sids = append(sids, row.Sid)
```

完整变更后的代码：

```go
seen := make(map[string]bool)
var pointsToWrite []*types.Point
var sids []uint64
const batchSize = 1000

flushBatch := func() error {
    if len(pointsToWrite) == 0 {
        return nil
    }
    if err := w.WritePoints(pointsToWrite, sids); err != nil {
        return err
    }
    task.OutputCount += len(pointsToWrite)
    pointsToWrite = pointsToWrite[:0]
    sids = sids[:0]
    return nil
}

for merged.Next() {
    select {
    case <-ctx.Done():
        _ = w.Close()
        return ctx.Err()
    default:
    }

    row := merged.Point()
    key := fmt.Sprintf("%d-%d", row.Timestamp, row.Sid)

    if seen[key] {
        task.DuplicateCount++
        continue
    }
    if tombstones.ShouldDelete(row.Sid, row.Timestamp) {
        continue
    }
    seen[key] = true

    point := &types.Point{
        Timestamp: row.Timestamp,
        Tags:      row.Tags,
        Fields:    row.Fields,
    }
    pointsToWrite = append(pointsToWrite, point)
    sids = append(sids, row.Sid)

    if len(pointsToWrite) >= batchSize {
        if err := flushBatch(); err != nil {
            _ = w.Close()
            return err
        }
        cm.ReportProgress(task.OutputCount)
    }
}
```

- [ ] **Step 2: 编译验证**

```bash
cd /root/projects/mts && go build ./internal/storage/compaction/...
```
Expected: 编译成功

---

### Task 8: Compaction level.go — tsSidMap → []uint64

**Files:**
- Modify: `internal/storage/compaction/level.go`

- [ ] **Step 1: 替换 tsSidMap 为 sids 切片**

与 merge.go 完全相同的变更模式。L318-361：

删除 `var tsSidMap map[int64][]uint64`，用 `var sids []uint64` 替代。

`flushBatch` 中 `tsSidMap = make(...)` → `sids = sids[:0]`

主循环中 `tsSidMap[row.Timestamp] = append(...)` → `sids = append(sids, row.Sid)`

`WritePoints(pointsToWrite, tsSidMap)` → `WritePoints(pointsToWrite, sids)`

- [ ] **Step 2: 编译验证**

```bash
cd /root/projects/mts && go build ./internal/storage/compaction/...
```
Expected: 编译成功

- [ ] **Step 3: 提交**

```bash
git add internal/storage/compaction/merge.go internal/storage/compaction/level.go
git commit -m "refactor(compaction): tsSidMap 替换为并行 sids 切片"
```

---

### Task 9: 测试适配

**Files:**
- Modify: `internal/storage/shard/level_compaction_e2e_test.go`
- Modify: `internal/storage/shard/iterator_test.go`

- [ ] **Step 1: 适配 level_compaction_e2e_test.go — createTestSSTableInLevel**

`level_compaction_e2e_test.go` L31-39：
```go
// 旧
tsSidMap := make(map[int64][]uint64)
for _, p := range points {
    sid := uint64(1)
    tsSidMap[p.Timestamp] = append(tsSidMap[p.Timestamp], sid)
}
if err := w.WritePoints(points, tsSidMap); err != nil {

// 新
sids := make([]uint64, len(points))
for i := range points {
    sids[i] = uint64(1)
}
if err := w.WritePoints(points, sids); err != nil {
```

- [ ] **Step 2: 适配 iterator_test.go — memTable.Write 调用**

将所有 `shard.memTable.Write(p)` 改为 `shard.memTable.Write(p, 0)`。

共 8 处：L37, L250, L364, L424, L589, L697, L739, L815。

搜索替换：`\.memTable\.Write(p)` → `.memTable.Write(p, 0)`

- [ ] **Step 3: 编译并运行受影响的测试**

```bash
cd /root/projects/mts && go test ./internal/storage/shard/... -v -count=1 -short -timeout 120s 2>&1 | tail -50
```
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add internal/storage/shard/level_compaction_e2e_test.go internal/storage/shard/iterator_test.go
git commit -m "test: 适配 Write/WritePoints 签名变更"
```

---

### Task 10: 全量编译 + lint + 格式化

- [ ] **Step 1: 全量编译**

```bash
cd /root/projects/mts && go build ./...
```
Expected: 编译成功

- [ ] **Step 2: goimports-reviser 格式化**

```bash
cd /root/projects/mts && find . -name '*.go' -not -path './.git/*' -not -path '*/vendor/*' -exec goimports-reviser -rm-unused -format {} \;
```

- [ ] **Step 3: golangci-lint**

```bash
cd /root/projects/mts && golangci-lint run ./internal/storage/memtable/... ./internal/storage/shard/... ./internal/storage/compaction/... 2>&1 | tail -30
```
Expected: 无新增问题

- [ ] **Step 4: 全量测试（短模式）**

```bash
cd /root/projects/mts && go test ./internal/... -count=1 -short -timeout 180s 2>&1 | tail -30
```
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add -u && git commit -m "chore: goimports-reviser 格式化"
```

---

### Task 11: E2E 测试

- [ ] **Step 1: Compaction E2E 测试**

```bash
cd /root/projects/mts/tests/e2e/compaction_test && go build -o compaction_test && ./compaction_test
```
Expected: 所有 8 个测试通过

- [ ] **Step 2: 清理构建产物**

```bash
rm -f /root/projects/mts/tests/e2e/compaction_test/compaction_test
```

- [ ] **Step 3: 验证无残留引用**

```bash
cd /root/projects/mts && grep -r "tsSidMap\|tsSidMap\|ts_sid_map" --include='*.go' --exclude-dir='.git' --exclude-dir='vendor' --exclude-dir='docs'
```
Expected: 无输出（代码中无残留引用）

- [ ] **Step 4: 最终提交**

```bash
git add -u && git commit -m "chore: E2E 测试通过，确认 tsSidMap 完全移除"
```
