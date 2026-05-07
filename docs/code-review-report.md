# mts 时序数据库 — 代码检视报告

> 检视日期：2026-05-07 | 分支：main | Go 版本：1.26

## 项目概览

| 维度 | 数据 |
|------|------|
| 模块名 | `codeberg.org/micro-ts/mts` |
| 核心代码量 | ~5,900 行（10 个核心文件） |
| 总 Go 代码量 | ~28,000 行 |
| 单元测试包 | 8 个，全部通过（0 failures） |
| golangci-lint | 0 issues |
| 构建产物残留 | 8 个二进制文件，约 125MB |

---

## 一、严重问题（Critical）

### 1.1 `mergeIterator.Next()` 数据丢失 Bug

**文件**: `internal/storage/shard/compaction.go:486-503`

**描述**: `mergeIterator` 的 `Next()` 方法设计存在逻辑缺陷，导致 compaction 过程中会丢失每个 SSTable 迭代器的**第一条数据**。此 Bug 同时影响 `CompactionManager.merge()` 和 `LevelCompactionManager.merge()`（两处使用完全相同的 merge 逻辑），两个 compaction 路径均受影响。

**根因**:

`newMergeIterator` 初始化时，每个迭代器已通过 `iter.Next()` 推进到第一条记录，其 `Point()` 作为 `mergeHeapItem.point` 存入堆中：

```go
for i, iter := range iters {
    if iter.Next() {          // ← 推进到第 1 条
        p := iter.Point()     // ← 取第 1 条
        h = append(h, &mergeHeapItem{point: p, ...})
    }
}
```

但 `Next()` 方法 pop 出最小值后，继续推进迭代器并写回新值，而 **pop 出的原值被丢弃**：

```go
func (m *mergeIterator) Next() bool {
    item := heap.Pop(m.heap).(*mergeHeapItem)  // ← 弹出第 1 条
    if item.iter.Next() {                        // ← 推进到第 2 条
        item.point = item.iter.Point()           // ← 覆盖为第 2 条
        heap.Push(m.heap, item)                  // ← 第 2 条回堆
    }
    return len(*m.heap) > 0
}

func (m *mergeIterator) Point() *types.PointRow {
    return (*m.heap)[0].point  // ← 返回堆顶，是"新的第 2 小"，而不是刚 pop 的第 1 小
}
```

**后果**: Compaction 合并 SSTable 时静默丢失数据。合并后的新 SSTable 会替代旧文件，丢失是永久性的。

**修复建议**: 添加 `current` 字段保存本次 pop 的元素，`Point()` 改为返回 `m.current.point`：

```go
type mergeIterator struct {
    iterators []*sstable.Iterator
    heap      *mergeHeap
    current   *mergeHeapItem  // ← 新增
    err       error
}

func (m *mergeIterator) Next() bool {
    if len(*m.heap) == 0 || m.err != nil {
        m.current = nil
        return false
    }
    m.current = heap.Pop(m.heap).(*mergeHeapItem)
    if m.current.iter.Next() {
        m.current.point = m.current.iter.Point()
        m.current.timestamp = m.current.point.Timestamp
        heap.Push(m.heap, m.current)
    }
    return true
}

func (m *mergeIterator) Point() *types.PointRow {
    if m.current == nil {
        return nil
    }
    return m.current.point
}
```

---

### 1.2 `DB.CreateMeasurement` 始终返回 nil，吞噬所有错误

**文件**: `mts.go:451-455`

```go
func (db *DB) CreateMeasurement(ctx context.Context, database, measurement string) error {
    _ = ctx
    _, _ = db.engine.CreateMeasurement(database, measurement)  // ← 错误被丢弃
    return nil  // ← 永远返回 nil
}
```

`engine.CreateMeasurement` 在参数无效时会返回错误（如空数据库名 `ErrEmptyDatabase`、空 measurement 名 `ErrEmptyMeasurement`），但 `DB.CreateMeasurement` 用 `_, _` 丢弃了所有返回值，调用方无法感知任何错误。

**修复建议**: 移除 `_, _`，正常返回错误：

```go
func (db *DB) CreateMeasurement(ctx context.Context, database, measurement string) error {
    _ = ctx
    _, err := db.engine.CreateMeasurement(database, measurement)
    return err
}
```

---

## 二、高危问题（High）

### 2.1 SSTable Iterator 硬编码测试标签

**文件**: `internal/storage/shard/sstable/iterator.go:383, 397`

```go
// 两处硬编码：
Tags: map[string]string{"host": "server1"},
```

这是测试残留代码。SSTable 文件本身不存储 Tags（Tags 通过 MetaStore 中的 SID 解析），但在 compaction 流程中 iterator 返回的 `PointRow.Tags` 会作为 `Point.Tags` 传递。当前 Writer 不使用 Tags 字段（只用 `tsSidMap` 中的 Sid），但一旦未来 Writer 逻辑变更开始使用 Tags，将造成全量数据标签损坏。

**修复建议**: Tags 字段返回 nil 或空 map，或通过 `metaStore.GetTagsBySID` 正确解析。

---

### 2.2 `isPathSafe` 注释与实际行为不符

**文件**: `internal/storage/util.go:24-39`

```go
// 注释声称"路径不能是绝对路径"，但实际实现：
func isPathSafe(path string) bool {
    cleaned := filepath.Clean(path)
    if strings.Contains(cleaned, "..") {  // ← 只检查了路径遍历
        return false
    }
    // 缺少 filepath.IsAbs(path) 检查
    ...
}
```

`SafeCreate`、`SafeOpenFile`、`SafeMkdirAll` 均依赖此函数。绝对路径（如 `/var/lib/microts/data/sst_0`）能通过检查。注释声明的安全保证与实际行为不一致，可能导致使用者误判安全边界。

**修复建议**: 要么补充 `filepath.IsAbs` 检查，要么修正注释以反映实际行为。

---

### 2.3 `slog.Default()` 使用导致 WAL 日志丢失

**文件**: `internal/storage/shard/wal.go:136`

```go
func NewWAL(dir string, seq uint64) (*WAL, error) {
    return NewWALWithLogger(dir, seq, slog.Default())
}
```

WAL 是核心数据持久化组件，其日志对排查数据一致性问题至关重要。`slog.Default()` 使用全局默认 logger，不会跟随应用的主 logger 配置（例如 gRPC 服务端配置的 JSON 格式 logger）。生产环境集中式日志采集时，WAL 的日志将无法被正常采集。

**修复建议**: 将 logger 作为配置参数从 `ShardConfig` → `NewShard` → `NewWAL` 全链路传递。

---

### 2.4 `time.Sleep(10ms)` goroutine 同步反模式

**文件**: `internal/storage/shard/compaction.go:628`、`internal/storage/shard/level_compaction.go:878`

```go
func (cm *CompactionManager) Stop() {
    cm.stopOnce.Do(func() { close(cm.stopCh) })
    time.Sleep(10 * time.Millisecond) // ← 猜测一个时间等待 goroutine 退出
}
```

`time.Sleep` 作为同步机制不可靠：goroutine 可能因系统调度延迟在 10ms 后尚未退出，高负载下更明显。若 Stop() 之后紧跟着资源清理（如删除目录），会导致竞态条件。

**修复建议**: 使用 `sync.WaitGroup` 确认 goroutine 已退出：

```go
func (cm *CompactionManager) Stop() {
    cm.stopOnce.Do(func() { close(cm.stopCh) })
    cm.wg.Wait() // ← 确定性等待
}
```

---

### 2.5 `cmd/server/main.go` 缺少优雅关闭

**文件**: `cmd/server/main.go:36-44`

```go
s := grpc.NewServer()
types.RegisterMicroTSServer(s, api.New(eng))
if err := s.Serve(lis); err != nil { ... }
```

没有监听 `SIGTERM`/`SIGINT` 信号。收到 kill 信号时进程直接退出，MemTable 中未刷盘的数据和 MetaStore 中未持久化的元数据会丢失。

**修复建议**: 添加 signal handler：

```go
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
go func() {
    <-sigCh
    s.GracefulStop()
}()
```

---

## 三、中等问题（Medium）

### 3.1 `NewFieldValue` 返回 nil 导致静默数据丢失

**文件**: `types/convert.go:73-86`

```go
func NewFieldValue(v any) *FieldValue {
    switch val := v.(type) {
    case int64: ...
    case float64: ...
    default:
        return nil  // ← 不支持的类型静默返回 nil
    }
}
```

如果调用方传入 `int`、`float32` 等常见类型（JSON 反序列化常产生这类类型），函数返回 nil 不会产生任何提示，导致数据静默丢失。

**修复建议**: 至少支持 `int`→`int64`、`float32`→`float64` 的自动转换，不支持的场景记录 WARN 日志。

---

### 3.2 `copyTags` 函数重复定义

**文件**: `internal/storage/shard/shard.go:39-47` 和 `internal/storage/measurement/meas_meta.go:79-88`

完全相同的函数在两个包中分别定义。后续修改容易因只改一处而导致行为不一致。建议提取到公共位置。

---

### 3.3 `LevelManifest.GetLevel` 无锁保护

**文件**: `internal/storage/shard/level_compaction.go:118-120`

```go
func (m *LevelManifest) GetLevel(level int) *Level {
    return m.levels[level]  // ← 无锁访问 map
}
```

`LevelManifest` 内部有 `mu sync.RWMutex` 但 `GetLevel` 不使用它。当前所有调用路径都在 `LevelCompactionManager.manifestMu` 的保护下，但若未来有新的直接调用方将触发 data race。建议补锁或将方法降级为 unexported 并文档化前提条件。

---

### 3.4 `collectQueryResults` 冗余的双重停止条件

**文件**: `internal/engine/engine.go:404-437`

```go
if hasLimit && collected >= int(req.Limit) {
    break
}
if collected >= targetCount-int(req.Offset) {
    break
}
```

当 `hasLimit=true` 时，`targetCount-int(req.Offset)` = `Limit`，与第一个条件完全等价。当 `hasLimit=false` 时，`targetCount` = `MaxInt`，条件永远不成立。应简化为单一停止条件。

---

### 3.5 MemoryMetaStore 和 MeasurementMetaStore 持久化格式不一致

`MemoryMetaStore.Persist()` 使用**二进制格式**（magic + version + binary encoding），而 `MeasurementMetaStore.Persist()` 使用 **JSON 格式**。两种 MetaStore 服务于同类元数据却用了不同格式，增加维护成本。

---

### 3.6 `Engine.Close()` 持久化失败只报第一个错误

**文件**: `internal/engine/engine.go:200-210`

```go
if err := e.shardManager.PersistAllMetaStores(); err != nil {
    return fmt.Errorf("persist metastore: %w", err)
}
```

`PersistAllMetaStores` 实现为"遇第一个错误即返回"，后续 MetaStore 不会被尝试。应收集所有错误后统一报告。

---

### 3.7 SSTable Reader `readRangeOptimized` 读取全量字段数据

**文件**: `internal/storage/shard/sstable/reader.go:472-498`

通过 BlockIndex 精确定位了需要的 block，但随后读取**所有字段的全部数据**（`io.ReadAll`），而非只读匹配行对应的字段数据。对于宽表（大量字段）场景，内存浪费显著。

---

### 3.8 项目根目录散落 e2e 测试构建产物

根目录下存在 8 个编译后的二进制文件（每个约 15MB，共约 125MB）：`compaction_test`、`grpc_write_query`、`integrity`、`persistence_test`、`retention_test`、`simple_integrity`、`wal_test`、`write_1k`。应加入 `.gitignore` 并清理。

---

## 四、低危问题（Low）

| # | 文件 | 描述 |
|---|------|------|
| 4.1 | `internal/storage/shard/wal.go:820,849` | `err.Error() == "EOF"` 字符串比较，应使用 `errors.Is(err, io.EOF)` |
| 4.2 | `mts.go:56` | 文档示例 `"cpu_usage"` 与代码示例 `"cpu"` 不一致 |
| 4.3 | `internal/api/grpc.go:372-381` | `ToProtoPointRow` 始终将 Fields 设为 nil，且未被使用 |
| 4.4 | `internal/query/executor.go:63-75` | `Execute` 永远返回空结果集，是未完成的桩实现 |
| 4.5 | `types/` | 缺少单元测试覆盖，`convert.go` 和 `compaction_config.go` 未被测试 |

---

## 五、修复优先级建议

| 优先级 | 问题 | 理由 |
|--------|------|------|
| P0 | 1.1 mergeIterator 数据丢失 | Compaction 导致永久数据丢失 |
| P0 | 1.2 CreateMeasurement 吞噬错误 | API 行为不正确，调用方无法感知失败 |
| P1 | 2.1 SSTable Iterator 硬编码标签 | Compaction 中的潜在数据损坏 |
| P1 | 2.4/2.5 goroutine 同步 + 优雅关闭 | 进程退出时可能丢数据 |
| P2 | 2.3 WAL 日志丢失 | 影响生产排障能力 |
| P2 | 3.1 NewFieldValue 静默丢数据 | 静默数据丢失 |
| P3 | 其余问题 | 代码质量与可维护性改进 |
