# MemTable Active/Passive 双缓冲设计

## 1. 概述

**问题**：当前 `flushLocked()` 在 `Shard.Write()` 中同步执行，持有 `s.mu.Lock()` 的情况下完成整个 SSTable 编码+压缩+磁盘 I/O（100ms-1s+）。期间该 Shard 的所有读写完全阻塞，且 flush 过程中 memtable entries + SSTable 编码缓冲同时在内存中，造成 2-3x 内存尖峰。

**目标**：
1. 写入路径不再阻塞于 SSTable I/O（异步 flush）
2. flush 期间内存不叠加（SSTable 写入在前台，新数据在 active memtable 并行累积）
3. 写入延迟稳定（无 100ms-1s+ 的 flush 抖动）

**核心思路**：引入 active/passive 双缓冲 + 异步 flush，参考 InfluxDB Cache.Snapshot 和 VictoriaMetrics active/passive memtable 模式。

---

## 2. 架构对比

### 2.1 当前架构

```
Write() → s.mu.Lock()
       → WAL.Write()
       → memTable.Write()        // 追加到单一 entries 切片
       → if ShouldFlush():
           flushLocked():        // ← 持锁执行，阻塞所有读写！
             memTable.Flush()    //    窃取切片
             SSTable.WritePoints()
             w.Close()           //    编码+压缩+fsync (100ms-1s+)
             WAL.Truncate()
       → s.mu.Unlock()
```

**问题**：
- SSTable I/O 期间 Shard 完全阻塞
- flush 中 entries + SSTable 缓冲内存叠加（2-3x 峰值）
- 写入延迟不可预测（取决于 SSTable 写入时长）

### 2.2 新架构

```
MemTable:
  ┌──────────┐   ┌───────────┐
  │  active   │   │  passive   │
  │ (接收写入) │   │ (等待flush) │
  └──────────┘   └───────────┘

Write() → s.mu.Lock()
       → WAL.Write()
       → memTable.active.append()  // 只写 active
       → if ShouldSwap():
           s.wal.Rotate()          // WAL 切新 segment（创建清理边界）
           memTable.Swap()         // active↔passive 交换
           go flushAsync(passive)  // 后台 flush，不持锁
       → s.mu.Unlock()

Read():
       合并 active + passive + SSTable → 排序 → 返回
```

**关键改进**：
- `Swap()` 和 `Rotate()` 在锁内完成，O(1) 时间（微秒级）
- SSTable 写入在后台 goroutine，完全无锁
- 新写入进入新的 active，与被 flush 的 passive 完全隔离

---

## 3. MemTable 结构变更

```go
type MemTable struct {
    mu        sync.RWMutex
    active    []types.InternalPoint  // 接收新写入
    passive   []types.InternalPoint  // 等待后台 flush（nil = 无待 flush 数据）
    flushing  atomic.Bool           // 是否有后台 flush 进行中
    maxSize   int64
    maxCount  int
    idleTimeout time.Duration
    lastWrite time.Time
    activeCount int                 // 仅统计 active
    sorted    bool
}
```

### 3.1 核心方法

```go
// Write 追加到 active。
func (m *MemTable) Write(ip types.InternalPoint) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    fields := make([]types.InternalField, len(ip.Fields))
    copy(fields, ip.Fields)
    m.active = append(m.active, types.InternalPoint{...})
    m.activeCount++
    m.lastWrite = time.Now()

    // 乱序检测与排序（仅对 active）
    if m.activeCount > 1 && m.active[m.activeCount-1].Timestamp < m.active[m.activeCount-2].Timestamp {
        sort.Slice(m.active, ...)
        m.sorted = true
    } else {
        m.sorted = true
    }
    return nil
}

// Swap 交换 active 和 passive。调用者需确保当前无 flush 进行中。
// 返回旧的 active（现为 passive），用于后台 flush。
func (m *MemTable) Swap() []types.InternalPoint {
    m.mu.Lock()
    defer m.mu.Unlock()

    if len(m.active) == 0 {
        return nil
    }

    // 复用 passive 的底层数组作为新的 active
    m.passive = m.active
    m.active = m.passive[:0]  // 共享底层数组但截断为 0
    m.activeCount = 0
    m.sorted = false
    m.flushing.Store(true)

    return m.passive
}

// ClearPassive 在后台 flush 成功后清空 passive，释放内存。
func (m *MemTable) ClearPassive() {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.passive = nil
    m.flushing.Store(false)
}

// ShouldSwap 检查 active 是否需要交换到 passive。
func (m *MemTable) ShouldSwap() bool {
    m.mu.RLock()
    defer m.mu.RUnlock()

    if m.flushing.Load() {
        return false // 已有 flush 进行中
    }
    estimatedSize := int64(len(m.active)) * 1024
    if estimatedSize >= m.maxSize {
        return true
    }
    if m.maxCount > 0 && m.activeCount >= m.maxCount {
        return true
    }
    if m.idleTimeout > 0 && m.activeCount > 0 && time.Since(m.lastWrite) >= m.idleTimeout {
        return true
    }
    return false
}
```

### 3.2 迭代器变更

当前 `Iterator()` 直接返回 `m.entries` 引用。新设计需要合并 active + passive：

```go
// Iterator 返回合并 active 和 passive 的迭代器。
func (m *MemTable) Iterator() *MemTableIterator {
    m.mu.RLock()
    active := m.active     // 浅拷贝引用（安全：迭代器生命周期内不修改）
    passive := m.passive   // 浅拷贝引用
    m.mu.RUnlock()

    return &MemTableIterator{
        active:  active,
        passive: passive,
        posA:    -1,
        posP:    -1,
    }
}

type MemTableIterator struct {
    active  []types.InternalPoint
    passive []types.InternalPoint
    posA    int
    posP    int
    nextA   *types.InternalPoint
    nextP   *types.InternalPoint
}

// Next 二路归并 active 和 passive，按 timestamp 升序返回。
func (it *MemTableIterator) Next() bool {
    // 惰性推进：首次调用时 peek 两边
    if it.posA == -1 && it.posP == -1 {
        it.advanceA()
        it.advanceP()
    }

    // 选择 timestamp 较小的一边推进
    if it.nextA != nil && it.nextP != nil {
        if it.nextA.Timestamp <= it.nextP.Timestamp {
            it.posA = ... // 记录当前位置
            it.advanceA()
            return true
        }
        it.posP = ...
        it.advanceP()
        return true
    }
    if it.nextA != nil {
        it.posA = ...
        it.advanceA()
        return true
    }
    if it.nextP != nil {
        it.posP = ...
        it.advanceP()
        return true
    }
    return false
}

func (it *MemTableIterator) Point() types.InternalPoint {
    // 返回 last advanced 的 point
}
```

---

## 4. Shard.Write() 变更

```go
func (s *Shard) Write(point *types.Point) error {
    // 1. 验证和序列化（不需要锁的部分）
    sid, err := s.seriesStore.AllocateSID(point.Tags)
    if err != nil { return err }
    if err := s.ValidateFieldTypes(point); err != nil { return err }
    ip := types.PointToInternal(point, sid)

    // 2. 持锁写入 WAL + active memtable
    s.mu.Lock()

    // 背压：如果 active 达到硬限制且 flush 进行中，等待 flush 完成
    // 硬限制 = maxSize * 2，防止 OOM
    for s.memTable.ActiveFull() && s.memTable.IsFlushing() {
        flushDone := s.flushDoneCh  // snapshot under lock
        s.mu.Unlock()
        select {
        case <-flushDone:
        case <-time.After(100 * time.Millisecond):
        }
        s.mu.Lock()
    }

    if s.wal != nil {
        data, _ := serializeInternalPoint(ip)
        s.wal.Write(data)
    }
    s.memTable.Write(ip)

    needSwap := s.memTable.ShouldSwap()
    s.mu.Unlock()

    // 3. 锁外触发异步 flush
    if needSwap {
        s.tryTriggerAsyncFlush()
    }

    return nil
}
```

---

## 5. 异步 Flush 流程

```go
// tryTriggerAsyncFlush 尝试触发异步 flush。CAS 保证只有一个 goroutine 执行。
func (s *Shard) tryTriggerAsyncFlush() {
    if !s.memTable.TrySetFlushing() {
        return // 已有 flush 进行中
    }

    s.compactionWg.Add(1)
    go func() {
        defer s.compactionWg.Done()
        s.executeAsyncFlush()
    }()
}

// executeAsyncFlush 后台执行：swap → SSTable 写入 → 清理。
func (s *Shard) executeAsyncFlush() {
    // Phase 1: 持锁交换 + WAL 切分（微秒级）
    s.mu.Lock()

    // WAL 先 rotate：创建新 segment，旧 segment 仅包含待 flush 的数据
    if s.wal != nil {
        if err := s.wal.Rotate(); err != nil {
            slog.Warn("WAL rotate before flush failed", "error", err)
            s.mu.Unlock()
            s.memTable.ClearFlushing()
            return
        }
    }

    passive := s.memTable.Swap() // active → passive，active 变空
    s.mu.Unlock()

    if len(passive) == 0 {
        s.memTable.ClearPassive()
        return
    }

    // Phase 2: 写 SSTable（不持任何锁，可耗时 100ms-1s+）
    sstPath, sstSeq := s.prepareSSTPath()
    w, err := sstable.NewWriter(s.dir, sstSeq, 0, s.compressionAlgo)
    if err != nil {
        slog.Error("async flush: create writer failed", "error", err)
        // passive 数据仍在内存中，下次 flush 重试
        s.mergePassiveBack() // 合并回 active
        return
    }

    if err := w.WritePoints(passive); err != nil {
        w.Close()
        slog.Error("async flush: write points failed", "error", err)
        s.mergePassiveBack()
        return
    }
    // Schema 持久化
    if s.schemaStore != nil {
        metaSchema := SSTableSchemaToMetadataSchema(w.Schema())
        s.schemaStore.SetSchema(s.db, s.measurement, metaSchema)
        s.UpdateSchemaInMemory(metaSchema)
    }
    if err := w.Close(); err != nil {
        slog.Error("async flush: close writer failed", "error", err)
        s.mergePassiveBack()
        return
    }

    // Phase 3: 持锁清理（微秒级）
    s.mu.Lock()

    // 注册 SSTable
    if s.levelCompaction != nil {
        s.levelCompaction.AddPart(0, ...)
    } else {
        s.sstSeq++
    }

    // 清理 passive，释放内存
    s.memTable.ClearPassive()

    // WAL 清理：删除 rotate 前的旧 segment（全部已 flush）
    if s.wal != nil {
        s.wal.TruncateCurrent()
    }

    s.mu.Unlock()

    // 通知等待背压的写入者
    s.notifyFlushComplete()

    s.triggerBackgroundCompaction()
}
```

### 5.1 失败恢复：`mergePassiveBack()`

```go
// mergePassiveBack 将 flush 失败的 passive 数据合并回 active。
// 新写入的数据排在 passive 数据之后，需重新排序。
func (s *Shard) mergePassiveBack() {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.memTable.MergePassiveBack() // passive 追加到 active 前面，重排序
}
```

---

## 6. WAL 交互

### 6.1 关键时序

```
时间线：
  WAL seg N: [R1, R2, ..., R100]     ← 所有写入在 seg N
  Swap 触发:
    s.wal.Rotate()                    ← 创建空 seg N+1
    memTable.Swap()                   ← active(R1..R100)→passive
  Swap 后新写入:
    WAL seg N+1: [R101, R102, ...]   ← 新 active 的 WAL 记录
  Flush 完成:
    passive(R1..R100) → SSTable      ← 已持久化
    s.wal.TruncateCurrent()          ← 删除 seg N（仅含已 flush 数据）
                                      保留 seg N+1（含未 flush 的 active 数据）
```

**安全性**：`Rotate()` 在 `Swap()` 之前调用，确保 WAL segment N 中不包含 swap 后的新写入。`TruncateCurrent()` 仅删除 segNum < current 的旧 segment，保留当前 segment（只含 active 数据）。

### 6.2 为什么不用 `TruncateAfterFlush()`

`TruncateAfterFlush()` 删除全部 segment → 新 segment。但异步 flush 场景下，swap 后新写入已进入当前 segment，全删会丢失 active 数据。改用 `Rotate()` + `TruncateCurrent()` 组合，精确删除仅含已 flush 数据的旧 segment。

---

## 7. Read 路径

Read 路径需合并三个数据源：active + passive + SSTable。

当前 `Shard.Read()` 已合并 memTable + SSTable。只需 memtable 迭代器支持 active+passive 归并：

```
Read():
  memIter = memTable.Iterator()     // 自动归并 active + passive
  sstRows = readFromSSTable(...)    // 不变
  rows = merge(memIter, sstRows)    // 不变（现有逻辑）
  sort(rows)
```

`ShardIterator`（流式）同理，memtable 迭代器内部已合并 active+passive，上层无感知。

---

## 8. 背压机制

### 8.1 三级阈值

| 阈值 | 触发行为 |
|------|---------|
| `maxSize` (64MB) | `ShouldSwap() = true`，触发异步 flush |
| `maxSize * 2` (128MB) | `ActiveFull() = true`，写入阻塞等待 flush 完成 |
| `maxSize * 4` (256MB) | 硬拒绝，返回错误（防止 OOM） |

### 8.2 背压流程

```
Write() → s.mu.Lock()
       → if ActiveFull() && IsFlushing():
           // 等待当前 flush 完成
           s.mu.Unlock()
           <-flushDoneCh
           s.mu.Lock()
       → if ActiveExceeded():
           return ErrMemTableFull
       → 正常写入
```

---

## 9. 定期 Flush 检查

当前 `doPeriodicFlush()` 检查 `ShouldFlush()` 然后持锁调用 `flushLocked()`。改为：

```go
func (s *Shard) doPeriodicFlush() {
    if !s.memTable.ShouldSwap() {
        return
    }
    s.tryTriggerAsyncFlush()
}
```

不需要持 `s.mu`（tryTriggerAsyncFlush 内部使用 CAS + 短暂持锁做 swap）。

---

## 10. Close 路径

Close 时需确保 passive 数据已持久化：

```go
func (s *Shard) closeWithLock() {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 1. 等待正在进行的 flush 完成
    for s.memTable.IsFlushing() {
        s.mu.Unlock()
        time.Sleep(10 * time.Millisecond)
        s.mu.Lock()
    }

    // 2. 最后一次 swap + 同步 flush（Close 场景可同步，因为没有新写入竞争）
    passive := s.memTable.Swap()
    if len(passive) > 0 {
        // 同步写 SSTable（与当前 closeWithLock 逻辑相同）
        s.writeSSTableSync(passive)
    }

    // 3. WAL 清理（与当前相同）
    s.wal.Close()
    s.wal.Purge()
}
```

---

## 11. 内存模型

### 11.1 正常运行时

```
active:  0 → 64MB (持续增长)
passive: 64MB → 0 (flush 完成后释放)
SSTable 写入缓冲: 仅在后台 goroutine
```

**峰值**：active(64MB) + passive(64MB) + SSTable缓冲(~64MB) ≈ 192MB（单 Shard），比当前的 2-3x 叠加更可控。且 passive 的 SSTable 写入不阻塞新写入。

### 11.2 与当前对比

| 阶段 | 当前 | 新设计 |
|------|------|--------|
| 正常运行 | entries ~64MB | active ~64MB, passive=nil |
| flush 触发 | entries + SSTable缓冲 ≈ 128-192MB | active ~64MB, passive ~64MB, 后台 SSTable |
| flush 中 | **所有写入阻塞** | **写入继续到 active** |
| flush 峰值 | ~192MB + 阻塞 | ~192MB + 无阻塞 |
| flush 完成 | entries 释放 | passive 释放 (ClearPassive) |

---

## 12. 涉及的代码变更

| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `memtable/memtable.go` | 重写 | active/passive 双缓冲，Swap/ClearPassive，新迭代器 |
| `memtable/memtable_test.go` | 重写/新增 | 双缓冲测试，并发测试，背压测试 |
| `shard/shard_flush.go` | 重写 | executeAsyncFlush，tryTriggerAsyncFlush |
| `shard/shard_io.go` | 修改 | Write() 改为锁内 swap + 锁外 flush |
| `shard/shard.go` | 修改 | 新增 flushDoneCh，移除旧的 flush 相关字段 |
| `shard/shard_lifecycle.go` | 修改 | closeWithLock 适配新 flush 流程 |
| `shard/iterator.go` | 微调 | MemTable 迭代器已内部归并，上层基本不变 |

---

## 13. 不改的范围

- **SSTable 写入格式**：不变
- **Compaction 逻辑**：不变
- **WAL 写入/Replay 逻辑**：不变
- **Query 路径**：不变（memtable 迭代器内部归并，对外接口一致）
- **gRPC API**：不变

---

## 14. 测试计划

### 14.1 MemTable 单元测试

1. **双缓冲基本写入**：Write → Swap → 验证 active 变空、passive 有数据
2. **Swap 后继续写入**：Swap → Write → 验证新数据进入 active
3. **ClearPassive**：Swap → ClearPassive → 验证 passive=nil，内存释放
4. **迭代器归并**：active 有 [T1,T3]，passive 有 [T2,T4] → 验证 Next 返回 T1,T2,T3,T4
5. **空 passive**：passive=nil 时迭代器仅返回 active
6. **ShouldSwap 条件**：达到 maxSize / maxCount / idleTimeout 分别触发
7. **并发安全**：多 goroutine 同时 Write + Swap + Iterator
8. **MergePassiveBack**：flush 失败后数据合并回 active

### 14.2 Shard 集成测试

1. **异步 flush 不阻塞写入**：Write 触发 flush → 立即返回 → 新 Write 立即成功
2. **flush 期间数据可见**：Swap 后、SSTable 写入中，Read 可看到 passive 数据
3. **flush 完成后数据可见**：ClearPassive 后，Read 从 SSTable 看到数据
4. **背压触发**：写入速度 > flush 速度 → active 达硬限制 → 写入阻塞 → flush 完成后恢复
5. **崩溃恢复**：flush 中途崩溃 → 重启后 WAL replay 恢复 passive 数据

### 14.3 E2E 测试

现有全部 E2E 测试通过，特别关注：
- `persistence_test`：持久化正确性
- `restart_recovery`：重启恢复完整
- `integrity`：10 万数据点完整性

---

## 15. 实施建议

分两个阶段：

**Phase 1（核心）**：MemTable 双缓冲 + 异步 flush
- 改动集中在 `memtable/` 和 `shard/`
- 约 400-500 行代码变更

**Phase 2（增强）**：精确内存估算
- 将 `len(active)*1024` 替换为基于 `unsafe.Sizeof` + 字段实际大小的估算
- 可选，Phase 1 即可解决核心问题
