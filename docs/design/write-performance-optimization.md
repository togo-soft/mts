# 写入性能优化方案

## 问题背景

`write_1m_pprof` 测试写入 1M 数据点耗时 ~3.5s（~280K TPS），CPU profile 显示异步 flush 占用 55.74% CPU，
WAL 写入 12%，schema 校验 4%。以下按收益从高到低排列优化项。

## CPU Profile 基线（200K 点）

| 热点 | 占比(cum) | 说明 |
|------|-----------|------|
| `executeAsyncFlush` → `writeGroupSSTable` | 55.74% | 异步 flush（SSTable 编码+压缩+Close） |
| `sstable.(*Writer).Close` | 30.33% | SSTable 关闭（flush block + 写 index + 落盘） |
| `sstable.(*Writer).WriteMemPoints` | 22.95% | 字段编码（XOR 压缩 18%） |
| `wal.(*WAL).Write` | 12.30% | WAL 写入（syscall） |
| `wal.CompressPayload` | 11.48% | WAL LZ4 压缩 |
| `validateFieldTypes` | 4.10% | schema 校验 + schemaMu 锁 |

---

## 优化一：提高 MemTable MaxCount 减少 Flush 频率

**收益预估**：~30-50%（flush CPU 从 55% 降至 ~3%）

**现状**：`MaxCount=3000` → 1M 点触发 ~333 次 flush → 333 次 SSTable Close

**方案**：将默认 `MaxCount` 从 3000 提升到 50000，flush 次数从 333 降至 20。

**内存影响**：50K 点 × ~200B/点 ≈ 10MB active + 10MB passive = 20MB MemTable，远低于 64MB MaxSize 限制。配合 `IdleDurationNanos` 空闲刷盘，内存安全。

**涉及文件**：
- `internal/storage/memtable/memtable.go` — `DefaultMemTableConfig()` 修改默认值
- `tests/e2e/write_1m_pprof/main.go` — 构造参数改为使用默认配置

**风险**：崩溃恢复时 WAL replay 最多 50K 点，恢复时间略微增加（毫秒级差异）。

---

## 优化二：WAL 写入前置化 — 减少写锁持有时间

**收益预估**：~8-12%

**现状**：`MeasurementWriter.Write` 持有 `mw.mu` 期间进行 WAL 序列化（LZ4 压缩 + 记录编码 + buffer copy）。WAL 有自己的 `mu`，写锁嵌套造成不必要的时间开销。

**方案**：将 WAL 序列化移到 `mw.mu.Lock()` 之前，只在 WAL buffer 写入阶段持锁。

```go
// 优化前（writer.go Write 方法）
mw.mu.Lock()
sid, _ := mw.seriesStore.AllocateSID(point.Tags)
mw.validateFieldTypes(point)
mp := types.PointToMemPoint(point, sid)
// WAL 序列化在 mw.mu 内执行
data, release := serializePointForWALPooled(mp.Timestamp, mp.Sid, mp.FieldData)
mw.wal.Write(data)
release()
mw.memTable.Write(mp)
mw.mu.Unlock()

// 优化后
mw.mu.Lock()
sid, _ := mw.seriesStore.AllocateSID(point.Tags)
mw.validateFieldTypes(point)
mp := types.PointToMemPoint(point, sid)
mw.mu.Unlock()
// ↓ WAL 序列化移到锁外（LZ4 压缩 + 编码，纯 CPU 操作）
var walData []byte
var walRelease func()
if mw.wal != nil {
    walData, walRelease = serializePointForWALPooled(mp.Timestamp, mp.Sid, mp.FieldData)
}
mw.mu.Lock()
if mw.wal != nil {
    mw.wal.Write(walData)
    walRelease()
}
mw.memTable.Write(mp)
shouldFlush := mw.memTable.ShouldSwap()
mw.mu.Unlock()
```

**注意**：MemTable.Write 和 WAL.Write 仍需要在同一个临界区内，保证 WAL 和 MemTable 的一致性。

**涉及文件**：
- `internal/storage/writer/writer.go` — `Write()` 和 `WriteBatch()` 方法

---

## 优化三：Schema 校验快速路径

**收益预估**：~3-5%

**现状**：每次 `validateFieldTypes` 都获取 `schemaMu.Lock()`（写锁），遍历所有字段名来查重。即使在 schema 稳定后（所有字段类型已知），仍然执行完整校验流程。

**方案**：
1. 添加 `schemaStable atomic.Bool` 标志位，首次遇到新字段后重置
2. schema 稳定时用 `schemaMu.RLock()` 替代 `Lock()`
3. 缓存字段名→索引映射，避免线性遍历

```go
func (mw *MeasurementWriter) validateFieldTypes(point *types.Point) error {
    if mw.schemaStable.Load() {
        // 快速路径：只读校验
        mw.schemaMu.RLock()
        defer mw.schemaMu.RUnlock()
        return mw.validateStableLocked(point)
    }
    // 慢路径：可能新增字段
    mw.schemaMu.Lock()
    defer mw.schemaMu.Unlock()
    return mw.validateWithUpdateLocked(point)
}
```

**涉及文件**：
- `internal/storage/writer/writer.go` — `validateFieldTypes` 方法 + 新增 `schemaStable` 字段

---

## 优化四：MemTable 写入排序优化

**收益预估**：~2-5%

**现状**：`MemTable.Write` 每次写入都检查是否需要排序：
```go
if !m.sorted || (m.activeCount > 1 && m.active[m.activeCount-1].Timestamp < m.active[m.activeCount-2].Timestamp) {
    m.sortActive()  // sort.Slice O(n log n)
}
```
对于时间戳单调递增的写入（最常见场景），每次 append 后都触发这个检查。`sortActive` 在已排序数据上仍会执行完整的 `sort.Slice`。

**方案**：对单调递增场景做快速路径：
- 新时间戳 >= 末尾时间戳 → 直接 append，标记 sorted，跳过 sort 检查
- 新时间戳 < 末尾时间戳 → 回退到 sort

```go
func (m *MemTable) Write(mp types.MemPoint) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    m.active = append(m.active, mp)
    m.activeCount++
    m.lastWrite = time.Now()

    // 快速路径：时间戳单调递增
    if m.activeCount <= 1 || mp.Timestamp >= m.active[m.activeCount-2].Timestamp {
        m.sorted = true
        return nil
    }
    // 乱序插入需要重排
    m.sortActive()
    m.sorted = true
    return nil
}
```

**涉及文件**：
- `internal/storage/memtable/memtable.go` — `Write()` 方法

---

## 优化五：减少 SSTable Close 中的 fsync 次数

**收益预估**：~5-10%

**现状**：每次 flush 产出一个 SSTable 文件，`Close()` 中执行 `f.Sync()`。333 次 flush = 333 次 fsync。

**方案**：SSTable Writer 增加 `SyncOnClose bool` 选项，flush 产出的临时 SSTable 不 fsync（Phase 3 的 rename 是原子的，即使 crash 也只会丢失未完成的 tmp 文件）。最终 Close 时统一 fsync。

```go
// writer_flush.go writeGroupSSTable
w, err := sstable.NewWriter(shard.Dir, sstSeq, 0, mw.compressionAlgorithm)
// 设置 Close 时不 fsync（Phase 3 rename 保证原子性）
w.SetSyncOnClose(false)
```

**风险**：低。tmp 文件在 Phase 3 rename 前不可见，crash 后残留 tmp 文件由下次启动清理。

**涉及文件**：
- `internal/storage/shard/sstable/writer.go` — 新增 `SetSyncOnClose` 选项
- `internal/storage/writer/writer_flush.go` — `writeGroupSSTable` 设置该选项

---

## 实施顺序与预期累计收益

| 阶段 | 优化项 | 预期提升 | 累计 TPS |
|------|--------|----------|----------|
| 基线 | — | — | ~280K |
| 1 | 提高 MaxCount 3000→50000 | +30-50% | ~420K |
| 2 | WAL 写入前置化 | +8-12% | ~460K |
| 3 | Schema 快速路径 | +3-5% | ~480K |
| 4 | MemTable 排序优化 | +2-5% | ~500K |
| 5 | SSTable 减少 fsync | +5-10% | ~530K |

**累计预期**：~280K → ~530K TPS（提升 ~90%）

## 风险评估

- **优化一** 增加 MemTable 内存（~20MB），但远低于 64MB 限制，且可以通过 `IdleDurationNanos` 控制空闲刷盘
- **优化二** 将锁分段，不改变写入顺序和持久性语义
- **优化三** 的 schemaStable 标志需要正确处理字段新增场景（如第一次遇到新 tag key）
- **优化四** 仅在时间戳单调递增时生效，乱序写入自动回退到 sort
- **优化五** 依赖 Phase 3 的原子 rename 保证一致性，不改变持久性保证
