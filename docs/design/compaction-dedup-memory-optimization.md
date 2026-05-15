# 方案六：Compaction Dedup 内存优化设计

> 日期：2025-05-15
> 目标：降低 compaction 去重路径的内存峰值，支撑百万级数据合并
> 前置：[InternalField 优化分析](../review/internalfield-optimization-analysis-2025-05-14.md)、[内存分配分析报告](../review/memory-allocation-analysis-2025-05-14.md)

---

## 一、问题诊断

### 1.1 当前 compaction 内存峰值构成

以合并 100 万行（10 个 SSTable 文件，每个 ~10 万行）为例：

| 对象 | 大小估算 | 占比 |
|------|---------|------|
| `map[uint64]bool` seen（去重集合） | ~36 MB（1M × 36B/entry） | **62%** |
| Tombstone index `map[uint64][]*Tombstone` | ~5 MB（视删除标记数量） | 9% |
| per-row `PointRow` + `FieldEntry` + `InternalField` | ~3.5 MB（瞬态，GC 及时回收） | 6% |
| `pointsToWrite` batch buffer（1000×InternalPoint） | ~0.5 MB | 1% |
| Block read/decompress buffers | ~2 MB（被 GC 回收但未归还 OS） | 3% |
| MergeHeap + iterators | ~1 MB | 2% |
| 其他（栈、runtime） | ~10 MB | 17% |
| **总计峰值** | **~58 MB** | 100% |

关键发现：**`seen` map 占据了超过 60% 的内存峰值，且存活时间覆盖整个合并过程。**

### 1.2 内存增长曲线

```
时间 →
内存 ↑
     │     ┌─────────────┐
 58MB│     │ seen map    │ ← 线性增长直到合并结束
     │    ╱│ (36 MB)     │
     │   ╱ └─────────────┘
 22MB│  ╱  ┌─────────────┐
     │ ╱   │ 其他固定开销  │
     │╱    │ (22 MB)      │
  0  └─────┴─────────────┴──→
     0     合并中期(50万)  完成
```

seen map 是唯一随数据量线性增长且永不释放的结构。

---

## 二、优化方案

### 方案 6A：Bloom Filter + 小窗口精确去重（核心优化，推荐立即执行）

#### 2.1 思路

用 Bloom Filter 替代 `map[uint64]bool` 做第一级去重判断，配合一个固定大小的滑动窗口做精确去重。消除 `map[uint64]bool` 的无限增长。

#### 2.2 数据结构

```go
type DedupFilter struct {
    bloom      *bloomFilter       // 快速排除"绝对未见过"的行
    window     map[uint64]bool    // 最近 N 条精确去重
    ringBuffer []uint64           // 环形缓冲区，FIFO 淘汰旧 key
    winIdx     int                // 当前环形位置
    windowSize int                // 窗口大小（如 10000）
}
```

#### 2.3 算法

```
去重判断: (key uint64) → (seen bool)

1. if bloom.MayContain(key) == false:
      bloom.Add(key)
      addToWindow(key)
      return false   // 绝对未见过
2. if window[key] == true:
      return true    // 窗口内精确命中 → 是重复
3. return false      // Bloom 假阳性 → 视为不重复（允许极少量重复）
```

#### 2.4 参数选择

| 参数 | 值 | 依据 |
|------|---|------|
| Bloom Filter 位数组 | 64 MB（512 Mbits） | 100 万条目下假阳性率 ≈ 0.1% |
| 哈希函数数 | 4 | 平衡假阳性率与计算开销 |
| 滑动窗口大小 | 10000 条 | 覆盖最近排序后的行（时间相邻的重复风险高） |
| Bloom 假阳性率 | 0.1% | 百万条目中约 1000 条可能被判定为非重复（不会丢数据，仅产生极少量重复保留） |

#### 2.5 内存对比

| 方案 | 100 万行内存 | 1000 万行内存 |
|------|-------------|--------------|
| 当前 `map[uint64]bool` | 36 MB | 360 MB |
| Bloom + 窗口 | 64 MB（固定） | 64 MB（固定） |

**100 万行时节省不明显（36→64，实际上是增加了）**。

等一下，让我重新计算。Bloom filter 64 MB 太大了。让我选择更合理的参数。

**修正后的参数**：

假设压缩合并的数据集为 100 万行，允许 1% 的假阳性（即最多 1万条重复可能被保留），并配合滑动窗口确保"相邻重复"（时间排序后的真正重复）被精确捕获：

| 参数 | 值 | 依据 |
|------|---|------|
| Bloom Filter 位数组 | 1.2 MB（10 Mbits）| 100 万条目，4 个哈希，假阳性率 ≈ 2.5% |
| 滑动窗口大小 | 50,000 条 | 覆盖最近时间窗口（实际去重主要发生在相邻行） |
| 哈希函数数 | 4 | 标准选择 |

这样 Bloom filter ≈ 1.2 MB，窗口 map ≈ 2.5 MB（50K × 50B/entry），总共 ≈ 3.7 MB。

**真实的假阳性场景分析**：在实际的 MTS 数据中，重复行（相同 Timestamp + SID）几乎总是**相邻出现**——写入者产生相同时间戳的事件在同一批次内、或在相邻文件中。即便 Bloom filter 假阳性率达到 2.5%，窗口（5万条）几乎总能捕获真正的重复。

**内存对比（修正）**：

| 方案 | 100 万行内存 | 1000 万行内存 |
|------|-------------|--------------|
| 当前 `map[uint64]bool` | ~36 MB | ~360 MB |
| Bloom(1.2MB) + 窗口(2.5MB) | ~3.7 MB（固定）| ~3.7 MB（固定）|
| **节省** | **90%** | **99%** |

#### 2.6 宽松模式 vs 严格模式

为满足不同场景需求，提供两种模式：

- **默认（宽松模式）**：Bloom Filter + 窗口。允许极少量重复（<1%），最大化内存效率。
- **严格模式**：当 compaction 输出来自少量文件且需要 100% 去重时，回退到当前的 `map[uint64]bool`。

自动选择逻辑：
```go
func chooseDedupMode(inputFiles, estimatedRows int) string {
    if estimatedRows < 50000 {
        return "strict"   // 小数据集直接用 map
    }
    return "relaxed"       // 大数据集用 Bloom
}
```

---

### 方案 6B：消除 InternalField 分配——直接写 FieldEntry→序列化

#### 2.7 问题

当前去重循环中：

```go
ip := types.InternalPoint{
    Fields: types.FieldEntryToInternalFields(row.Fields), // make([]InternalField, N)
}
pointsToWrite = append(pointsToWrite, ip)
```

每条非重复行都执行 `FieldEntryToInternalFields` → `make([]InternalField, len(fields))` → 分配新切片。

然后 `Writer.WritePoints(pointsToWrite)` 内部调用 `serializeInternalPoint(ip)` → 遍历 `Fields` 写序列化字节。**两次操作都读取同样的字段数据。**

#### 2.8 优化：Writer 直接接受 FieldEntry 序列化

新增 Writer 方法：

```go
// WriteFieldEntryRows 直接写入 []*FieldEntry 行（跳过 InternalField 中间层）
func (w *Writer) WriteFieldEntryRows(rows []FieldEntryRow) error
```

其中：
```go
type FieldEntryRow struct {
    Timestamp int64
    Fields    []*types.FieldEntry
    Sid       uint64
}
```

或更简单的：在循环中直接写入而不经过 batch：

```go
// 直接写入，在一次循环中完成序列化
w.WritePointRow(row)  // row 是 *types.PointRow
```

#### 2.9 收益

- 消除 compaction 路径上每条非重复行的 `make([]InternalField, N)` 分配
- 节省约 N×24B 的堆分配 / row（5 字段 = 120B × 100万行 = 120MB 累积分配，GC 压力显著）
- 减少一次完整的字段遍历

#### 2.10 实现路径

1. 在 `sstable.Writer` 中添加 `WritePointRows(rows []*types.PointRow) error` 方法
2. 内部直接从 `FieldEntry` 序列化写入（复用现有的 per-field 写入逻辑）
3. Compaction 循环中去掉 `FieldEntryToInternalFields` 调用

---

### 方案 6C：MergeHeapItem 复用（快速赢）

#### 2.11 问题

当前 `MergeIterator.Next()` 每次迭代都 `new(MergeHeapItem)` 压入堆。100 万行 = 100 万次 `MergeHeapItem` 分配。

#### 2.12 优化

复用堆中 Pop 出的 `MergeHeapItem`：

```go
func (m *MergeIterator) Next() bool {
    if len(*m.heap) == 0 || m.err != nil {
        m.current = nil
        return false
    }

    m.current = heap.Pop(m.heap).(*MergeHeapItem)

    if m.current.Iter.Next() {
        p := m.current.Iter.Point()
        // 复用 m.current 而非 new
        m.current.Point = p
        m.current.Timestamp = p.Timestamp
        // m.current.Iter 和 m.current.Idx 不变
        heap.Push(m.heap, m.current)
    }

    return true
}
```

#### 2.13 收益

- 消除每条行一个 `*MergeHeapItem` 分配
- 100 万行 ≈ 节省 100 万次 32B 的堆分配 ≈ 32MB 总分配量减少
- 改动极小（4 行代码），风险极低

---

### 方案 6D：pointsToWrite 预分配 + 池化（辅助优化）

#### 2.14 问题

`pointsToWrite` 在每次 `flushBatch` 后清空但底层数组可能保留。目前使用 `pointsToWrite[:0]`（已有保留），但初始分配较小。

#### 2.15 优化

```go
pointsToWrite := make([]types.InternalPoint, 0, mergeBatchSize+mergeBatchSize/10)
```

预留 10% 额外容量，避免 append 时的扩容分配。

#### 2.16 收益

小优化，100 万行中仅在 flush（1000 次）中避免扩容，收益有限但实现代价为零。

---

## 三、代码重构：消除 merge.go 与 level.go 的重复

### 3.1 当前状态

`merge.go:125-157` 和 `level.go:364-395` 中的去重循环逻辑**几乎完全相同**（约 30 行），差异仅在于：
- merge.go 跟踪 `task.DuplicateCount`
- merge.go 调用 `cm.ReportProgress()`

### 3.2 优化

提取公共的去重函数：

```go
// DedupedMergeWriter 执行去重合并写入。
type DedupedMergeWriter struct {
    writer       *sstable.Writer
    tombstones   *TombstoneSet
    dedup        *DedupFilter
    batch        []types.InternalPoint
    outputCount  int
    dupCount     int
}

func NewDedupedMergeWriter(w *sstable.Writer, ts *TombstoneSet, estimatedRows int) *DedupedMergeWriter

func (dw *DedupedMergeWriter) WriteMerged(merged *MergeIterator) error
func (dw *DedupedMergeWriter) Flush() error
func (dw *DedupedMergeWriter) Close() error
```

两个 Manager 的 Merge 方法都使用同一个 `DedupedMergeWriter`，仅配置差异（进度回调、去重模式）。

---

## 四、依赖关系与执行顺序

```
6C (MergeHeapItem 复用) ──→ 6A (Bloom 去重) ──→ 6B (消除 InternalField)
  ↓                          ↓
6D (预分配)                重构消除重复代码
```

### 推荐执行批次

| 批次 | 内容 | 改动量 | 预期收益 | 风险 |
|------|------|--------|---------|------|
| **第 1 批** | 6C MergeHeapItem 复用 + 6D 预分配 | ~10 行 | -100万次 heap 分配 | ★☆ |
| **第 2 批** | 6A Bloom Filter + 滑动窗口去重 | ~150 行（新文件）| 内存峰值 -90% | ★★☆ |
| **第 3 批** | 消除 merge.go/level.go 重复代码 | ~80 行（重构）| 维护性提升 | ★☆☆ |
| **第 4 批** | 6B 消除 InternalField | ~50 行 | 减少 per-row 字段拷贝 | ★★☆ |

---

## 五、验收标准

1. **Bloom Filter 单元测试**：假阳性率在预期范围内（<3% @ 100万条目）
2. **DedupFilter 单元测试**：松弛模式下重复行正确率 ≥ 99%
3. **内存基准测试**：100 万行 compaction 内存峰值 < 15 MB（当前 ~58 MB）
4. **E2E compaction_test**：全部 8 个测试用例通过
5. **数据完整性**：compaction 合并后数据量一致（允许 <0.1% 重复偏差）
6. **MergeHeapItem 复用**：无数据竞争，-race 通过

---

## 六、风险与权衡

| 风险 | 缓解措施 |
|------|---------|
| Bloom 假阳性导致少量重复未被去重 | 窗口足够大 + 小数据集自动回退严格模式 |
| 滑动窗口大小选择不当 | 提供配置项 `DedupWindowSize`，默认 50000 |
| 重构 loop 引入行为差异 | 先写测试覆盖现有行为，再重构 |
| MergeHeapItem 复用引入并发问题 |  单线程模型不变，仅复用结构体 |
