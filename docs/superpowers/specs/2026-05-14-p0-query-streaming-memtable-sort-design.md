# P0 优化方案：查询路径流式化 + MemTable 有序性加固

**日期**：2026-05-14
**状态**：设计完成，待实现
**关联检视**：`docs/review/code-review-2026-05-13-1800.md`

---

## 一、目标与范围

### 1.1 目标

1. **消除查询路径 OOM 风险**：`ShardIterator` 不再全量预加载 SSTable 数据，改为流式块级读取 + 堆归并
2. **消除冗余代码路径**：移除 `Shard.Read` / `readFromSSTable` / `readSSTableFile`，统一为 `ShardIterator`
3. **加固 MemTable 有序性**：修复 `sorted` 字段语义，确保 WAL Replay 后数据有序

### 1.2 范围

| 涉及包 | 文件 | 改动类型 |
|--------|------|----------|
| `internal/storage/shard/` | `shard_io.go` | 移除 Read/readFromSSTable/readSSTableFile |
| `internal/storage/shard/` | `iterator.go` | 重写，用 SSTableMergeIterator 替代预加载 |
| `internal/storage/shard/sstable/` | 新增 `merge_iterator.go` | 多 SSTable 堆归并 |
| `internal/storage/memtable/` | `memtable.go` | Write 逻辑修正 + 新增 Sort() |
| `internal/storage/shard/` | `shard.go` | ReplayWAL 结尾调用 Sort() |
| `internal/storage/shard/` | `shard_test.go` 等测试 | 迁移 Shard.Read → ShardIterator |
| `tests/e2e/` | `write_and_compact/main.go` | 迁移 Shard.Read → ShardIterator |

---

## 二、P0-1：查询路径流式化

### 2.1 新增：SSTableMergeIterator

**位置**：`internal/storage/shard/sstable/merge_iterator.go`

**职责**：封装多个 SSTable 文件的流式堆归并，按时间戳升序输出。

**接口**：

```go
type MergeIterator struct {
    iters   []*Iterator        // 每个 SSTable 一个 Iterator
    heap    mergeHeap          // min-heap，按当前行时间戳比较
    current *types.PointRow
    err     error
    schema  Schema
    // 引用计数相关
    shardAccess ShardAccess   // 用于 AcquireSSTRef/ReleaseSSTRef
    files       []string      // 已打开的文件，用于关闭时释放引用
}

// NewMergeIterator 创建多 SSTable 合并迭代器
// files: SSTable 文件路径列表
// startTime, endTime: 时间范围过滤
// schema: SSTable 解码所需的 schema
func NewMergeIterator(files []string, startTime, endTime int64, schema Schema, shardAccess ShardAccess) (*MergeIterator, error)

// Next 返回下一个时间戳最小的行
func (mi *MergeIterator) Next() bool

// Point 返回当前行
func (mi *Point) Point() *types.PointRow

// Close 关闭所有底层 Iterator 并释放引用
func (mi *MergeIterator) Close() error

// Err 返回遍历过程中发生的错误
func (mi *MergeIterator) Err() error
```

**内部堆归并**：

```
1. 为每个 SSTable 文件创建 sstable.Iterator，SeekToTime(startTime)
2. 每个 Iterator 读取第一行，推入 min-heap
3. Next() 弹出堆顶（最小时间戳），从对应 Iterator 取下一行推回
4. 过滤 endTime 范围外的行
5. 堆为空时迭代结束
```

**设计要点**：
- 不区分 flat/leveled 目录结构——由调用方（ShardIterator）负责收集文件列表
- 每个 Iterator 内部已实现块级按需读取（`loadBlock`），MergeIterator 不关心块边界
- 引用计数：MergeIterator 持有 SSTable 文件的 `AcquireSSTRef`，Close 时释放

### 2.2 修改：ShardIterator

**位置**：`internal/storage/shard/iterator.go`

**变化**：

```diff
- rows    []*types.PointRow          // SSTable 全量预加载
- rowIdx  int                        // 当前在 rows 中的位置
- sstRow  *types.PointRow            // 当前 peek 的 SSTable 行
+ sstIter *sstable.MergeIterator     // 流式 SSTable 归并
+ sstRow  *types.PointRow            // 当前 peek 的 SSTable 行（语义不变）
```

**NewShardIterator 变化**：

```diff
- rows, err := shard.readFromSSTable(startTime, endTime, maxRows)
- si.rows = rows
- si.sstRow = si.rows[0]
+ sstFiles := shard.listSSTableFiles()  // 收集 SSTable 文件列表
+ sstIter, err := sstable.NewMergeIterator(sstFiles, startTime, endTime, schema, shard)
+ si.sstIter = sstIter
+ if sstIter.Next() { si.sstRow = sstIter.Point() }
```

**nextSstRowLocked 变化**：

```diff
- si.rowIdx++
- if si.rowIdx < len(si.rows) { return si.rows[si.rowIdx] }
- return nil
+ if si.sstIter.Next() { return si.sstIter.Point() }
+ return nil
```

**其他方法**：`Next()`、`Current()` 等归并逻辑不变（仅数据源变了）。

**Close 支持**：新增 `ShardIterator.Close()` 释放 `sstIter` 持有的 SSTable 引用。

### 2.3 新增：shard.listSSTableFiles()

**位置**：`internal/storage/shard/shard_io.go`（或 `shard.go`）

```go
// listSSTableFiles 列出 Shard 中所有 SSTable 文件
// 自动处理 flat（data/sst_*.bin）和 leveled（data/L0/sst_*.bin, ...）两种目录结构
func (s *Shard) listSSTableFiles() []string
```

提取自现有 `readFromSSTable` 中的文件扫描逻辑。

### 2.4 移除代码

| 方法 | 位置 | 移除理由 |
|------|------|----------|
| `Shard.Read()` | `shard_io.go:116-153` | 全量加载，已无调用方 |
| `Shard.readFromSSTable()` | `shard_io.go:155-230` | 全量加载，被 MergeIterator 替代 |
| `Shard.readSSTableFile()` | `shard_io.go:232-256` | 全量加载，被 MergeIterator 替代 |

### 2.5 调用方迁移

**测试迁移模式**：

```go
// 旧：rows, err := s.Read(startTime, endTime)
// 新：
iter := NewShardIterator(s, startTime, endTime, 0)
defer iter.Close()
rows := collectAll(iter)

// 辅助函数
func collectAll(si *ShardIterator) []*types.PointRow {
    var rows []*types.PointRow
    for row := si.Next(); row != nil; row = si.Next() {
        rows = append(rows, row)
    }
    return rows
}
```

**影响范围**：

| 文件 | 调用点 | 迁移方式 |
|------|--------|----------|
| `shard_test.go` | 14 处 | `s.Read()` → 辅助函数 |
| `shard_extra_test.go` | 5 处 | 同上 |
| `write_and_compact/main.go` | 1 处 | 直接使用 `ShardIterator` |

---

## 三、P0-2：MemTable 有序性加固

### 3.1 修改：Write() 逻辑

**位置**：`internal/storage/memtable/memtable.go`

**变更前**：

```go
if m.activeCount > 1 && m.active[m.activeCount-1].Timestamp < m.active[m.activeCount-2].Timestamp {
    sort.Slice(m.active, ...)
    m.sorted = true
} else {
    m.sorted = true
}
```

**变更后**：

```go
if !m.sorted || (m.activeCount > 1 && m.active[m.activeCount-1].Timestamp < m.active[m.activeCount-2].Timestamp) {
    sort.Slice(m.active, func(i, j int) bool {
        return m.active[i].Timestamp < m.active[j].Timestamp
    })
}
m.sorted = true
```

**语义变化**：
- `!m.sorted` 守卫：如果已知未排序（如刚 Swap），无条件全量排序
- last-two 优化：如果已排序 + 末尾乱序，触发局部检查后排序
- 无条件设 `sorted = true`（已通过排序保证）

### 3.2 新增：Sort() 方法

```go
// Sort 对 active 进行排序，确保数据有序
// 用于 WAL Replay 后或任何需要防御性排序的场景
func (m *MemTable) Sort() {
    m.mu.Lock()
    defer m.mu.Unlock()
    if m.activeCount > 1 {
        sort.Slice(m.active, func(i, j int) bool {
            return m.active[i].Timestamp < m.active[j].Timestamp
        })
    }
    m.sorted = true
}
```

### 3.3 修改：ReplayWAL 结束后显式排序

**位置**：`internal/storage/shard/shard.go:557-600`

在 ReplayWAL 中，replay 循环结束后、最终 flush 前调用：

```go
// replay 完成后，确保 MemTable 数据有序
s.memTable.Sort()

// replay 完成后，如果 MemTable 还有数据，flush 到 SSTable
if s.memTable.Count() > 0 {
    ...
}
```

### 3.4 sorted 生命周期

| 操作 | sorted 值 | 原因 |
|------|----------|------|
| `NewMemTable` | `false` | 初始状态，空切片 |
| `Swap()` | `false` | 新 active 为空，保守标记 |
| `Write()` | `true` | 已排序保证 |
| `MergePassiveBack()` | `true` | 方法内部已排序 |
| `ClearPassive()` | 不变 | 不涉及 active |
| `Sort()` | `true` | 方法内部已排序 |

---

## 四、风险评估

### 4.1 P0-1 风险

| 风险 | 概率 | 缓解措施 |
|------|------|----------|
| SSTableMergeIterator 性能退化（块级随机访问） | 中 | Iterator 已有块级缓冲；单次查询文件数通常 <10；benchmark 验证 |
| 测试迁移引入回归 | 中 | 所有测试用例保持断言不变；collectAll 辅助函数模拟 Shard.Read 语义 |
| 引用计数泄漏 | 低 | MergeIterator.Close() 统一释放；defer 模式 |

### 4.2 P0-2 风险

| 风险 | 概率 | 缓解措施 |
|------|------|----------|
| `!m.sorted` 守卫导致每次 Swap 后第一次 Write 全量排序 | 低 | Swap 后的 active 为空（0 元素），sort.Slice 对空/单元素切片是 O(1) |
| 未发现的乱序产生路径 | 低 | Sort() 提供防御性出口；可被外部调用于任意需要的位置 |

---

## 五、测试策略

### 5.1 新增测试

| 测试 | 覆盖目标 |
|------|----------|
| `TestSSTableMergeIterator_SingleFile` | 单文件归并正确性 |
| `TestSSTableMergeIterator_MultiFile` | 多文件归并 + 时间范围过滤 |
| `TestSSTableMergeIterator_EmptyFiles` | 空文件列表处理 |
| `TestSSTableMergeIterator_SeekToTime` | 时间范围跳转 |
| `TestShardIterator_Streaming` | 验证 SSTable 数据未被全量预加载（内存断言） |
| `TestMemTable_SortAfterSwap` | Swap 后排序正确性 |
| `TestMemTable_SortAfterWALReplay` | WAL Replay 后排序正确性 |
| `TestMemTable_WriteUnsortedActive` | 未排序 active 的 Write 行为 |

### 5.2 回归测试

- 运行全部 `go test ./internal/storage/shard/... -count=1`
- 运行全部 e2e 测试
- 覆盖率不变（100%）

### 5.3 Benchmark

- `BenchmarkShardIterator_Streaming`：对比改造前后的内存分配和耗时

---

## 六、验收标准

1. `Shard.Read`、`readFromSSTable`、`readSSTableFile` 从代码中完全移除
2. `ShardIterator` 不持有全量 SSTable 数据（无 `[]*types.PointRow` 预加载切片）
3. `SSTableMergeIterator` 每个 Next() 仅加载当前块的数据
4. `sorted` 字段满足 3.4 生命周期表
5. WAL Replay 结束时调用 `Sort()`
6. 所有现有测试用例通过（迁移后）
7. 新增测试覆盖全部新代码
8. golangci-lint 0 issues
9. 覆盖率 ≥ 90%（目标 100%）
