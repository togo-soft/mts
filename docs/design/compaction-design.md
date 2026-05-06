# MTS Compaction 设计文档

- 文档版本：v1.0
- 创建日期：2026-05-06
- 状态：待审核

---

## 1. 背景与问题

### 1.1 当前数据写入流程

```
写入 → WAL → MemTable → Flush → SSTable
```

每次 Flush 生成一个新的 SSTable 文件。随着时间推移，同一 Shard 下会积累多个 SSTable：

```
data/
├── sst_0/    # 包含 Point(t=100), Point(t=200)
├── sst_1/    # 包含 Point(t=150), Point(t=250)
├── sst_2/    # 包含 Point(t=180), Point(t=220)
└── ...
```

### 1.2 存在的问题

| 问题 | 描述 | 影响 |
|------|------|------|
| **数据重复** | WAL replay 不去重 + 多次 flush 导致同一 (timestamp, sid) 在多个 SSTable 存在 | 查询返回重复数据 |
| **IO 放大** | 查询需要读取所有 SSTable | 查询性能下降 |
| **文件膨胀** | 小文件堆积，浪费存储空间 | 存储成本增加 |
| **元数据开销** | 每个 SSTable 独立 schema/index | 内存占用增加 |

### 1.3 根因分析

1. **MemTable flush 后 WAL TruncateCurrent() 清空文件内容，但文件本身仍存在**
2. **WAL replay 时未做去重检查**
3. **没有 Compaction 机制合并重复数据**

---

## 2. 设计目标

### 2.1 功能目标

1. **数据去重**：合并多个 SSTable 时，相同 (timestamp, sid) 只保留一条
2. **存储优化**：删除冗余数据，减少存储占用
3. **查询加速**：减少查询时需要扫描的 SSTable 数量

### 2.2 非功能目标

1. **低侵入性**：Compaction 在后台执行，不影响正常写入
2. **可中断**：Compaction 可被中断，不丢失数据
3. **可配置**：触发条件和策略可通过配置控制

### 2.3 不纳入本设计的内容

- Leveled Compaction（多层级）
- 跨 Shard Compaction
- Compaction 进度暂停/恢复
- 实时查询与 Compaction 的 MVCC

---

## 3. 技术方案总览

### 3.1 Compaction 类型

| 类型 | 触发条件 | 说明 |
|------|----------|------|
| **Minor Compaction** | MemTable 达到 flush 条件 | 已实现（MemTable → SSTable） |
| **Major Compaction** | SSTable 数量超阈值 / 定时 | 本设计实现 |

### 3.2 实现路径

```
Phase 1: 基础 Major Compaction（核心合并逻辑）
Phase 2: 触发策略（阈值 + 定时）
Phase 3: 优化（并发、安全加固）
```

---

## 4. Phase 1: 基础 Major Compaction

### 4.1 核心数据结构

#### 4.1.1 CompactionManager

```go
// Package shard
// compaction.go

// CompactionConfig Compaction 配置
type CompactionConfig struct {
    // 最大 SSTable 数量，超过此值触发 compaction
    MaxSSTableCount int
    
    // Compaction 输出目录
    OutputDir string
    
    // 单次 compaction 最大文件数（0 表示不限制）
    MaxCompactionBatch int
}

// DefaultCompactionConfig 返回默认配置
func DefaultCompactionConfig() *CompactionConfig {
    return &CompactionConfig{
        MaxSSTableCount:     4,
        MaxCompactionBatch:  0,
    }
}

// CompactionManager 管理 Shard 的 Compaction
type CompactionManager struct {
    shard   *Shard
    config  *CompactionConfig
    mu      sync.Mutex // 保护 compaction 状态
}

// NewCompactionManager 创建 CompactionManager
func NewCompactionManager(shard *Shard, config *CompactionConfig) *CompactionManager {
    if config == nil {
        config = DefaultCompactionConfig()
    }
    return &CompactionManager{
        shard:  shard,
        config: config,
    }
}
```

#### 4.1.2 CompactionTask

```go
// CompactionTask 描述一次 compaction 任务
type CompactionTask struct {
    // 输入文件
    inputFiles []string
    
    // 输出文件路径
    outputPath string
    
    // 进度 (0-100)
    progress int
    
    // 开始时间
    startedAt time.Time
    
    // 合并后保留的记录数
    outputCount int
    
    // 删除的重复记录数
    duplicateCount int
}

// NewCompactionTask 创建 compaction 任务
func NewCompactionTask(inputFiles []string, outputPath string) *CompactionTask {
    return &CompactionTask{
        inputFiles:  inputFiles,
        outputPath:  outputPath,
        progress:    0,
        startedAt:   time.Now(),
    }
}
```

### 4.2 核心算法：归并去重

#### 4.2.1 合并算法流程

```
Input: [sst_0, sst_1, sst_2]  (各自内部按 timestamp 有序)
Output: sst_merged (按 timestamp 有序，相同 (timestamp, sid) 只保留一条)

Step 1: 创建输出 Writer (sst_new)
Step 2: 使用 k-way merge 归并所有输入
Step 3: 对于相同 (timestamp, sid) 的多条记录，保留最新一条
Step 4: 写入合并后的数据到 sst_new
Step 5: 关闭 sst_new
Step 6: 原子性替换旧文件
```

#### 4.2.2 算法实现

```go
// Compact 执行 compaction 合并
// 返回合并后的 SSTable 路径，和被删除的旧文件列表
func (cm *CompactionManager) Compact(ctx context.Context) (string, []string, error) {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    // Step 1: 收集待合并的 SSTable
    sstFiles, err := cm.collectSSTables()
    if err != nil {
        return "", nil, fmt.Errorf("collect sstables: %w", err)
    }
    
    if len(sstFiles) < 2 {
        // 少于 2 个文件不需要 compaction
        return "", nil, nil
    }
    
    // Step 2: 创建输出文件
    outputSeq := cm.shard.NextSSTSeq()
    outputPath := filepath.Join(cm.shard.DataDir(), fmt.Sprintf("sst_%d", outputSeq))
    
    task := NewCompactionTask(sstFiles, outputPath)
    
    // Step 3: 执行归并
    if err := cm.merge(ctx, task); err != nil {
        // 清理临时文件
        _ = os.RemoveAll(outputPath)
        return "", nil, fmt.Errorf("merge failed: %w", err)
    }
    
    // Step 4: 原子性替换
    if err := cm.commit(task); err != nil {
        _ = os.RemoveAll(outputPath)
        return "", nil, fmt.Errorf("commit failed: %w", err)
    }
    
    return task.outputPath, task.inputFiles, nil
}

// collectSSTables 收集需要 compaction 的 SSTable
func (cm *CompactionManager) collectSSTables() ([]string, error) {
    dataDir := filepath.Join(cm.shard.Dir(), "data")
    entries, err := os.ReadDir(dataDir)
    if err != nil {
        return nil, err
    }
    
    var sstFiles []string
    for _, entry := range entries {
        if !entry.IsDir() {
            continue
        }
        if strings.HasPrefix(entry.Name(), "sst_") {
            sstFiles = append(sstFiles, filepath.Join(dataDir, entry.Name()))
        }
    }
    
    // 按文件名排序（确保序号小的先处理）
    sort.Strings(sstFiles)
    return sstFiles, nil
}

// merge 执行归并操作
func (cm *CompactionManager) merge(ctx context.Context, task *CompactionTask) error {
    // 打开所有输入文件
    readers := make([]*sstable.Reader, 0, len(task.inputFiles))
    for _, path := range task.inputFiles {
        r, err := sstable.NewReader(path)
        if err != nil {
            for _, r := range readers {
                _ = r.Close()
            }
            return err
        }
        readers = append(readers, r)
    }
    defer func() {
        for _, r := range readers {
            _ = r.Close()
        }
    }()
    
    // 创建输出 Writer
    w, err := sstable.NewWriter(cm.shard.Dir(), cm.shard.NextSSTSeq(), 0)
    if err != nil {
        return err
    }
    
    // 创建迭代器列表（每个输入一个）
    iterators := make([]*sstable.Iterator, 0, len(readers))
    for _, r := range readers {
        iterators = append(iterators, r.Iterator())
    }
    
    // k-way merge
    merged := mergeIterators(ctx, iterators)
    
    // 用于去重：(timestamp, sid) -> 已见过的标记
    seen := make(map[string]bool) // key = fmt.Sprintf("%d-%d", timestamp, sid)
    
    var lastTs int64 = math.MinInt64
    var lastSid uint64 = 0
    
    for merged.Next() {
        select {
        case <-ctx.Done():
            _ = w.Close()
            return ctx.Err()
        default:
        }
        
        point := merged.Point()
        key := fmt.Sprintf("%d-%d", point.Timestamp, point.Sid)
        
        // 去重：相同 (timestamp, sid) 只保留最新（这里简化处理，实际应该比较写入顺序或文件序号）
        if seen[key] {
            task.duplicateCount++
            continue
        }
        
        seen[key] = true
        lastTs = point.Timestamp
        lastSid = point.Sid
        
        // 写入合并后的数据
        if err := w.AppendPoint(point); err != nil {
            _ = w.Close()
            return err
        }
        task.outputCount++
        
        // 更新进度
        task.progress = int(float64(task.outputCount) / float64(estimateTotal) * 100)
    }
    
    if err := merged.Error(); err != nil {
        _ = w.Close()
        return err
    }
    
    if err := w.Close(); err != nil {
        return err
    }
    
    return nil
}

// commit 原子性提交 compaction 结果
func (cm *CompactionManager) commit(task *CompactionTask) error {
    // 1. 验证输出文件完整性
    if err := cm.verifyOutput(task.outputPath); err != nil {
        return fmt.Errorf("verify output: %w", err)
    }
    
    // 2. 原子性替换：删除旧文件
    for _, oldFile := range task.inputFiles {
        if err := os.RemoveAll(oldFile); err != nil {
            // 删除失败不影响结果，旧文件下次再清理
            slog.Warn("failed to remove old sstable", "path", oldFile, "error", err)
        }
    }
    
    return nil
}
```

#### 4.2.3 K-way Merge 迭代器

```go
// mergeIterators 将多个有序迭代器合并为一个有序迭代器
type mergeIterator struct {
    iterators []*sstable.Iterator
    heap      []*heapItem
}

type heapItem struct {
    iter *sstable.Iterator
    point *types.Point
    idx   int // 迭代器索引
}

func mergeIterators(ctx context.Context, iters []*sstable.Iterator) *mergeIterator {
    m := &mergeIterator{
        iterators: iters,
        heap:      make([]*heapItem, 0, len(iters)),
    }
    
    // 初始化：每个迭代器 advance 到第一个元素
    for i, iter := range iters {
        if iter.Next() {
            m.heap = append(m.heap, &heapItem{
                iter:  iter,
                point: iter.Point(),
                idx:   i,
            })
        }
    }
    
    // 构建最小堆
    heap.Init(m.heap)
    
    return m
}

func (m *mergeIterator) Next() bool {
    if len(m.heap) == 0 {
        return false
    }
    
    // 弹出最小元素
    item := heap.Pop(m.heap).(*heapItem)
    
    // 将该迭代器 advance 到下一个元素
    if item.iter.Next() {
        item.point = item.iter.Point()
        heap.Push(m.heap, item)
    }
    
    return len(m.heap) > 0
}

func (m *mergeIterator) Point() *types.Point {
    if len(m.heap) == 0 {
        return nil
    }
    return m.heap[0].point
}

func (m *mergeIterator) Error() error {
    return nil // TODO: 收集所有迭代器的错误
}
```

### 4.3 目录结构变更

Compaction 后目录结构：

```
{shardDir}/
├── wal/
├── data/
│   ├── sst_0/          # Compaction 前
│   ├── sst_1/          # Compaction 前
│   ├── sst_2/          # Compaction 前
│   └── sst_3/          # Compaction 后生成的新文件
└── meta.json
```

Compaction 完成后：

```
{shardDir}/
├── wal/
├── data/
│   └── sst_3/          # 合并后的文件
└── meta.json
```

### 4.4 新增文件

| 文件 | 用途 |
|------|------|
| `internal/storage/shard/compaction.go` | Compaction 核心逻辑 |
| `internal/storage/shard/compaction_test.go` | 单元测试 |

---

## 5. Phase 2: 触发策略

### 5.1 触发条件

#### 5.1.1 SSTable 数量阈值

```go
// ShouldCompact 检查是否应该触发 compaction
func (cm *CompactionManager) ShouldCompact() bool {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    files, err := cm.collectSSTables()
    if err != nil {
        return false
    }
    
    return len(files) >= cm.config.MaxSSTableCount
}
```

#### 5.1.2 定时触发

```go
// StartPeriodicCheck 启动定期检查
func (cm *CompactionManager) StartPeriodicCheck(interval time.Duration, stopCh <-chan struct{}) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            if cm.ShouldCompact() {
                ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
                _, _, err := cm.Compact(ctx)
                if err != nil {
                    slog.Error("compaction failed", "error", err)
                }
                cancel()
            }
        case <-stopCh:
            return
        }
    }
}
```

### 5.2 ShardManager 集成

```go
// Shard 新增字段
type Shard struct {
    // ... 现有字段 ...
    compaction *CompactionManager
}

// NewShard 修改
func NewShard(cfg ShardConfig) *Shard {
    shard := &Shard{
        // ... 现有初始化 ...
    }
    
    // 创建 CompactionManager
    shard.compaction = NewCompactionManager(shard, cfg.CompactionConfig)
    
    return shard
}

// Shard.Flush 修改 - flush 后检查是否需要 compaction
func (s *Shard) Flush() error {
    // ... 现有 flush 逻辑 ...
    
    // Flush 成功后检查 compaction
    if s.compaction.ShouldCompact() {
        go func() {
            ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
            defer cancel()
            
            if _, _, err := s.compaction.Compact(ctx); err != nil {
                slog.Error("background compaction failed", "error", err)
            }
        }()
    }
    
    return nil
}
```

### 5.3 CompactionConfig 完整定义

```go
// CompactionConfig 完整配置
type CompactionConfig struct {
    // 是否启用 compaction
    Enabled bool
    
    // SSTable 数量阈值，超过此值触发 compaction
    MaxSSTableCount int
    
    // 定期检查间隔（0 表示不启用定期检查）
    CheckInterval time.Duration
    
    // 单次 compaction 最大文件数（0 表示不限制）
    MaxCompactionBatch int
    
    // compaction 超时时间
    Timeout time.Duration
}

func DefaultCompactionConfig() *CompactionConfig {
    return &CompactionConfig{
        Enabled:            true,
        MaxSSTableCount:    4,
        CheckInterval:      1 * time.Hour,
        MaxCompactionBatch: 0, // 不限制
        Timeout:            30 * time.Minute,
    }
}
```

---

## 6. Phase 3: 优化

### 6.1 并发安全

#### 6.1.1 Compaction 与写入的并发

**问题**：Compaction 执行期间，写入操作仍可进行，可能导致数据不一致。

**解决方案**：
1. Compaction 读取的是「执行开始时」的 SSTable 快照
2. Compaction 执行期间写入的新数据在下一次 Compaction 时合并
3. 不需要阻塞写入

```go
// Compact 快照语义
func (cm *CompactionManager) Compact(ctx context.Context) (string, []string, error) {
    cm.mu.Lock()
    
    // 收集当前 SSTable 列表（快照）
    sstFiles, err := cm.collectSSTables()
    if err != nil {
        cm.mu.Unlock()
        return "", nil, err
    }
    
    // 释放锁，允许写入继续
    cm.mu.Unlock()
    
    // 执行合并（可能耗时很长）
    outputPath, err := cm.mergeFiles(ctx, sstFiles)
    if err != nil {
        return "", nil, err
    }
    
    // 重新加锁提交
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    // 验证输出并删除旧文件
    if err := cm.commit(outputPath, sstFiles); err != nil {
        return "", nil, err
    }
    
    return outputPath, sstFiles, nil
}
```

#### 6.1.2 查询与 Compaction 的并发

**问题**：查询读取多个 SSTable 时，Compaction 正在删除旧文件。

**解决方案**：
1. 使用引用计数跟踪 SSTable 是否被查询使用
2. Compaction 提交时只删除「已无引用」的文件

```go
// SSTableRef SSTable 引用
type SSTableRef struct {
    path    string
    refCnt  atomic.Int32
}

// Acquire 增加引用
func (r *SSTableRef) Acquire() { r.refCnt.Add(1) }

// Release 释放引用
func (r *SSTableRef) Release() { r.refCnt.Add(-1) }

// IsUnused 无引用
func (r *SSTableRef) IsUnused() bool { r.refCnt.Load() == 0 }

// Shard.AddSSTableRef 添加引用
func (s *Shard) AddSSTableRef(path string) *SSTableRef {
    s.sstRefsMu.Lock()
    defer s.sstRefsMu.Unlock()
    
    if ref, ok := s.sstRefs[path]; ok {
        ref.Acquire()
        return ref
    }
    
    ref := &SSTableRef{path: path}
    ref.Acquire()
    s.sstRefs[path] = ref
    return ref
}

// readFromSSTable 使用引用
func (s *Shard) readFromSSTable(...) {
    ref := s.AddSSTableRef(sstDir)
    defer ref.Release()
    
    // 读取...
}
```

### 6.2 进度追踪

```go
// CompactionProgress Compaction 进度
type CompactionProgress struct {
    TaskID      string
    InputFiles  []string
    OutputFile  string
    Progress    int    // 0-100
    Status      string // running, completed, failed
    StartedAt   time.Time
    CompletedAt time.Time
    Error       error
}

// GetProgress 获取当前 compaction 进度
func (cm *CompactionManager) GetProgress() *CompactionProgress {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    
    if cm.currentTask == nil {
        return nil
    }
    
    return &CompactionProgress{
        TaskID:    cm.currentTask.ID,
        Progress:  cm.currentTask.progress,
        Status:    cm.currentTask.status,
        StartedAt: cm.currentTask.startedAt,
    }
}
```

### 6.3 估算合并总数

```go
// estimateTotal 估算待合并的记录总数
func (cm *CompactionManager) estimateTotal(inputFiles []string) int {
    var total int
    for _, path := range inputFiles {
        r, err := sstable.NewReader(path)
        if err != nil {
            continue
        }
        // 使用 Index 获取行数
        if idx := r.Index(); idx != nil {
            total += int(idx.RowCount())
        }
        _ = r.Close()
    }
    return total
}
```

---

## 7. 数据流变更

### 7.1 写入流程（无变化）

```
写入 → WAL → MemTable → Flush → SSTable
                                   ↓
                              检查是否需要 Compaction
                                   ↓
                              是 → 后台执行 Major Compaction
                              否 → 完成
```

### 7.2 查询流程（无变化）

```
查询 → MemTable → SSTable → 归并排序 → 结果
                    ↑
              已被 Compaction 合并
```

### 7.3 Compaction 流程

```
触发条件满足 → 收集 SSTable → 创建新 Writer → K-way Merge → 去重写入
                                                            ↓
                                                   关闭 Writer
                                                            ↓
                                                   原子性替换
                                                            ↓
                                                   删除旧文件
```

---

## 8. 错误处理

### 8.1 错误分类

| 错误类型 | 处理策略 | 示例 |
|----------|----------|------|
| 输入文件不存在 | 跳过该文件，继续合并 | sst_1 被意外删除 |
| 读取失败 | 返回错误，中止 compaction | 磁盘 IO 错误 |
| 写入失败 | 清理输出文件，返回错误 | 磁盘满 |
| 超时 | 清理输出文件，返回错误 | compaction 时间过长 |
| 中断 | 清理输出文件 | Context 取消 |

### 8.2 恢复机制

```go
// Compact 如果失败，输出文件被清理
// 下次 compaction 会重新处理所有 SSTable
// 不会导致数据丢失
```

---

## 9. 测试计划

### 9.1 单元测试

| 测试用例 | 描述 |
|----------|------|
| `TestCompact_Basic` | 两个 SSTable 简单合并 |
| `TestCompact_WithDuplicates` | 有重复数据，验证去重 |
| `TestCompact_ThreeWay` | 三个 SSTable 归并 |
| `TestCompact_EmptySSTable` | 包含空文件的处理 |
| `TestCompact_AllDuplicates` | 全部重复，只保留一份 |
| `TestCompact_PreserveOrder` | 验证时间顺序正确 |
| `TestCompact_Progress` | 验证进度更新 |

### 9.2 集成测试

| 测试用例 | 描述 |
|----------|------|
| `TestCompact_AfterFlush` | Flush 后自动触发 compaction |
| `TestCompact_QueryDuring` | Compaction 期间查询仍正确 |
| `TestCompact_WriteDuring` | Compaction 期间写入正常 |

### 9.3 边界测试

| 测试用例 | 描述 |
|----------|------|
| `TestCompact_SingleFile` | 只有一个文件时不执行 |
| `TestCompact_LargeFile` | 大文件性能测试 |
| `TestCompact_Cancel` | 中断 compaction |

---

## 10. 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| Compaction 期间磁盘满 | 写入失败 | 预先检查空间 |
| Compaction 时间过长 | 阻塞查询 | 使用 context 超时 |
| Compaction 失败 | 数据冗余 | 下次重试 |
| 旧文件删除失败 | 磁盘占用高 | 记录未删除文件，下次清理 |
| 并发 compaction | 数据不一致 | 使用锁保护 |

---

## 11. 后续扩展

| 扩展点 | 描述 |
|--------|------|
| Leveled Compaction | 多层级压缩 |
| 跨 Shard Compaction | 全局优化 |
| Compaction 限流 | 避免占用过多 IO |
| 进度暂停/恢复 | 支持长时间中断 |
| Compaction 统计 | Metrics 上报 |

---

## 12. 总结

### Phase 1 交付物

- `internal/storage/shard/compaction.go` - Compaction 核心逻辑
- `internal/storage/shard/compaction_test.go` - 单元测试
- K-way merge 算法实现
- 基本的错误处理

### Phase 2 交付物

- 阈值触发逻辑
- 定时触发逻辑
- ShardManager 集成
- 配置项

### Phase 3 交付物

- 并发安全加固
- 引用计数机制
- 进度追踪 API
- 边界测试

---

## 13. 审核要点

1. **去重策略**：相同 (timestamp, sid) 保留哪一条？当前简化处理是否满足需求？
2. **原子性**：替换策略是否足够安全？
3. **并发安全**：是否覆盖所有场景？
4. **资源控制**：是否有超时/限流机制？
5. **可观测性**：进度/错误是否可追踪？
