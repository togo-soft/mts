# MTS Level Compaction 设计文档

- 文档版本：v2.0
- 创建日期：2026-05-07
- 状态：待审核
- 基于：InfluxDB TSM + Prometheus TSDB + VictoriaMetrics 融合方案

---

## 1. 背景与目标

### 1.1 现有问题

当前实现的平坦 Compaction（Flat Compaction）存在以下问题：

| 问题 | 描述 | 影响 |
|------|------|------|
| **线性增长** | 所有 SSTable 平等合并，随数量增长成本线性增加 | 大数据量时 compaction 变慢 |
| **无层次优化** | 无法利用数据的时间局部性 | 查询无法利用层次结构优化 |
| **IO 放大** | 每次合并所有文件 | 磁盘 IO 压力随数据量增长 |

### 1.2 设计目标

1. **层次化管理**：引入 Level 概念，实现指数增长的容量设计
2. **高效合并**：借鉴 VictoriaMetrics 的 Small Parts 优先合并策略
3. **崩溃恢复**：借鉴 Prometheus 的 Checkpoint 机制
4. **时序优化**：沿用 InfluxDB TSM 的文件格式和索引结构

### 1.3 参考方案

| 数据库 | 借鉴点 |
|--------|--------|
| **InfluxDB TSM** | TSM 文件格式、层次容量设计（10x 增长） |
| **Prometheus TSDB** | Checkpoint 机制、Tombstone 文件处理 |
| **VictoriaMetrics** | Small Parts 优先合并策略、灵活的 merge 调度 |

---

## 2. 架构设计

### 2.1 层次结构

```
┌─────────────────────────────────────────────────────────────┐
│                         Write Path                          │
│  Point → WAL → MemTable → Flush → L0 Part (small)         │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                    Compaction Path                          │
│  L0 → L1 → L2 → L3 → ... (exponential growth)           │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 层次配置

| 层次 | 大小上限 | 文件数上限 | 说明 |
|------|----------|------------|------|
| **L0** | 10 MB | 10 个 | MemTable flush 后直接落入 L0 |
| **L1** | 100 MB | 20 个 | L0 合并目标，指数增长起点 |
| **L2** | 1 GB | 10 个 | 10x L1 |
| **L3** | 10 GB | 5 个 | 10x L2 |
| **L4+** | 10x 增长 | 5 个 | 指数继续增长 |

**容量公式**: `L(n) = 100MB * 10^(n-1)`，其中 n >= 1

### 2.3 目录结构

```
{shardDir}/data/
├── L0/
│   ├── sst_001/
│   └── sst_002/
├── L1/
│   └── sst_010/
├── L2/
│   └── sst_050/
├── L3/
│   └── sst_200/
└── _manifest.json
```

### 2.4 Part 文件格式（沿用 TSM）

```
{shardDir}/data/L0/sst_{seq}/
├── _header.json      # Part header (magic, version, min/max time, size)
├── _timestamps.bin   # 时间戳列（delta encoded）
├── _sids.bin         # Series ID 列
├── _fields/          # 字段数据目录
│   ├── {field1}.bin
│   └── {field2}.bin
├── _index.bin        # Block 索引
├── _tombstones.json  # 删除标记
└── _checksum.json    # Merkle tree 根校验
```

---

## 3. 核心数据结构

### 3.1 LevelManifest

```go
// LevelManifest 管理层次结构元数据
type LevelManifest struct {
    mu     sync.RWMutex
    levels map[int]*Level // level -> Level info

    // Level 配置
    levelConfigs []LevelConfig

    // 全局序列号
    nextSeq uint64

    // Manifest 文件路径
    manifestPath string
}

// Level 表示单个层次
type Level struct {
    Level   int
    Parts   []PartInfo  // 该层包含的 Part
    Size    int64        // 该层总大小（字节）
    MaxSize int64        // 该层大小上限
}

// PartInfo 单个 Part 信息
type PartInfo struct {
    Name      string // 文件名（不含路径）
    Size      int64  // 大小
    MinTime   int64  // 最小时间戳
    MaxTime   int64  // 最大时间戳
    DeletedAt int64  // 删除标记时间（0 表示未删除）
}
```

### 3.2 LevelConfig

```go
// LevelConfig 层次配置
type LevelConfig struct {
    Level   int
    MaxSize int64 // 层次最大大小
    MaxParts int  // 最大文件数（L0 专用）
}

// DefaultLevelConfigs 返回默认层次配置
func DefaultLevelConfigs() []LevelConfig {
    return []LevelConfig{
        {Level: 0, MaxSize: 10 * 1024 * 1024, MaxParts: 10},       // L0: 10MB
        {Level: 1, MaxSize: 100 * 1024 * 1024, MaxParts: 0},      // L1: 100MB
        {Level: 2, MaxSize: 1024 * 1024 * 1024, MaxParts: 0},     // L2: 1GB
        {Level: 3, MaxSize: 10 * 1024 * 1024 * 1024, MaxParts: 0}, // L3: 10GB
        {Level: 4, MaxSize: 100 * 1024 * 1024 * 1024, MaxParts: 0}, // L4: 100GB
    }
}
```

### 3.3 CompactionCheckpoint

借鉴 Prometheus 的 Checkpoint 机制：

```go
// CompactionCheckpoint compaction 进度检查点
type CompactionCheckpoint struct {
    Version     int       // 版本号
    Level       int       // 当前 compaction 层次
    InputParts  []string  // 输入文件列表
    OutputPart  string    // 输出文件
    MergedSize  int64     // 已合并大小
    StartedAt   int64     // 开始时间戳
    Timestamp   int64     // 检查点更新时间
}

// Save 保存检查点到文件
func (cp *CompactionCheckpoint) Save(path string) error

// Load 从文件加载检查点
func (cp *CompactionCheckpoint) Load(path string) error

// Clear 删除检查点文件
func (cp *CompactionCheckpoint) Clear(path string) error
```

### 3.4 Tombstone

借鉴 Prometheus 的删除标记机制：

```go
// Tombstone 删除标记
type Tombstone struct {
    SID       uint64 `json:"sid"`       // Series ID
    MinTime   int64  `json:"mint"`    // 范围起始时间
    MaxTime   int64  `json:"maxt"`    // 范围结束时间
    DeletedAt int64  `json:"deleted"` // 删除时间
}

// TombstoneSet Tombstone 集合
type TombstoneSet struct {
    Tombstones []Tombstone `json:"tombstones"`
}

// ShouldDelete 检查给定 (timestamp, sid) 是否应被删除
func (ts *TombstoneSet) ShouldDelete(sid uint64, timestamp int64) bool
```

---

## 4. Compaction 策略

### 4.1 触发条件

| 层次 | 触发条件 | Compaction 类型 |
|------|----------|----------------|
| L0 | 文件数 >= MaxParts(10) 或 大小 >= MaxSize(10MB) | L0 → L1 |
| L1 | 大小 >= MaxSize(100MB) | L1 → L2 |
| L2 | 大小 >= MaxSize(1GB) | L2 → L3 |
| L3+ | 大小 >= MaxSize(10GB+) | Ln → L(n+1) |

### 4.2 Small Parts 优先策略（借鉴 VictoriaMetrics）

```go
// selectPartsForMerge 选择待合并的 Parts
// 策略：小文件优先，保持大文件顺序读取优势
func (lcm *LevelCompactionManager) selectPartsForMerge(level int) []PartInfo {
    l := lcm.manifest.GetLevel(level)

    // 按文件大小排序，小的优先
    sort.Slice(l.Parts, func(i, j int) bool {
        return l.Parts[i].Size < l.Parts[j].Size
    })

    // 选择直到达到下一层容量上限
    var selected []PartInfo
    var totalSize int64
    targetSize := lcm.levelMaxSize(level + 1) / 2 // 目标：合并到下一层容量的 50%

    for _, p := range l.Parts {
        if totalSize+p.Size > targetSize && len(selected) >= 1 {
            break
        }
        selected = append(selected, p)
        totalSize += p.Size
    }

    return selected
}
```

### 4.3 Compaction 流程

```
L0 触发 Compaction:
┌──────────────────────────────────────────────────────────────┐
│ 1. 收集 L0 所有 Parts（小文件优先，选择总大小约 L1 一半）      │
└──────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────┐
│ 2. 找出 L1 中时间范围重叠的 Parts                              │
└──────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────┐
│ 3. K-way merge 所有重叠文件，流式写入 L1 新文件                │
└──────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────┐
│ 4. 保存 Checkpoint，原子性更新 Manifest                        │
└──────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────┐
│ 5. 删除旧文件，清理 Checkpoint                                │
└──────────────────────────────────────────────────────────────┘
```

### 4.4 时间范围重叠检测

```go
// hasOverlap 检查两个 Part 时间范围是否重叠
func hasOverlap(p1, p2 PartInfo) bool {
    return p1.MinTime <= p2.MaxTime && p2.MinTime <= p1.MaxTime
}

// collectOverlapParts 收集与目标 Parts 时间重叠的所有 Parts
func (lcm *LevelCompactionManager) collectOverlapParts(level int, targets []PartInfo) []PartInfo {
    var overlaps []PartInfo

    for _, target := range targets {
        // 检查当前层
        current := lcm.manifest.GetLevel(level)
        for _, p := range current.Parts {
            if hasOverlap(p, target) {
                overlaps = append(overlaps, p)
            }
        }

        // 检查下一层
        next := lcm.manifest.GetLevel(level + 1)
        for _, p := range next.Parts {
            if hasOverlap(p, target) {
                overlaps = append(overlaps, p)
            }
        }
    }

    return deduplicateParts(overlaps)
}
```

---

## 5. 崩溃恢复

### 5.1 Checkpoint 机制

借鉴 Prometheus 的定期 checkpoint 设计：

```go
// SaveCheckpoint 保存 compaction 进度
func (lcm *LevelCompactionManager) SaveCheckpoint(cp *CompactionCheckpoint) error {
    path := filepath.Join(lcm.shardDir, "data", "_compaction.cp")
    return cp.Save(path)
}

// LoadCheckpoint 加载 compaction 进度
func (lcm *LevelCompactionManager) LoadCheckpoint() (*CompactionCheckpoint, error) {
    path := filepath.Join(lcm.shardDir, "data", "_compaction.cp")
    cp := &CompactionCheckpoint{}
    err := cp.Load(path)
    if os.IsNotExist(err) {
        return nil, nil // 没有 checkpoint，正常启动
    }
    return cp, err
}

// ClearCheckpoint 清理 checkpoint
func (lcm *LevelCompactionManager) ClearCheckpoint() error {
    path := filepath.Join(lcm.shardDir, "data", "_compaction.cp")
    return os.Remove(path)
}
```

### 5.2 恢复流程

```
启动时:
┌──────────────────────────────────────────────────────────────┐
│ 1. 检查是否存在 _compaction.cp                                │
└──────────────────────────────────────────────────────────────┘
                              ↓ (不存在)
┌──────────────────────────────────────────────────────────────┐
│ 2. 正常启动，无恢复操作                                      │
└──────────────────────────────────────────────────────────────┘
                              ↓ (存在)
┌──────────────────────────────────────────────────────────────┐
│ 3. 加载 Checkpoint，获取未完成的 compaction 信息             │
└──────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────┐
│ 4. 清理未完成的输出文件（如果存在）                           │
└──────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────┐
│ 5. 删除 Checkpoint 文件                                     │
└──────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────┐
│ 6. 正常启动，下次 compaction 会重新处理                      │
└──────────────────────────────────────────────────────────────┘
```

### 5.3 Tombstone 压缩

```go
// CompactTombstones 清理已过期的 tombstone
// 策略：tombstone 至少保留一个 compaction 周期
func (lcm *LevelCompactionManager) CompactTombstones() error {
    retentionPeriod := lcm.config.TombstoneRetention

    l := lcm.manifest.GetLevel(0)
    for _, p := range l.Parts {
        tombstones := p.LoadTombstones()
        if tombstones == nil {
            continue
        }

        // 过滤掉已过期的 tombstone
        var active []Tombstone
        now := time.Now().Unix()
        for _, t := range tombstones.Tombstones {
            if now-t.DeletedAt < int64(retentionPeriod.Seconds()) {
                active = append(active, t)
            }
        }

        if len(active) != len(tombstones.Tombstones) {
            if len(active) == 0 {
                // 所有 tombstone 都过期，删除文件
                _ = p.RemoveTombstones()
            } else {
                // 更新 tombstone 文件
                _ = p.SaveTombstones(&TombstoneSet{Tombstones: active})
            }
        }
    }

    return nil
}
```

---

## 6. 并发控制

### 6.1 锁策略

```go
type LevelCompactionManager struct {
    shard *Shard
    config *LevelCompactionConfig

    // Manifest 锁（读写锁）
    manifestMu sync.RWMutex

    // Compaction 执行锁（互斥）
    compactMu   sync.Mutex
    compactInProgress atomic.Int32

    // 当前 compaction 进度
    currentCP *CompactionCheckpoint
}
```

### 6.2 原子性更新

```go
// CommitCompaction 原子性提交 compaction 结果
func (lcm *LevelCompactionManager) CommitCompaction(cp *CompactionCheckpoint, newParts []PartInfo) error {
    lcm.manifestMu.Lock()
    defer lcm.manifestMu.Unlock()

    // 1. 从源层删除旧 Parts
    srcLevel := lcm.manifest.GetLevel(cp.Level)
    srcLevel.RemoveParts(cp.InputParts)

    // 2. 添加新 Parts 到目标层
    dstLevel := lcm.manifest.GetLevel(cp.Level + 1)
    dstLevel.AddParts(newParts)

    // 3. 更新 Manifest 文件
    if err := lcm.manifest.Save(); err != nil {
        return err
    }

    // 4. 删除旧文件
    for _, p := range cp.InputParts {
        _ = os.RemoveAll(p.Path)
    }

    // 5. 清理 Checkpoint
    _ = lcm.ClearCheckpoint()

    return nil
}
```

---

## 7. 向后兼容性

### 7.1 旧格式检测

```go
// IsOldFormat 检测是否为旧的扁平结构
func (lcm *LevelCompactionManager) IsOldFormat() bool {
    dataDir := filepath.Join(lcm.shardDir, "data")

    // 检查是否存在 L0 目录
    l0Dir := filepath.Join(dataDir, "L0")
    if _, err := os.Stat(l0Dir); os.IsNotExist(err) {
        return true // 没有 L0 目录，是旧格式
    }

    // 检查是否存在 Manifest
    manifestPath := filepath.Join(dataDir, "_manifest.json")
    if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
        return true // 没有 Manifest，是旧格式
    }

    return false
}
```

### 7.2 格式迁移

```go
// MigrateFromOldFormat 从旧格式迁移
func (lcm *LevelCompactionManager) MigrateFromOldFormat() error {
    dataDir := filepath.Join(lcm.shardDir, "data")

    // 1. 创建 L0 目录
    l0Dir := filepath.Join(dataDir, "L0")
    if err := os.MkdirAll(l0Dir, 0700); err != nil {
        return err
    }

    // 2. 移动旧 SSTable 到 L0
    entries, err := os.ReadDir(dataDir)
    if err != nil {
        return err
    }

    for _, entry := range entries {
        if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "sst_") {
            continue
        }

        oldPath := filepath.Join(dataDir, entry.Name())
        newPath := filepath.Join(l0Dir, entry.Name())

        if err := os.Rename(oldPath, newPath); err != nil {
            slog.Warn("failed to migrate SSTable", "from", oldPath, "to", newPath, "error", err)
        }
    }

    // 3. 创建 Manifest
    return lcm.manifest.Init()
}
```

---

## 8. 配置项

### 8.1 LevelCompactionConfig

```go
// LevelCompactionConfig Level Compaction 配置
type LevelCompactionConfig struct {
    // 是否启用
    Enabled bool

    // 层次配置（覆盖默认）
    LevelConfigs []LevelConfig

    // L0 到 L1 的合并大小阈值
    L0ToL1SizeThreshold int64

    // 单次最大合并文件数
    MaxCompactionParts int

    // Tombstone 保留时间
    TombstoneRetention time.Duration

    // Compaction 检查间隔
    CheckInterval time.Duration

    // Compaction 超时时间
    Timeout time.Duration

    // 是否启用 Checkpoint
    EnableCheckpoint bool
}

// DefaultLevelCompactionConfig 返回默认配置
func DefaultLevelCompactionConfig() *LevelCompactionConfig {
    return &LevelCompactionConfig{
        Enabled:               true,
        LevelConfigs:          DefaultLevelConfigs(),
        L0ToL1SizeThreshold:  5 * 1024 * 1024, // 5MB
        MaxCompactionParts:    10,
        TombstoneRetention:    1 * time.Hour,
        CheckInterval:         5 * time.Minute,
        Timeout:               30 * time.Minute,
        EnableCheckpoint:      true,
    }
}
```

---

## 9. 测试计划

### 9.1 单元测试

| 测试用例 | 验证点 |
|----------|--------|
| `TestLevelManifest_AddRemove` | 层级增删 Parts |
| `TestLevelManifest_SaveLoad` | Manifest 持久化 |
| `TestSelectParts_SmallFirst` | 小文件优先选择 |
| `TestHasOverlap` | 时间范围重叠检测 |
| `TestCollectOverlapParts` | 重叠 Parts 收集 |
| `TestCheckpoint_SaveLoad` | Checkpoint 持久化 |
| `TestTombstone_ShouldDelete` | 删除标记判断 |
| `TestMergeIterator_Leveled` | 层次合并迭代 |

### 9.2 集成测试

| 测试用例 | 验证点 |
|----------|--------|
| `TestLevelCompaction_L0ToL1` | L0 合并到 L1 |
| `TestLevelCompaction_L1ToL2` | L1 合并到 L2 |
| `TestLevelCompaction_Cascade` | 级联合并 |
| `TestLevelCompaction_Recovery` | Checkpoint 恢复 |
| `TestLevelCompaction_Migrate` | 旧格式迁移 |
| `TestLevelCompaction_Concurrent` | 并发 compaction |

### 9.3 E2E 测试

| 测试用例 | 验证点 |
|----------|--------|
| `TestE2E_LevelCompaction_DataIntegrity` | 多层合并数据完整性 |
| `TestE2E_LevelCompaction_Tombstone` | 删除标记处理 |
| `TestE2E_LevelCompaction_Restart` | 重启后恢复正确 |

---

## 10. 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| Checkpoint 丢失 | 重复 compaction | 幂等设计，重复合并安全 |
| Manifest 损坏 | 层次信息丢失 | Manifest 写入前先备份 |
| 磁盘空间不足 | compaction 失败 | 预先检查，保留 10% 空间 |
| 级联 compaction | 资源占用高 | 并发限制，分批执行 |
| Tombstone 泄漏 | 删除数据仍可见 | 保留时间窗口验证 |

---

## 11. 实施计划

见执行计划文档：`docs/superpowers/plans/2026-05-07-level-compaction-plan.md`

---

## 12. 审核要点

1. **层次配置合理性**：10x 增长是否适合时序数据特征？
2. **Small Parts 策略**：是否真的能优化 IO？
3. **Checkpoint 频率**：多长时间保存一次？
4. **Tombstone 保留期**：1 小时是否足够？
5. **迁移策略**：旧数据如何平滑迁移？
6. **并发控制**：是否会发生死锁？
