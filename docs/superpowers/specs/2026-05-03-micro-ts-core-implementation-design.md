# micro-ts 核心功能实现设计

## 1. 概述

本文档描述 micro-ts 核心写入路径和元数据管理的实现设计。

**目标**：实现 WAL 写入集成、MemTable 自动 Flush、MetaStore SID 分配、Tag 索引构建。

**架构**：采用分层设计，按 Database → Measurement 分层管理元数据。

---

## 2. 架构设计

### 2.1 分层结构

```
Engine
  ├── metaStores: map[database_name] → DatabaseMetaStore
  │       │
  │       └── measurements: map[measurement_name] → MeasurementMetaStore
  │               ├── series: map[sid] → tags
  │               ├── tagIndex: map[tagKey\0tagValue] → []sid
  │               └── NextSID: int64
  │
  └── shardManager: ShardManager
          └── shards: map[shardKey] → Shard
                  ├── db, measurement
                  ├── startTime, endTime
                  ├── wal: WAL
                  ├── memTable: MemTable
                  └── metaStore: *MeasurementMetaStore (引用)
```

### 2.2 写入路径

```
Write Request
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ Shard.Write(point)                                         │
│                                                             │
│ 1. WAL.Write                                               │
│    - 序列化 Point 为二进制                                  │
│    - 写入 WAL buffer                                       │
│    - periodic sync 后台 goroutine (每 3 秒)                 │
│                                                             │
│ 2. MetaStore.AllocateSID                                   │
│    - 计算 tags hash，查找或分配 SID                          │
│    - 更新 tagIndex                                          │
│                                                             │
│ 3. MemTable.Write(point with sid)                          │
│    - 写入跳表                                               │
│                                                             │
│ 4. Check ShouldFlush                                       │
│    - 条数 >= 10000 或 大小 >= 64MB                         │
│    - 触发 flush                                             │
└─────────────────────────────────────────────────────────────┘
```

### 2.3 Flush 路径

```
Shard.Flush()
    │
    ├── flushMu.Lock()
    │
    ├── points = memTable.Flush()
    │
    ├── sstable.Write(points)
    │   └── 失败 → 返回错误，数据保留在内存
    │
    ├── metaStore.Persist()
    │   └── 失败 → log 警告，不阻止
    │
    └── flushMu.Unlock()
```

### 2.4 查询路径（并发）

```
Engine.Query(req)
    │
    ├── shards = GetShards(db, meas, startTime, endTime)
    │
    ├── 并发读取所有 Shard (goroutine)
    │   └── Shard.Read → PointRows
    │
    ├── 收集结果 channel
    │
    ├── 全局排序 (按 Timestamp)
    │
    ├── Tag 过滤 (使用 tagIndex)
    │
    └── 分页 (Offset + Limit)
```

---

## 3. 组件设计

### 3.1 DatabaseMetaStore

```go
// DatabaseMetaStore 数据库级元数据容器
type DatabaseMetaStore struct {
    mu            sync.RWMutex
    measurements   map[string]*MeasurementMetaStore
}

func (d *DatabaseMetaStore) GetMeasurementMetaStore(name string) *MeasurementMetaStore {
    d.mu.RLock()
    defer d.mu.RUnlock()
    return d.measurements[name]
}

func (d *DatabaseMetaStore) CreateMeasurementMetaStore(name string) *MeasurementMetaStore {
    d.mu.Lock()
    defer d.mu.Unlock()
    if m, ok := d.measurements[name]; ok {
        return m
    }
    m := NewMeasurementMetaStore()
    d.measurements[name] = m
    return m
}
```

### 3.2 MeasurementMetaStore

```go
// MeasurementMetaStore Measurement 级元数据
type MeasurementMetaStore struct {
    mu       sync.RWMutex
    series   map[uint64]map[string]string  // sid → tags
    tagIndex map[string][]uint64            // tagKey\0tagValue → sids
    nextSID  uint64
    dirty    bool
}

// AllocateSID 分配或查找 SID
func (m *MeasurementMetaStore) AllocateSID(tags map[string]string) uint64 {
    m.mu.Lock()
    defer m.mu.Unlock()

    // 计算 tags hash 作为查找 key
    key := tagsHash(tags)

    // 查找已存在的 SID
    for sid, t := range m.series {
        if tagsEqual(t, tags) {
            return sid
        }
    }

    // 分配新 SID
    sid := m.nextSID
    m.nextSID++
    m.series[sid] = tags
    m.dirty = true

    // 更新 tagIndex
    for k, v := range tags {
        indexKey := k + "\x00" + v
        m.tagIndex[indexKey] = append(m.tagIndex[indexKey], sid)
    }

    return sid
}

// GetTagsBySID 根据 SID 获取 tags
func (m *MeasurementMetaStore) GetTagsBySID(sid uint64) (map[string]string, bool) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    tags, ok := m.series[sid]
    return tags, ok
}

// GetSidsByTag 根据 tag 条件获取 sids
func (m *MeasurementMetaStore) GetSidsByTag(tagKey, tagValue string) []uint64 {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return m.tagIndex[tagKey+"\x00"+tagValue]
}

// Persist 持久化到磁盘
func (m *MeasurementMetaStore) Persist(path string) error { ... }

// Load 从磁盘加载
func (m *MeasurementMetaStore) Load(path string) error { ... }
```

### 3.3 Shard 变更

```go
type Shard struct {
    db           string
    measurement  string
    startTime    int64
    endTime      int64
    dir          string
    memTable     *MemTable
    wal          *WAL              // 新增
    metaStore    *MeasurementMetaStore  // 引用
    flushMu      sync.Mutex        // 保护 flush
}
```

### 3.4 WAL Periodic Sync

```go
func (w *WAL) StartPeriodicSync(interval time.Duration, done <-chan struct{}) {
    go func() {
        ticker := time.NewTicker(interval)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                w.Sync()
            case <-done:
                return
            }
        }
    }()
}
```

---

## 4. 错误处理

| 场景 | 处理方式 |
|------|----------|
| WAL 写入失败 | 返回错误，阻止写入 |
| SID 分配失败 | 返回错误 |
| MemTable 写入失败 | 返回错误 |
| Flush 失败 | 返回错误，MemTable 数据保留 |
| MetaStore Persist 失败 | log 警告，不阻止写入 |

---

## 5. 文件权限

- 目录: 0700 (rwx------)
- 普通文件: 0600 (rw-------)

---

## 6. 测试策略

| 级别 | 测试内容 |
|------|----------|
| 单元测试 | MeasurementMetaStore.AllocateSID, tagsHash, tagsEqual |
| 单元测试 | Shard.Write → Read 数据一致性 |
| 单元测试 | WAL.Write → ReplayWAL |
| 集成测试 | 写入 → Flush → 重启加载 → 读取 |
| E2E 测试 | 完整性测试（已有），添加重启恢复测试 |

---

## 7. 实现计划

### Phase 1: WAL 集成
- Shard 新增 wal 字段
- Shard.Write 先写 WAL
- WAL periodic sync goroutine

### Phase 2: MetaStore 重构
- DatabaseMetaStore + MeasurementMetaStore 分层
- AllocateSID 实现
- tagIndex 更新

### Phase 3: Flush 集成
- Shard.Write 检查 ShouldFlush
- 失败处理
- MetaStore lazy persist

### Phase 4: 并发查询
- Engine.Query 并发读取 Shards
- 结果合并排序

---

## 8. 验收标准

- [ ] 写入 1M 数据后重启，数据完整恢复
- [ ] Tag 过滤使用 tagIndex，不全表扫描
- [ ] E2E 完整性测试通过
- [ ] WAL 重放测试通过