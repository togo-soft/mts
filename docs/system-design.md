# MTS 微时序数据库系统设计文档

- 文档版本：v4.0
- 更新日期：2026-05-16
- 状态：与当前实现同步

---

## 1. 架构概述

MTS（Micro Time-Series）是一个高性能微时序数据库，专为高吞吐写入和快速时间范围查询设计。整体架构采用 **LSM-Tree** 存储引擎模式，结合 **Write-Ahead Log (WAL)** 实现崩溃恢复。

### 1.1 核心架构图

```
┌──────────────────────────────────────────────────────────────────┐
│                       gRPC API Layer                              │
│   microts.v1.MicroTS: Write / WriteBatch / QueryRange(stream)     │
│                  ListDatabases / ListMeasurements / Health         │
├──────────────────────────────────────────────────────────────────┤
│                        Engine Layer                               │
│                                                                    │
│  Write Path:                                                       │
│  db.Write(p) → Engine.Write() → getOrCreateWriter()               │
│      → MeasurementWriter.Write() → WAL.Write() + MemTable.Write() │
│                                                                    │
│  Query Path:                                                       │
│  db.Iterator(req) → Engine.Iterator()                             │
│      → MemTable.Iterator() (active+passive 归并)                   │
│      → ShardManager.GetShards() → ShardIterator                   │
│          → SSTable.MergeIterator (多文件堆归并)                     │
│      → query.Iterator (多 Shard 堆归并 + tag 过滤 + 字段投影)       │
│                                                                    │
│  Flush Path:                                                       │
│  FlushCoordinator.checkAndFlush() [每 1s 检查]                     │
│      → IdleExceeded 或 NearFull+冷却期 → flushWriterLocked()      │
│      → MemTable.Swap() (active↔passive)                           │
│      → ShardManager.Flush() → groupByShard()                      │
│      → Shard.WriteSSTable() [goroutine+5s超时]                    │
│      → RegisterSSTable() → TriggerCompaction()                    │
│                                                                    │
├──────────────────────────────────────────────────────────────────┤
│                      Storage Layer                                 │
│                                                                    │
│  ┌───────────┐  ┌──────────────┐  ┌───────────────────────────┐ │
│  │ MemTable  │  │ ShardManager │  │ SSTable                    │ │
│  │active/    │  │              │  │  Level Compaction:         │ │
│  │passive    │  │ - GetShard() │  │  L0(10MB/10parts)         │ │
│  │ 双缓冲     │  │ - Flush()    │  │  → L1(100MB) → L2(1GB)   │ │
│  │           │  │ - Compact()  │  │  → L3(10GB) → ...         │ │
│  └───────────┘  └──────────────┘  └───────────────────────────┘ │
│         │                │                    ↑                    │
│         ↓                ↓                    │                    │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │                   WAL (预写日志)                               │ │
│  │  Segment 滚动 (64MB/segment, 最多5个)                         │ │
│  │  LZ4 压缩 + CRC32 校验                                       │ │
│  │  周期 fsync (默认 1s)                                         │ │
│  │  Checkpoint 跳过已持久化段                                    │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
├──────────────────────────────────────────────────────────────────┤
│                   Metadata Layer (bbolt)                           │
│                                                                    │
│  ┌────────────┐  ┌──────────────┐  ┌──────────────────────┐     │
│  │  Catalog   │  │ SeriesStore  │  │     ShardIndex       │     │
│  │DB/Meas管理 │  │SID分配+标签  │  │  Shard时间范围索引    │     │
│  │Schema/     │  │哈希缓存(256  │  │  bbolt JSON存储      │     │
│  │Retention   │  │分片FIFO)     │  │                      │     │
│  └────────────┘  └──────────────┘  └──────────────────────┘     │
└──────────────────────────────────────────────────────────────────┘
```

### 1.2 目录结构

```
mts/
├── db.go                      # 公开 API (Open/Close/Write/Query)
├── internal/
│   ├── api/                   # gRPC API 实现
│   │   ├── grpc.go            # MicroTSService (委托给 Engine)
│   │   └── auth/              # API Key 认证拦截器
│   ├── engine/                # 核心引擎
│   │   ├── interfaces.go      # 五大核心接口定义
│   │   ├── engine.go          # Engine + Config + New/Close
│   │   ├── engine_write.go    # Write / WriteBatch + getOrCreateWriter
│   │   ├── engine_query.go    # Iterator (查询入口)
│   │   ├── engine_catalog.go  # Catalog 委托方法
│   │   └── flush_coordinator.go # 刷盘编排器
│   ├── query/                 # 查询执行器
│   │   └── iterator.go        # 多Shard堆归并+过滤+投影
│   ├── storage/
│   │   ├── metadata/          # bbolt 元数据
│   │   │   ├── manager.go     # Manager (Load/Sync/Close)
│   │   │   ├── catalog.go     # Schema 类型定义
│   │   │   ├── catalog_impl.go
│   │   │   ├── series_impl.go # SeriesStore + MeasSeriesStore
│   │   │   ├── series_simple.go # 纯内存 SimpleSeriesStore (测试)
│   │   │   ├── hash_cache.go  # 256分片FIFO有界哈希缓存
│   │   │   ├── schema_simple.go
│   │   │   └── shard_index_impl.go
│   │   ├── memtable/          # MemTable 双缓冲
│   │   │   └── memtable.go
│   │   ├── wal/               # WAL 预写日志
│   │   │   ├── wal.go         # WAL 主结构
│   │   │   ├── segment.go     # Segment 文件管理
│   │   │   ├── format.go      # 记录编码格式
│   │   │   ├── compress.go    # LZ4 压缩
│   │   │   ├── checkpoint.go  # Checkpoint 跳过已持久化段
│   │   │   ├── reader.go      # 流式记录读取
│   │   │   ├── buffer_pool.go # 三级缓冲区池
│   │   │   └── cleanup.go
│   │   ├── shard/             # Shard 管理
│   │   │   ├── shard.go       # Shard + WriteSSTable + 超时保护
│   │   │   ├── shard_io.go    # SSTable 文件列表扫描
│   │   │   ├── shard_sstable_ref.go # SSTable 引用计数
│   │   │   ├── shard_lifecycle.go   # Close 生命周期
│   │   │   ├── manager.go     # ShardManager (Flusher 实现)
│   │   │   ├── iterator.go    # ShardIterator (MemTable+SSTable归并)
│   │   │   ├── retention.go   # RetentionService 数据过期清理
│   │   │   └── sstable/       # SSTable 实现
│   │   │       ├── writer.go       # 多字段临时文件+Close合并
│   │   │       ├── writer_close.go # Close 合并逻辑
│   │   │       ├── writer_field.go
│   │   │       ├── reader.go       # 按块惰性解码
│   │   │       ├── reader_blocks.go
│   │   │       ├── reader_range.go # 时间范围二分查找
│   │   │       ├── merge_iterator.go # SSTable多文件堆归并
│   │   │       ├── format.go       # 文件格式定义
│   │   │       ├── encoding.go     # 多种编码算法
│   │   │       └── compress.go     # Snappy/LZ4 + CRC32C
│   │   ├── compaction/        # Compaction 策略
│   │   │   ├── compaction.go  # 平坦Compaction Manager
│   │   │   ├── level.go       # Level Compaction Manager
│   │   │   ├── level_manifest.go # Level Manifest JSON持久化
│   │   │   ├── merge.go       # K路归并 + DedupFilter
│   │   │   ├── dedup.go       # Bloom+滑动窗口去重
│   │   │   ├── tombstone.go   # 墓碑标记集合
│   │   │   └── shard_access.go # ShardAccess 接口
│   │   └── util.go            # SafeMkdirAll/SafeCreate/SafeWriteFile
│   ├── metrics/               # expvar 指标收集
│   └── cmd/server/main.go     # 服务入口
└── types/                     # 公共类型定义
    ├── proto.go               # Point/PointRow/FieldValue 等
    ├── internal.go            # MemPoint/InternalPoint 紧凑类型
    └── mts_grpc.pb.go         # gRPC 生成代码
```

---

## 2. 核心接口定义

### 2.1 五大核心接口 (`internal/engine/interfaces.go`)

```go
// Writer — 管理单个 measurement 的 WAL + MemTable 写入
type Writer interface {
    Write(point *types.Point) error
    WriteBatch(points []*types.Point) (int, error)
    MemTable() *memtable.MemTable
    Close() error
}

// Flusher — SSTable 刷盘和 Compaction
type Flusher interface {
    Flush(db, measurement string, points []types.MemPoint) error
    Compact(db, measurement string, startTime int64) error
    GetShards(db, measurement string, startTime, endTime int64) []*shard.Shard
    CloseAll() error
    SetConfig(config *compaction.Config)
}

// Catalog — Database/Measurement 生命周期 + Schema/Retention 管理
type Catalog interface {
    CreateDatabase(name string) error
    DropDatabase(name string) error
    ListDatabases() []string
    DatabaseExists(name string) bool
    CreateMeasurement(database, name string) error
    DropMeasurement(database, name string) error
    ListMeasurements(database string) ([]string, error)
    MeasurementExists(database, name string) bool
    GetSchema(database, measurement string) (*metadata.Schema, error)
    SetSchema(database, measurement string, s *metadata.Schema) error
    GetRetention(database, measurement string) (time.Duration, error)
    SetRetention(database, measurement string, d time.Duration) error
}

// SeriesStore — Series ID 分配和标签索引
type SeriesStore interface {
    AllocateSID(database, measurement string, tags map[string]string) (uint64, error)
    GetTags(database, measurement string, sid uint64) (map[string]string, bool)
    GetSIDsByTag(database, measurement string, tagKey, tagValue string) []uint64
    SeriesCount(database, measurement string) int
}

// ShardIndex — Shard 时间范围注册/查询
type ShardIndex interface {
    RegisterShard(database, measurement string, info ShardInfo) error
    UnregisterShard(database, measurement string, shardID string) error
    QueryShards(database, measurement string, startTime, endTime int64) []ShardInfo
    ListShards(database, measurement string) []ShardInfo
    UpdateShardStats(database, measurement, shardID string, sstableCount int, totalSize int64) error
}
```

### 2.2 接口实现映射

| 接口 | 实现类 | 存储后端 | 说明 |
|------|--------|----------|------|
| Writer | `writer.MeasurementWriter` | WAL文件 + 内存MemTable | 每measurement一个实例 |
| Flusher | `shard.ShardManager` | 磁盘SSTable文件 | 管理所有Shard生命周期 |
| Catalog | `metadata.catalogStore` | bbolt `_catalog/` bucket | database/meas两层bucket |
| SeriesStore | `metadata.seriesStore` | bbolt `_series/` bucket | 256分片FIFO hash→SID缓存 |
| ShardIndex | `metadata.shardIndex` | bbolt `_shards/` bucket | ShardInfo JSON序列化 |

### 2.3 Engine 结构体

```go
type Engine struct {
    cfg          *Config
    dataDir      string
    catalog      Catalog
    seriesStore  SeriesStore
    shardIndex   ShardIndex
    flusher      Flusher
    coordinator  *FlushCoordinator
    memTableCfg  *types.MemTableConfig
    metaManager  *metadata.Manager
    shardMgr     *shard.ShardManager
    retentionSvc *shard.RetentionService
    mu           sync.RWMutex    // 保护 writer 创建 (双检查锁)
    queryWg      sync.WaitGroup  // 进行中查询计数
    closed       bool
    shutdownMu   sync.Mutex      // 关闭序列化
}
```

---

## 3. 数据写入路径

### 3.1 完整写入流程

```
db.Write(point) / db.WriteBatch(points)
    │
    ▼
Engine.Write(ctx, point)
    │
    ├─ 1. ctx 取消检查
    ├─ 2. closed 检查
    ├─ 3. 参数校验 (nil / 空db / 空meas / 负timestamp)
    │
    ├─ 4. 自动创建 Database (如不存在)
    │      └─ catalog.CreateDatabase(name) → bbolt Update
    │
    ├─ 5. 自动创建 Measurement (如不存在)
    │      └─ catalog.CreateMeasurement(db, name) → bbolt Update
    │
    ├─ 6. 获取或创建 Writer (双检查锁)
    │      └─ getOrCreateWriter(db, meas)
    │           ├─ 读检查: coordinator.GetWriter()
    │           ├─ 写锁: e.mu.Lock()
    │           ├─ 再检查: coordinator.GetWriter()
    │           ├─ 创建: writer.New(Config{
    │           │       DB, Measurement, Dir, SeriesStore, MemTableCfg})
    │           │    ├─ WAL 初始化: goroutine + 10s 超时
    │           │    │   └─ 超时或失败 → mw.wal = nil (降级为非持久模式)
    │           │    └─ 创建 MemTable
    │           └─ 注册: coordinator.RegisterWriter()
    │
    └─ 7. Writer.Write(point)
            │
            ├─ 背压检查: ActiveFull() → 等待/超时 (30s)
            │
            ├─ mw.mu.Lock()
            ├─ AllocateSID(db, meas, tags)
            │   ├─ 1) hashSidCache.Load()       ← 内存快速路径
            │   ├─ 2) lookupSIDReadOnly()         ← bbolt View (只读)
            │   └─ 3) allocateSIDWriteTx()        ← bbolt Update (写事务)
            │       ├─ 二次检查 hash_idx (防止重复)
            │       ├─ _next_sid 自增
            │       ├─ 写入 series bucket (sidKey → tagsJSON)
            │       ├─ 写入 hash_idx (hashKey → sidKey)
            │       └─ 写入 tag_index (tagK\x00tagV → sidKey)
            │
            ├─ serializePointDirect(ts, sid, fields)
            │   └─ 单次map遍历 → MemPoint + WAL字节 (池化缓冲区)
            │
            ├─ mw.mu.Unlock()
            ├─ closed 再检查
            ├─ mw.mu.Lock()
            │
            ├─ WAL.Write(walData)     ← LZ4压缩 + 缓冲写入
            ├─ MemTable.Write(mp)     ← 追加到 active 切片
            │
            └─ mw.mu.Unlock() → 返回
```

### 3.2 WriteBatch 批量优化

WriteBatch 在 Engine 层按 Writer 分组，对每组调用 `Writer.WriteBatch()`：

- **单次锁获取**: 所有点的 SID 分配在一次 `mw.mu.Lock()` 内完成
- **单次 WAL 写入**: `wal.WriteBatch(data)` 一次 fsync
- **部分失败安全**: 返回成功写入的数量，不回滚已写入点

### 3.3 背压机制 (Backpressure)

MemTable 有三级容量阈值，逐级触发不同的行为：

| 级别 | 方法 | 阈值 | 行为 |
|------|------|------|------|
| 软限制 | `ShouldSwap()` | `estimatedSize ≥ maxSize` 或 `activeCount ≥ maxCount` 或 `idle 超时` | 触发 Swap+Flush |
| 预警 | `NearFull()` | `estimatedSize ≥ maxSize×2` 或 `activeCount ≥ maxCount×2` | FlushCoordinator 主动刷盘 (3s冷却期) |
| 硬限制 | `ActiveFull()` | `estimatedSize ≥ maxSize×5` 或 `activeCount ≥ maxCount×5` | 写入阻塞等待，30s 超时 |

```go
// Writer 背压循环 (writer.go)
for mw.memTable.ActiveFull() {
    if time.Now().After(deadline) {  // 30s 超时
        return errBackpressureTimeout
    }
    time.Sleep(time.Millisecond)
}
```

### 3.4 MemPoint 紧凑序列化

写入路径使用 `MemPoint` 类型，FieldData 为紧凑 `[]byte`：

```go
type MemPoint struct {
    Timestamp int64
    Sid       uint64
    FieldData []byte  // 紧凑二进制: FieldCount(2B) + {KeyLen(2B)+Key+Type(1B)+Value}...
}
```

- **单次分配**: `serializePointDirect()` 合并字段序列化 + WAL 序列化，单次 map 遍历
- **池化缓冲**: 使用 `sync.Pool` 管理序列化缓冲区
- **惰性解码**: 查询/刷盘时通过 `MemPointToInternal()` 反序列化为 `InternalPoint`

---

## 4. 数据刷盘路径 (Flush)

### 4.1 FlushCoordinator 设计

```go
type FlushCoordinator struct {
    writers            map[string]Writer      // key: "db/meas"
    lastFlushAt        map[string]time.Time   // 冷却期记录
    lastMu             sync.Mutex
    flusher            Flusher
    mu                 sync.RWMutex
    closed             bool
    stopCh             chan struct{}
    stopOnce           sync.Once
    flushAllInProgress atomic.Bool            // 防止 checkAndFlush 与 FlushAll 冲突
}
```

**自动刷盘**: `StartPeriodicCheck(1s)` 启动后台 goroutine，每秒检查所有 Writer：

```
checkAndFlush()
    │
    ├─ flushAllInProgress? → 跳过
    ├─ closed? → 跳过
    │
    └─ 遍历所有 Writer:
         ├─ IsFlushing()? → 跳过 (已有进行中刷盘)
         ├─ IdleExceeded()? → 立即刷盘 (无冷却期)
         └─ NearFull() && 距上次刷盘 ≥ 3s? → 触发刷盘
```

### 4.2 Flush 详细流程

```
flushWriterLocked(db, meas, key, w)
    │
    ├─ 自旋等待 IsFlushing == false (超时 30s)
    │
    ├─ MemTable.Swap()
    │   ├─ 安全兜底: passive 非空则合并回 active
    │   ├─ passive = active, active = new (cap=len(passive))
    │   ├─ activeCount = 0
    │   └─ flushing = true
    │
    ├─ len(passive) == 0? → ClearPassive() → 返回
    │
    ├─ Flusher.Flush(db, meas, passive)
    │   │
    │   ▼
    │   ShardManager.Flush()
    │       │
    │       ├─ groupByShard() — 按时间窗口分组
    │       │   └─ shardDuration = 1h, 同小时数据 → 同一Shard
    │       │
    │       └─ 对每个 Shard:
    │             ├─ Shard.WriteSSTable(points)
    │             │   └─ goroutine + 5s context 超时
    │             │       ├─ NewWriter → WriteMemPoints → Close → Rename
    │             │       └─ 超时 → 返回错误 (上层 MergePassiveBack 恢复)
    │             │
    │             ├─ RegisterSSTable(seq, minTime, maxTime, size)
    │             │   └─ Level Compaction 启用:
    │             │       移动 data/sst_N.bin → data/L0/sst_N.bin
    │             │       调用 LevelManager.AddPart(0, PartInfo{...})
    │             │
    │             └─ TriggerCompaction()  ← 后台 CAS 锁, 非阻塞
    │
    ├─ 成功: MemTable.ClearPassive() → flushing=false → 记录 lastFlushAt
    ├─ 失败: MemTable.MergePassiveBack() → 数据回并到 active
    │
    └─ 更新指标 (FlushTotal, FlushPoints)
```

### 4.3 ShardManager 分组逻辑

```go
func (m *ShardManager) groupByShard(db, measurement string, points []types.MemPoint) []flushGroup {
    // 按 Shard 时间窗口分组
    // calcShardStart(ts) = ts - (ts % shardDuration)
    // shardDuration 默认 1h
}
```

---

## 5. MemTable 双缓冲设计

### 5.1 核心结构

```go
type MemTable struct {
    mu          sync.RWMutex
    active      []types.MemPoint   // 接收新写入 (写入路径持写锁)
    passive     []types.MemPoint   // 等待 Flush (Swap 后由 FlushCoordinator 消费)
    flushing    atomic.Bool        // 是否有后台 flush 进行中
    maxSize     int64              // 默认 64MB
    maxCount    int                // 默认 50000
    idleTimeout time.Duration      // 默认 1 分钟
    lastWrite   time.Time
    activeCount int
    sorted      bool               // active 是否已按时间戳排序
}
```

### 5.2 默认配置

```go
func DefaultMemTableConfig() *MemTableConfig {
    return &MemTableConfig{
        MaxSize:           64 * 1024 * 1024,  // 64MB
        MaxCount:          50000,              // 5万条
        IdleDurationNanos: int64(time.Minute), // 1分钟
    }
}
```

### 5.3 Swap 机制

```
Swap() 调用时:
  active ──────────────────────────────► passive
  (接收新写入)                            (等待刷盘)
  
  new([]MemPoint, 0, len(oldActive)) ──► active
  (新的空缓冲区)                           (恢复接收写入)
  
  flushing = true
```

```go
func (m *MemTable) Swap() []types.MemPoint {
    m.mu.Lock()
    defer m.mu.Unlock()

    // 安全兜底: passive 还有数据则合并回 active (防止上次刷盘失败残留)
    if len(m.passive) > 0 {
        m.active = append(m.passive, m.active...)
        m.passive = nil
    }

    m.passive = m.active
    m.active = make([]MemPoint, 0, len(m.passive))
    m.activeCount = 0
    m.flushing.Store(true)
    return m.passive
}
```

### 5.4 写入单调递增优化

```go
func (m *MemTable) Write(mp types.MemPoint) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    // 快速路径: 时间戳单调递增 → 维持 sorted = true (O(1) 追加)
    if !m.sorted || (len(m.active) > 0 && mp.Timestamp < m.active[len(m.active)-1].Timestamp) {
        m.sorted = false  // 乱序写入 → 需要后续 Sort()
    }
    m.active = append(m.active, mp)
    m.activeCount++
    m.lastWrite = time.Now()
    return nil
}
```

### 5.5 容量检查三级体系

| 方法 | 检测条件 | 触发行为 | 调用者 |
|------|----------|----------|--------|
| `ShouldSwap()` | `size≥maxSize` 或 `count≥maxCount` 或 `idle超时` | 返回 true，由上层决定是否 Swap | (内部) |
| `NearFull()` | `size≥maxSize×2` 或 `count≥maxCount×2` | FlushCoordinator 提前刷盘 | `checkAndFlush()` |
| `ActiveFull()` | `size≥maxSize×5` 或 `count≥maxCount×5` | 写入背压等待 | `Write()` / `WriteBatch()` |

---

## 6. 查询路径

### 6.1 查询架构

```
Engine.Iterator(req)
    │
    ├─ 获取 MemTable (未刷盘数据)
    │   └─ coordinator.GetWriter(db, meas) → writer.MemTable()
    │
    ├─ 获取 Shards (已刷盘数据)
    │   └─ flusher.GetShards(db, meas, startTime, endTime)
    │       ├─ discoverShardsLocked() — 扫描磁盘 data/ 目录
    │       └─ 过滤时间范围匹配的 Shard
    │
    └─ query.NewIteratorWithMemTable(ctx, shards, writerMT, seriesStore, req)
            │
            ├─ ShardIterator (每个 Shard)
            │   ├─ MemTable.Iterator() → active+passive 二路归并
            │   └─ SSTable.MergeIterator → 多文件堆归并
            │
            ├─ query.Iterator (多 Shard 堆归并)
            │   ├─ shardHeap: 按 Current().Timestamp 最小堆
            │   ├─ Offset 跳过
            │   ├─ Tag 过滤 (精确匹配)
            │   └─ 字段投影 (仅返回请求字段)
            │
            └─ 流式输出 (gRPC server-side streaming)
```

### 6.2 迭代器层次

```
┌─────────────────────────────────────────┐
│          query.Iterator                  │
│  多 Shard 堆归并 + Tag过滤 + 字段投影     │
│  + Offset/Limit 分页                     │
├─────────────────────────────────────────┤
│        ShardIterator (×N)                │
│  单 Shard 内 MemTable + SSTable 二路归并  │
├──────────────┬──────────────────────────┤
│ MemTableIter │  SSTable.MergeIterator   │
│ active+      │  多文件堆归并              │
│ passive归并   │  + 块级索引二分查找        │
└──────────────┴──────────────────────────┘
```

### 6.3 查询优化

- **块级时间索引**: SSTable BlockIndex 支持 `O(log N)` 二分定位目标块
- **惰性字段解码**: SSTable Reader 仅在 `Point()` 调用时解码当前行字段值
- **字段投影**: Iterator 仅解码请求的字段，跳过不需要的字段数据
- **Offset/Limit**: 流式跳过 + 提前终止，避免全量加载
- **SSTable 引用计数**: 查询期间持有 SSTable ref，防止被 Compaction 删除

---

## 7. SSTable 文件格式

### 7.1 单文件格式 (v3)

```
sst_{seq}.bin
├── Header (64 bytes)
│   ├── Magic: "TSERSTBL" (8 bytes)
│   ├── Version: 3 (4 bytes)
│   ├── RowCount (8 bytes)
│   ├── FieldCount (4 bytes)
│   ├── BlockCount (4 bytes)
│   ├── BlockSize (4 bytes, 默认 64KB)
│   ├── TimestampsOffset (8 bytes)
│   ├── SIDsOffset (8 bytes)
│   ├── BlockIndexOffset (8 bytes)
│   └── SectionTableOffset (8 bytes)
├── Timestamps Section  (Delta-Varint 编码)
├── SIDs Section         (Varint 编码)
├── Block Sections        (按字段独立存储)
│   └── Per-Section:
│       ├── BlockSectionMap (每个块的字节偏移)
│       └── Compressed Blocks (Snappy/LZ4 + CRC32C)
│           └── [uncompressedLen:4B][compressed][CRC32C:4B]
├── Block Index
│   └── N × BlockIndexEntry {FirstTimestamp, LastTimestamp, Offset(累计行号), RowCount}
└── Section Table
    └── N × SectionEntry {Type, Name, Offset, Size, Encoding, Compression}
```

### 7.2 编码算法

| 数据类型 | 编码算法 | 说明 |
|----------|----------|------|
| Timestamp | Delta-Varint | 存储相邻时间戳差值 |
| SID | Varint | 变长整数编码 |
| int64 | ZigZag-Varint | 有符号整数变长编码 |
| float64 | XOR-Float | 相邻浮点数 XOR 差异编码 |
| string | Dict-String | 字典编码 |
| bool | Bitmap-Bool | 位图编码 |

### 7.3 Writer 写入机制

SSTable Writer 采用**多临时文件 + Close 合并**策略：

1. **写入阶段**: 每个字段独立写入临时文件 (`data/.sst_{seq}_tmp/`)
2. **缓冲**: 数据先写入内存缓冲，达到 BlockSize (64KB) 后刷入临时文件
3. **Close 合并**: 编码 + 压缩所有字段块 → 追加 BlockIndex + SectionTable → 合并为单文件
4. **原子 Rename**: 临时目录 → 最终 `sst_{seq}.bin`

### 7.4 Reader 流式读取

- **按需加载**: 通过 BlockSectionMap 定位目标块，仅解压需要的块
- **惰性解码**: `loadBlock(blockIdx)` 解码时间戳和 SID，字段值到 `Point()` 时才解码
- **二分查找**: `FindBlock(startTime)` 基于 BlockIndex 做时间范围二分查找
- **ReadRange**: `ReadRange(startTime, endTime, maxRows)` 仅解码匹配块

---

## 8. Compaction 策略

### 8.1 两级 Compaction 架构

| 类型 | 实现 | 适用场景 |
|------|------|----------|
| 平坦 Compaction | `compaction.Manager` | 基础合并，K路归并去重 |
| Level Compaction | `compaction.LevelManager` | 层次化合并，指数容量增长 |

Level Compaction 启用后，Shard 优先使用 LevelManager。

### 8.2 Level 层次配置

```
L0:  ≤ 10 个 Parts，每个 ≤ 10MB    (总 ≤ 100MB)
L1:  ≤ 100MB                         (总 ≤ 100MB)
L2:  ≤ 1GB                           (总 ≤ 1GB)
L3:  ≤ 10GB                          (总 ≤ 10GB)
L4+: 指数增长 (10x)
```

### 8.3 Level Compaction 触发条件

```go
func (lm *LevelManager) ShouldCompactLevel(level int) bool {
    if level == 0 {
        // L0: Parts 数超限 或 总大小超阈值 (5MB)
        return partsCount >= MaxParts || totalSize >= L0ToL1SizeThreshold
    }
    // L1+: 总大小超过该层容量上限
    return totalSize >= LevelMaxSize(level)
}
```

### 8.4 Compaction 流程 (Level)

```
TriggerCompaction() [后台 goroutine]
    │
    ├─ CAS compactInProgress (0→1) — 失败则跳过(已有进行中)
    │
    ├─ 选择源Level: ShouldCompactLevel(level)
    │
    ├─ SelectPartsForMerge(level)
    │   └─ 按大小排序, 累积到 LevelMaxSize(level+1)/2
    │
    ├─ CollectOverlapParts(level, targets)
    │   └─ 收集当前层+下一层与目标时间范围重叠的 Parts
    │
    ├─ 构建 Checkpoint (可选, 崩溃恢复)
    │
    ├─ Merge 合并:
    │   ├─ 打开所有源 SSTable Reader
    │   ├─ 收集 Tombstone 墓碑
    │   ├─ 创建 MergeIterator (K路堆归并)
    │   ├─ DedupFilter 去重 (Bloom + 滑动窗口)
    │   ├─ Tombstone 过滤
    │   └─ 批量写入新 SSTable
    │
    ├─ Commit:
    │   ├─ 从 Manifest 移除源 Parts
    │   ├─ 在目标层添加新 Part
    │   ├─ 保存 Manifest JSON
    │   └─ 删除不再使用的源文件 (IsSSTUnused 检查)
    │
    └─ 清除 Checkpoint
```

### 8.5 Dedup 去重过滤器

```go
type DedupFilter struct {
    bloom     *BloomFilter    // 10M bits, 4 哈希函数
    window    []uint64        // 滑动窗口 FIFO (50000 条目)
    strict    map[uint64]bool // 严格模式 (行数 < 50000)
    windowPos int
}
```

- **小数据集 (<50000行)**: 纯 map 精确去重
- **大数据集**: Bloom Filter (概率去重) + 滑动窗口 (窗口内精确去重)
- **内存固定**: 大数据集模式下约 3.7MB (vs 纯 map 的 O(N) 增长)

### 8.6 平坦 Compaction (旧式，兼容)

```go
type Manager struct {
    ShardAccess      ShardAccess
    Config           *Config            // MaxSstableCount: 4, ShardSizeLimit: 1GB
    compactInProgress atomic.Int32      // CAS 互斥锁
    CurrentTask      *Progress
}

type Config struct {
    MaxSstableCount    int32  // 默认 4
    MaxCompactionBatch int32  // 0 = 自动, 使用 TwoPhaseThreshold=10
    ShardSizeLimit     int64  // 默认 1GB (超出则停止 compaction)
    CheckIntervalNanos int64  // 定时检查间隔
    TimeoutNanos       int64  // compaction 超时
}
```

### 8.7 Level Manifest

```go
type LevelManifest struct {
    levels       map[int]*Level      // level → Level{parts: []PartInfo}
    levelConfigs []LevelSpec         // 每层容量配置
    nextSeq      uint64              // 全局 SSTable 序列号
    manifestPath string              // data/_manifest.json
}
```

持久化为 `data/_manifest.json`，支持原子保存 (临时文件 + Rename)。

---

## 9. WAL 设计

### 9.1 WAL 结构

```go
type WAL struct {
    dir        string
    gen        uint64         // 世代号 (Unix 时间戳)
    segNum     uint64         // 当前 segment 序号
    seg        *segment
    mu         sync.Mutex
    buf        []byte         // 1MB 写缓冲
    bufPos     int
    cfg        Config
    closed     atomic.Bool
    syncDone   chan struct{}  // 停止周期同步信号
    compressed bool           // LZ4 压缩开关
}
```

### 9.2 WAL 配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| SegmentSize | 64MB | 单 segment 文件大小上限 |
| MaxSegments | 5 | 最大 segment 数 (0=无限制) |
| SyncMode | SyncPeriodic | 同步模式 |
| SyncInterval | 1s | 周期 fsync 间隔 |
| Compressed | true | 启用 LZ4 压缩 |

### 9.3 文件命名与格式

```
{dataDir}/wal/
├── 00000000682f3b00_00000001.wal   ← gen=1747382016 (Unix timestamp), num=1
├── 00000000682f3b00_00000002.wal   ← gen=1747382016, num=2
└── wal_checkpoint                  ← {generation, segment} JSON
```

**Segment 文件内部格式**:

```
┌──────────────────────────────────────┐
│ Segment Header (14 bytes)             │
│  Magic(4B) + Version(2B) + Flags(2B) │
│  + SegmentNum(4B) + Reserved(2B)     │
├──────────────────────────────────────┤
│ Record 1: CRC32(4B) + Type(1B)       │
│           + Length(4B) + Payload(N)  │
│           + Padding(0-7B) [8字节对齐] │
├──────────────────────────────────────┤
│ Record 2: ...                        │
├──────────────────────────────────────┤
│ Record N: ...                        │
└──────────────────────────────────────┘
Magic: 0xD0C0A1FE
Version: 1
Flags: bit0=compressed
```

**Payload 内容** (WAL v2 格式，LZ4 压缩前):

```
[version:1B=2][timestamp:8B BE][sid:8B BE][FieldData:...]
```

### 9.4 WAL 生命周期

```
Open(dir) → Write/WriteBatch → [Rotate] → TruncateAfterFlush → [Write继续] → Close/Purge
                │                              │
                └─ segment 达到 64MB 自动轮转    └─ Flush 成功后调用，删除旧 segment
```

**TruncateAfterFlush 流程**:
1. `flushLocked()` — 刷写缓冲
2. `rotateLocked()` — 关闭当前 segment → 创建新空 segment
3. 删除所有旧 segment (`gen==w.gen && num<w.segNum`)
4. 清除 Checkpoint

### 9.5 缓冲区池

三级池化设计，减少 GC 压力：

| 级别 | 大小 | 用途 |
|------|------|------|
| Small | 256B | 小记录 |
| Medium | 4KB | 中等记录 |
| Large | 64KB | 大记录/批量 |

存储 `*[]byte` 避免接口装箱逃逸。

---

## 10. 崩溃恢复

### 10.1 启动流程

```
Engine.New(cfg)
    │
    ├─ 1. 应用默认 MemTable 配置 (如未设置)
    │
    ├─ 2. metadata.Manager.Load()
    │   ├─ 打开 bbolt: metadata.db
    │   ├─ catalog.rebuildCache()   ← 遍历 _catalog bucket
    │   └─ series.rebuildCache()    ← 遍历 _series bucket
    │       ├─ 填充 cache: SID → Tags
    │       └─ 填充 hashSidCache: hash → SID
    │
    ├─ 3. 创建 ShardManager (传入 catalog, seriesStore, shardIndex)
    │
    ├─ 4. 创建 FlushCoordinator + StartPeriodicCheck(1s)
    │
    ├─ 5. 创建 RetentionService (如 RetentionPeriod > 0)
    │
    └─ 6. go discoverAndRecover()   ← 后台 goroutine
            │
            ├─ catalog.ListDatabases()
            ├─ catalog.ListMeasurements(db)
            │
            └─ 对每个 db/meas:
                  ├─ flusher.GetShards()      ← 扫描磁盘 + 注册到 ShardManager
                  ├─ getOrCreateWriter()      ← 创建 Writer
                  └─ ReplayWAL()              ← 流式回放 WAL
                      │
                      ├─ wal.Replay(fn)
                      │   ├─ 列出 segment 文件
                      │   ├─ 加载 Checkpoint → 跳过已持久化段
                      │   └─ 对每个 segment 流式读取记录 → fn(payload)
                      │
                      └─ 每个记录:
                            ├─ deserializeFromWAL(data) → MemPoint
                            ├─ GetTags() 预热 series 缓存
                            └─ MemTable.Write(mp)
                      
                      MemTable.Sort()  ← WAL replay 后排序
```

### 10.2 Checkpoint 加速恢复

```go
type Checkpoint struct {
    Generation uint64 `json:"generation"`
    Segment    uint64 `json:"segment"`
}
```

- Flush 成功后通过 `ClearCheckpoint()` 创建，标记已持久化到 SSTable 的 WAL 段
- 下次启动 `Replay()` 时跳过 `gen==checkpoint.gen && num<=checkpoint.seg` 的 segment

---

## 11. 元数据管理 (bbolt)

### 11.1 Bucket 结构

```
metadata.db
├── {dbName}/                          # Database bucket
│   └── {measName}/                    # Measurement bucket
│       ├── _schema        (JSON)      # Schema{Version, Fields, TagKeys}
│       └── _retention     (binary)    # time.Duration
│
├── _series/
│   └── {dbName}/{measName}/
│       ├── series/                    # SID → tags JSON
│       │   ├── {sidKey:8B} → {tagsJSON}
│       │   └── _next_sid (uint64)    # 自增 SID 计数器
│       ├── hash_idx/                  # tags hash → SID
│       │   └── {hashKey:8B} → {sidKey:8B}
│       └── tag_index/                 # 反向索引
│           └── {tagK\x00tagV}/        # key=value bucket
│               └── {sidKey:8B} → (空)
│
└── _shards/
    └── {dbName}/{measName}/
        └── {shardID}/                 # ShardInfo JSON
            ├── ShardID
            ├── StartTime / EndTime
            ├── SSTableCount / TotalSize
            └── ...
```

### 11.2 AllocateSID 三级查找

```
AllocateSID(db, meas, tags)
    │
    ├─ 1️⃣ hashSidCache.Load(hash)
    │     └─ 命中 → 直接返回 SID (0次 bbolt 访问)
    │
    ├─ 2️⃣ lookupSIDReadOnly() → db.View()
    │     └─ 命中 → storeHashSid() + 返回 (1次 bbolt 读事务, 无 fsync)
    │
    └─ 3️⃣ allocateSIDWriteTx() → db.Update()
          ├─ 二次检查 hash_idx (防并发重复)
          ├─ _next_sid 自增
          ├─ 写入 series bucket
          ├─ 写入 hash_idx
          ├─ 写入 tag_index
          └─ storeHashSid() + 返回 (1次 bbolt 写事务)
```

### 11.3 hashSidCache 设计

```go
type hashSidCache struct {
    shards  [256]hashShard  // 256 个分片, 减少锁竞争
    maxSize int             // 总容量 100000
}
```

- **分片策略**: `maphash.Hash( key ) % 256`
- **淘汰策略**: FIFO (每个分片独立, 超容量时淘汰最旧条目)
- **键格式**: `"db/meas/{hash_hex_16}"`

### 11.4 Manager 聚合入口

```go
type Manager struct {
    catalog    *catalogStore
    series     *seriesStore
    shardIndex *shardIndex
    db         *bolt.DB
}
```

- `Load()`: 打开 bbolt → 重建 catalog 和 series 缓存
- `Sync()`: bbolt 文件 fsync
- `Close()`: 关闭 bbolt 数据库

---

## 12. SSTable 引用计数

查询进行中的 SSTable 文件受引用计数保护，防止被 Compaction 删除：

```go
type sstRefs struct {
    mu   sync.Mutex
    refs map[string]*SSTableRef  // path → 引用
}

type SSTableRef struct {
    path   string
    refCnt atomic.Int32
}
```

- **查询开始**: `AcquireSSTRef(path)` → refCnt+1
- **查询结束**: `ReleaseSSTRef(path)` → refCnt-1
- **Compaction 删除前**: `IsSSTUnused(path)` → refCnt==0 检查

---

## 13. gRPC API

### 13.1 服务定义

```protobuf
service MicroTS {
    rpc Write(WriteRequest) returns (WriteResponse);
    rpc WriteBatch(WriteBatchRequest) returns (WriteBatchResponse);
    rpc QueryRange(QueryRangeRequest) returns (stream Row);  // 服务端流式
    rpc ListDatabases(ListDatabasesRequest) returns (ListDatabasesResponse);
    rpc CreateDatabase(CreateDatabaseRequest) returns (CreateDatabaseResponse);
    rpc DropDatabase(DropDatabaseRequest) returns (DropDatabaseResponse);
    rpc ListMeasurements(ListMeasurementsRequest) returns (ListMeasurementsResponse);
    rpc CreateMeasurement(CreateMeasurementRequest) returns (CreateMeasurementResponse);
    rpc DropMeasurement(DropMeasurementRequest) returns (DropMeasurementResponse);
    rpc Health(HealthRequest) returns (HealthResponse);
}
```

### 13.2 服务实现

```go
type MicroTSService struct {
    engine *engine.Engine
}
```

所有方法委托给 Engine，错误转换为 gRPC 状态码:
- 参数错误 → `codes.InvalidArgument`
- 内部错误 → `codes.Internal`
- 不存在 → `codes.NotFound`

### 13.3 服务配置

- 监听端口: `:2026`
- 最大并发流: 100
- 最大消息大小: 4MB
- 优雅关闭: SIGINT/SIGTERM → GracefulStop (10s 超时) → Flush → Close

---

## 14. 配置参数

### 14.1 完整配置结构

```go
type Config struct {
    DataDir                string
    ShardDuration          time.Duration   // 默认 1h

    MemTableCfg   *MemTableConfig          // nil = 使用默认 (64MB/50000/1min)
    CompactionCfg *CompactionConfig        // nil = 使用默认

    CompressionAlgorithm   sstable.CompressionAlgorithm  // 默认 LZ4
    RetentionPeriod        time.Duration                 // 数据保留期 (0=不限)
    RetentionCheckInterval time.Duration                 // 清理检查间隔
}

type MemTableConfig struct {
    MaxSize           int64  // 默认 64MB
    MaxCount          int32  // 默认 50000
    IdleDurationNanos int64  // 默认 1min
}

type CompactionConfig struct {
    MaxSstableCount    int32  // 默认 4 (平坦模式触发阈值)
    MaxCompactionBatch int32  // 0 = 自动
    ShardSizeLimit     int64  // 默认 1GB (超过停止 compaction)
    CheckIntervalNanos int64  // 定时检查间隔
    TimeoutNanos       int64  // 单次 compaction 超时
}
```

### 14.2 场景推荐

| 场景 | MaxSize | MaxCount | IdleTimeout | ShardDuration |
|------|---------|----------|-------------|---------------|
| 高频写入 (IoT) | 128MB | 100000 | 5min | 1h |
| 中频写入 (监控) | 64MB | 50000 | 1min | 24h |
| 低频写入 (日志) | 32MB | 10000 | 10min | 7d |

---

## 15. 数据过期清理 (Retention)

```go
type RetentionService struct {
    manager       *ShardManager
    retention     time.Duration      // 数据保留时长
    checkInterval time.Duration      // 检查间隔
    done          chan struct{}
}
```

- 周期性扫描所有 Shard
- 删除 `EndTime < now - retention` 的 Shard (包括目录)
- 调用 `ShardManager.DeleteShard(key)` 完成清理

---

## 16. 安全性

### 16.1 文件权限

| 操作 | 权限 | 函数 |
|------|------|------|
| 创建目录 | 0700 (`rwx------`) | `SafeMkdirAll()` |
| 创建文件 | 0600 (`rw-------`) | `SafeCreate()`, `SafeOpenFile()` |
| 写入文件 | 0600 + 原子 Rename | `SafeWriteFile()` |

### 16.2 路径安全

所有 `Safe*` 函数在操作前检查路径是否包含 `..` 路径遍历组件。

### 16.3 认证

- API Key 认证: `authorization: Bearer <key>` 头部
- 一元和流式 gRPC 拦截器均支持

---

## 17. 可观测性

### 17.1 指标 (expvar)

按子系统分组:

| 子系统 | 指标 |
|--------|------|
| Write | `write_total`, `write_errors`, `write_batch_total` |
| Flush | `flush_total`, `flush_points`, `flush_errors`, `flush_duration_ns` |
| Compaction | `compaction_total`, `compaction_errors`, `compaction_duration_ns`, `compaction_files_in`, `compaction_files_out` |
| Query | `query_total`, `query_duration_ns`, `query_rows_returned`, `query_errors` |
| MemTable | `memtable_size`, `memtable_count` |
| WAL | `wal_write_total`, `wal_write_bytes`, `wal_replay_total`, `wal_errors` |

### 17.2 Gauges

- `memtable`: 每个 measurement 的 active/passive 大小
- `wal`: segment 数、当前 segment 大小
- `shard`: 每个 shard 的 SSTable 文件数

---

## 18. 接口设计决策

### 18.1 为什么分离 Writer 和 Flusher?

- **职责单一**: Writer 只管接收写入 (WAL + MemTable)，Flusher 只管刷盘 (SSTable + Compaction)
- **并发安全**: 写入路径不执行 I/O 密集的刷盘操作
- **资源隔离**: 刷盘失败不影响写入继续 (MergePassiveBack 安全兜底)
- **可测试性**: 可独立 mock 和测试

### 18.2 为什么 MemTable 放在 Writer 而非 Engine?

- 写入路径高频访问 MemTable，放在 Writer 减少 Engine 层锁竞争
- 每个 Measurement 有独立 MemTable，避免跨 measurement 污染
- FlushCoordinator 通过 Writer 接口操作 Swap/ClearPassive，不直接依赖实现

### 18.3 为什么 Shard 是纯 SSTable 容器?

- 消除 "Shard = 写引擎" 的架构缺陷
- 内存增长从 O(Shard数) 降为 O(活跃Measurement数)
- 10M 数据点 (1h ShardDuration) → ~2800 个 Shard，每个仅需 ~500B 元数据

### 18.4 为什么用 bbolt 而不是其他 KV 存储?

- **纯 Go 实现**: 无 CGO 依赖, 交叉编译简单
- **ACID 事务**: 单文件 MVCC + 内置 WAL
- **零配置**: 无需独立服务进程
- **成熟稳定**: etcd 项目的存储后端

---

## 19. 并发模型

| 组件 | 并发机制 | 说明 |
|------|----------|------|
| Engine.writers | `sync.RWMutex` + 双检查 | Writer 创建序列化 |
| MeasurementWriter | `sync.Mutex` | 写入互斥 |
| MemTable | `sync.RWMutex` + `atomic.Bool` | 读写分离 + flushing 标志 |
| FlushCoordinator | `sync.RWMutex` + `atomic.Bool` | writer map 保护 + flushAllInProgress |
| ShardManager | `sync.RWMutex` | shards map 保护 |
| Shard | `sync.RWMutex` + CAS | sstSeq 递增 + compaction 互斥 |
| Compaction | `atomic.Int32` CAS | 单实例执行保证 |
| WAL | `sync.Mutex` | 写缓冲保护 |
| hashSidCache | 256 × `sync.Mutex` | 分片减少竞争 |
| sstRefs | `sync.Mutex` | 引用计数 map |

---

## 20. 相关文档索引

| 文档 | 说明 |
|------|------|
| `docs/design/shard-refactoring-plan.md` | Shard 重构为纯 SSTable 容器的设计 (已实现) |
| `docs/design/level-compaction-design.md` | Level Compaction 详细设计 (已实现) |
| `docs/design/compaction-design.md` | 平坦 Compaction 设计 (已实现) |
| `docs/design/memory-optimization-plan.md` | 内存优化方案 (部分已实现) |
| `docs/design/compaction-dedup-memory-optimization.md` | Dedup Bloom Filter 设计 (已实现) |
| `docs/superpowers/specs/2026-05-13-memtable-double-buffer-design.md` | MemTable 双缓冲设计 (已实现) |
| `docs/superpowers/specs/2026-05-15-engine-interface-refactor-design.md` | Engine 五大接口设计 (已实现) |
| `docs/superpowers/specs/2026-05-08-metadata-bolt-redesign.md` | 元数据 bbolt 重设计 (已实现) |
| `docs/superpowers/specs/2026-05-08-wal-redesign-design.md` | WAL 模块重构设计 (已实现) |
| `docs/superpowers/specs/2026-05-04-sstable-streaming-read-design.md` | SSTable 流式读取设计 (已实现) |
| `docs/superpowers/specs/2026-05-14-grpc-streaming-query-design.md` | gRPC 流式查询设计 (已实现) |
| `docs/design/write-performance-optimization.md` | 写入性能优化方案 (部分已实现) |
| `docs/api.md` | gRPC API 参考文档 |
