# Shard 架构重构方案：全局 MemTable + WAL

## 问题回顾

当前 Shard 是"自包含写引擎"：每个 Shard 独立持有 WAL、MemTable、SeriesStore 适配器。
10M 数据点（ShardDuration=1h）产生 ~2,800 个 Shard，每个固定开销 ~100KB，合计 ~280MB+
基础设施开销。更根本的问题是内存增长模型为 **O(时间跨度 / ShardDuration)**，
而非 O(活跃写入量)。

主流 TSDB（InfluxDB、Prometheus、LevelDB）的做法：

```
时间分区（Shard/Block）= 磁盘结构，只管 SSTable 文件
内存缓冲（MemTable/Cache）= 全局共享，接收所有写入
WAL = 全局的，与 MemTable 生命周期绑定
```

## 目标架构

```
┌─────────────────────────────────────────────────┐
│                   Engine                        │
│  Write() ─────────────────────────> Iterator()  │
└───────────────┬───────────────────────┬─────────┘
                │                       │
    ┌───────────▼───────────┐   ┌───────▼─────────┐
    │  MeasurementWriter    │   │  Query Pipeline  │
    │  (per db.measurement) │   │  (multi-source   │
    │                       │   │   merge)         │
    │  ┌─────────────────┐  │   └─────────────────┘
    │  │   MemTable      │  │
    │  │ (active/passive)│  │
    │  └─────────────────┘  │
    │  ┌─────────────────┐  │
    │  │   WAL           │  │
    │  │ (per-meas,      │  │
    │  │  rotated)       │  │
    │  └─────────────────┘  │
    │  ┌─────────────────┐  │
    │  │ SeriesStore     │──│────> bbolt (global)
    │  └─────────────────┘  │
    └───────────┬───────────┘
                │ flush: group by shard key
                │ write SSTable per shard
                ▼
    ┌───────────────────────────────────┐
    │         ShardManager              │
    │  shards map[string]*Shard         │
    │  (disk-only, cheap)               │
    └───────────┬───────────────────────┘
                │
    ┌───────────▼───────────┐
    │    Shard (per time    │
    │    window, DISK ONLY) │
    │                       │
    │  - SSTable files      │
    │  - Compaction Manager │
    │  - NO WAL             │
    │  - NO MemTable        │
    └───────────────────────┘
```

## 核心改动

### 1. 新增：MeasurementWriter（承接写入）

```go
// 一个 measurement 一个 writer，全局唯一
type MeasurementWriter struct {
    db          string
    measurement string

    memTable    *memtable.MemTable   // 唯一的写缓冲
    wal         *wal.WAL            // 唯一的 WAL
    seriesStore SeriesStore         // 复用现有 MeasSeriesStore
    schemaStore SchemaStore         // 复用现有
    schema      *metadata.Schema
    schemaMu    sync.RWMutex

    mu         sync.Mutex
    flushing   atomic.Bool
    flushConds []*flushCondition    // 等待 flush 完成的 goroutine

    dir        string               // data root for this measurement
    shardDur   int64                // ShardDuration in nanoseconds
}
```

**Write 流程：**

1. 背压检查（MemTable.ActiveFull → 等待 flush）
2. 持锁：AllocateSID → ValidateFieldTypes → WAL.Write → MemTable.Write
3. 释放锁：检查 ShouldSwap，触发异步 flush

与现有 Shard.Write 逻辑完全一致，差异仅在于 Writer 层面而非 Shard 层面。

### 2. 精简：Shard（退化为磁盘分区）

```go
type Shard struct {
    db          string
    measurement string
    startTime   int64
    endTime     int64
    dir         string  // 如 data/db_meas_1715702400/

    sstSeq      uint64
    sstRefs     *sstRefs
    mu          sync.RWMutex

    compaction      *compaction.Manager     // 可选
    levelCompaction *compaction.LevelManager // 可选
    compressionAlgo sstable.CompressionAlgorithm

    closed atomic.Bool
    closeOnce sync.Once
}
```

移除的字段：`memTable`、`wal`、`seriesStore`、`schemaStore`、`schema`、`schemaMu`、`flushDone`、`flushTicker`、`flushWg`、`compactionWg`、`replaying`

### 3. 改动：ShardManager（管理两层）

```go
type ShardManager struct {
    // 新：measurement 级别 writer
    writers   map[string]*MeasurementWriter  // key: "db/meas"
    writersMu sync.RWMutex

    // 现有：shard 注册表（不变）
    shards      map[string]*Shard
    shardIndex  ShardIndex
    mu          sync.RWMutex

    dataDir     string
    shardDur    int64
    // ... 其他配置
}
```

### 4. Flush 路径：关键差异

当前：每个 Shard 的 MemTable flush → 1 个 SSTable

新设计：全局 MemTable flush → 按 ShardKey 分组 → N 个 SSTable

```
MeasurementWriter.executeAsyncFlush():

  Phase 1（持锁）:
    wal.Rotate()                  // 创建新 WAL segment
    passive = memTable.Swap()     // active → passive

  Phase 2（无锁）:
    // 按时间窗口分组
    shardGroups = groupByShardKey(passive, shardDur)
    // shardGroups: map[string][]MemPoint

    对每组并行:
      shard = shardMgr.GetOrCreateShard(key)
      writer = sstable.NewWriter(tmpPath)
      writer.WriteMemPoints(points)
      writer.Close()
      // 返回 tmpPath → finalPath 映射

  Phase 3（持锁）:
    memTable.ClearPassive()
    对每个 shard:
      os.Rename(tmp, final)       // 原子化
      shard.registerSSTable(seq)  // 注册到 shard
      触发 shard compaction（如果需要）
    wal.TruncateCurrent()         // 清理旧 WAL segments
    saveCheckpoint()              // 持久化 checkpoint
```

**按 ShardKey 分组的实现：**

由于 MemTable 已按 Timestamp 排序，分组只需一次线性扫描：

```go
func groupByShardKey(points []MemPoint, shardDur int64) map[int64][]MemPoint {
    groups := make(map[int64][]MemPoint)
    for _, mp := range points {
        key := mp.Timestamp / shardDur * shardDur  // 向下取整到 shard 边界
        groups[key] = append(groups[key], mp)
    }
    return groups
}
```

### 5. 读路径（需合并两个来源）

```
Engine.Iterator(req):
  writer = shardMgr.GetWriter(db, meas)
  shards = shardMgr.GetShards(db, meas, start, end)

  来源 1: writer.MemTable.Iterator()   // active + passive（未 flush 数据）
  来源 2: [shard.NewIterator() for shard in shards]  // SSTable 文件

  合并：N 路归并（按 Timestamp）
```

ShardIterator 移除 MemTable 部分，仅保留 SSTable 读取。

### 6. WAL Replay（启动恢复）

```
recovery:
  for each db.measurement:
    writer = new MeasurementWriter(...)
    writer.wal.Replay(func(data []byte) error {
        mp = deserializeFromWAL(data)
        writer.memTable.Write(mp)
        if writer.memTable.ShouldFlush():
            writer.flushSync()    // 同步 flush，按 shard 分组写 SSTable
        return nil
    })
    如果 MemTable 还有数据:
      writer.flushSync()
```

## 内存模型对比

| 指标 | 当前 | 新架构 |
|------|------|--------|
| MemTable 数量 | N（Shard 数）= ~2,800 | 1（每 measurement） |
| WAL 缓冲数量 | N = ~2,800 | 1 |
| 空闲 Shard 开销 | ~100KB × N = ~280MB | ~500B × N = ~1.4MB（仅元数据） |
| 内存增长因子 | O(时间跨度 / ShardDuration) | O(活跃 measurement 数 × MemTable 大小) |
| 10M 点峰值 | ~1700MB | ~1200MB（预估） |

## 不变的部分

以下模块基本保持不变，仅调用方从 Shard 变为 MeasurementWriter：

| 模块 | 变化 |
|------|------|
| `memtable/` | **不变** — 接口和实现完全复用 |
| `wal/` | **不变** — WAL 不再绑定 Shard，但 Write/Replay/Rotate 逻辑不变 |
| `sstable/writer*.go` | **不变** — 仍从 MemPoint 写出列式 SSTable |
| `sstable/reader*.go` | **不变** |
| `compaction/` | **小幅改动** — ShardAccess 接口精简，去掉不必要的方法 |
| `types/` | **不变** |
| `engine/engine_write.go` | **改动** — 路由到 MeasurementWriter 而非 Shard |
| `engine/engine_query.go` | **改动** — 合并 MemTable + SSTable 两个来源 |
| `metadata/` | **不变** — SeriesStore/SchemaStore 本身已是全局的 |

## 实施步骤

### 阶段 1：MeasurementWriter（无 Shard 交互）

1. 创建 `internal/storage/writer/writer.go` — MeasurementWriter 结构体 + Write/WriteBatch
2. 从 Shard 移植 WAL 绑定、MemTable 管理、Schema 验证
3. 单元测试：Write 正确性、并发安全、背压机制

### 阶段 2：Shard 精简（移除 MemTable/WAL）

1. 删除 Shard 中的 `memTable`、`wal`、flush 相关字段和方法
2. 精简 `shard_flush.go` → 移除（flush 逻辑迁移到 writer）
3. 修改 `shard_io.go` → 移除 Write/WriteBatch（写入逻辑迁移到 writer）
4. Shard 保留：Open、Close、SSTable 管理、Compaction

### 阶段 3：Flush 实现（分组 + 多 SSTable）

1. 实现 `groupByShardKey()` 分组逻辑
2. 实现三阶段异步 flush（Rotate → Group+Write → Clear+Rename）
3. 单元测试：分组正确性、并发 flush、异常恢复

### 阶段 4：写路径切换

1. 修改 `Engine.Write/WriteBatch` 路由到 MeasurementWriter
2. 修改 `ShardManager.GetWriter()` 惰性创建 Writer
3. 集成测试：E2E write 测试通过

### 阶段 5：读路径适配

1. 修改 `ShardIterator` 移除 MemTable 部分
2. `Engine.Iterator` 合并 Writer.MemTable + Shard SSTables
3. 集成测试：E2E query 测试通过

### 阶段 6：WAL Replay 适配

1. 实现 Writer 级别的 Replay
2. 启动时按 measurement 恢复
3. 集成测试：E2E WAL 恢复测试通过

### 阶段 7：清理 + 文档

1. 移除废弃代码（Shard 的 write/flush/replay 方法）
2. 更新 README.md
3. 运行全部 E2E 测试套件

## 风险与缓解

| 风险 | 缓解 |
|------|------|
| Flush 分组产生大量小 SSTable | 复用现有 compaction 逻辑，小文件自然合并 |
| 单 MemTable 成为写入瓶颈 | MemTable 已有 active/passive 双缓冲，Swap 后写不阻塞 |
| 读路径需合并 MemTable + SSTable | 现有 Iterator 已有二路归并逻辑（memtable.go 中 `MemTableIterator` 合并 active+passive），扩展为合并 writer+shards |
| 向后兼容 | 外部 API（Engine.Write/Iterator）不变，E2E 测试无需改动 |

## 与现有 PR 的对比

之前的 Arena 方案（Task 1）试图在分配层面优化，但因"多小 Shard"场景下的 Arena 膨胀效应反而恶化。
**Arena 的教训：代码级优化无法弥补架构级缺陷。**

本次重构从架构层面消除"Shard = 写引擎"的设计，使内存增长仅依赖于活跃写入量，
不受查询时间范围影响。
