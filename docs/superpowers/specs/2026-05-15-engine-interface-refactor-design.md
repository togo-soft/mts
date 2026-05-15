# Engine 接口重构设计

## 目标

将 Engine 中的写入、刷盘、元数据三大职责通过细粒度接口解耦，提高可测试性和可维护性。

## 核心接口

### Writer 接口（engine 包）

负责 WAL/MemTable 写入处理，实现者为 `writer.MeasurementWriter`。

```go
type Writer interface {
    Write(point *types.Point) error
    WriteBatch(points []*types.Point) (int, error)
    MemTable() *memtable.MemTable
    SeriesStore() SeriesStore
    Close() error
}
```
注：Flush() 从 Writer 接口中移除，刷盘编排由 FlushCoordinator 直接操作 MemTable.Swap() 完成。

### Flusher 接口（engine 包）

负责 SSTable/Compaction 处理，实现者为重构后的 `shard.ShardManager`。

```go
type Flusher interface {
    Flush(db, measurement string, points []types.MemPoint) error
    Compact(db, measurement string, startTime int64) error
    GetShards(db, measurement string, startTime, endTime int64) []*shard.Shard
    CloseAll() error
    SetConfig(config *compaction.Config)
}
```
注：GetShards 返回 `[]*shard.Shard` 而非 `[]*ShardHandle`，避免链式修改 query 包。

### Metadata 子接口（engine 包，三个独立接口）

```go
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

type SeriesStore interface {
    AllocateSID(database, measurement string, tags map[string]string) (uint64, error)
    GetTags(database, measurement string, sid uint64) (map[string]string, bool)
    GetSIDsByTag(database, measurement string, tagKey, tagValue string) []uint64
    SeriesCount(database, measurement string) int
}

type ShardIndex interface {
    RegisterShard(database, measurement string, info ShardInfo) error
    UnregisterShard(database, measurement string, shardID string) error
    QueryShards(database, measurement string, startTime, endTime int64) []ShardInfo
    ListShards(database, measurement string) []ShardInfo
    UpdateShardStats(database, measurement, shardID string, sstableCount int, totalSize int64) error
}
```

## Engine 新结构

```go
type Engine struct {
    cfg      *Config
    catalog  Catalog
    series   SeriesStore
    shards   ShardIndex
    writers  map[string]Writer
    flusher  Flusher
    coord    *FlushCoordinator
    mu       sync.RWMutex
    closed   atomic.Bool
    queryWg  sync.WaitGroup
}
```

## FlushCoordinator（engine 包内部组件）

编排 Writer → Flusher 的异步刷盘流程，不暴露为接口：
- 后台检查 `Writer.MemTable().ShouldSwap()`
- 调用 `Writer.Flush()` 触发 MemTable swap
- 将 passive 数据通过 `Flusher.Flush()` 写入 SSTable
- 触发 `Flusher.Compact()`

## 数据流

```
写入: Engine.Write(p) → Writer.Write(p) → WAL + MemTable

刷新: FlushCoordinator
        ├── Writer.Flush() → swap MemTable
        └── Flusher.Flush(points) → SSTable → Shard
             └── Flusher.Compact() → Level Compaction

查询: Engine.Iterator(req) → Flusher.GetShards() + Writer.MemTable()
        └── 归并 MemTable + SSTable → Iterator

元数据: Engine → Catalog / SeriesStore / ShardIndex
```

## 实现映射

| 接口 | 实现 | 说明 |
|------|------|------|
| Writer | writer.MeasurementWriter | 去掉 Flush/ShardStore/Compaction 依赖 |
| Flusher | shard.ShardManager（重构后） | Shard 纯 SSTable 容器，实现 Flusher |
| Catalog | metadata.catalogStore | 已有接口，直接复用 |
| SeriesStore | metadata.seriesStore | 已有接口 |
| ShardIndex | metadata.shardIndex | 已有接口 |

## 删除内容

- Shard 中的 WAL、MemTable 字段及相关方法
- ShardConfig.DiskOnly 标志
- writer.ShardStore 接口（被 Flusher 替代）
- writer.SeriesStore、writer.SchemaStore（Engine 直接注入）
- Engine 对具体类型的直接依赖

## 验收标准

- 所有现有单元测试通过
- 所有 e2e 测试通过
- 代码行覆盖率 >= 90%
- golangci-lint 无新增问题
