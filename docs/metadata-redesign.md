# MTS 全局元数据系统重设计方案

> 2026-05-08 | 基于 bbolt 存储引擎的单文件元数据系统

## 一、现状痛点总结

| # | 问题 | 严重程度 |
|---|------|----------|
| 1 | 元数据分散在多个小 JSON 文件中，文件数量随 DB/Meas 线性增长 | 高 |
| 2 | `tmp + fsync + rename` 原子写入模式复杂，每个文件单独操作 | 中 |
| 3 | 无事务支持，多文件写入可能不一致 | 高 |
| 4 | 内存状态与磁盘状态全量序列化/反序列化，非增量同步 | 中 |
| 5 | Manager.Load() 需遍历目录加载所有 JSON 文件 | 中 |
| 6 | 迁移逻辑复杂（migrate.go 处理旧格式兼容） | 低 |
| 7 | `MemoryMetaStore` 与 `MeasurementMetaStore` 双轨并行，前者死代码 | 中 |
| 8 | `MetaStore` 接口无人实现，`MeasurementMetaStore` 无接口抽象 | 中 |
| 9 | `Shard.sidCache` 写入但不用于读取，冗余 | 低 |
| 10 | 无统一模块边界 — Engine/ShardManager/Shard 各自直连 MetaStore | 高 |

## 二、主流 TSDB 元数据架构参考

### 2.1 InfluxDB IOx（云原生分析型 TSDB）

```
Catalog (PostgreSQL-backed)
  ├── NamespaceSchema      → 数据库/表/列定义
  ├── ParquetFileCatalog   → Parquet 文件清单 + 分区
  ├── TombstoneCatalog     → 删除标记
  └── PartitionCatalog     → 分区→Chunk 映射

关键决策：
- 集中式 Catalog，PostgreSQL 提供事务保证
- Schema 版本化（每列有类型和演进历史）
- 分区元数据与存储文件元数据分离
```

**对 MTS 的启发**：集中式目录管理 Database/Measurement，Schema 版本化。

---

### 2.2 Prometheus（监控型 TSDB）

```
TSDB Block
  ├── index/               → 全局倒排索引（postings list）
  │   ├── label→series     → 标签到 Series 的映射
  │   └── series→chunks    → 每个 Series 的数据块位置
  ├── chunks/              → 压缩数据块
  └── meta.json            → Block 元数据（时间范围、统计）

关键决策：
- 无全局元数据服务 — 每个 Block 自包含索引
- Series 由排序的 label set 唯一标识（无显式 SID）
- 索引文件按符号表 + posting list 组织，内存映射访问
```

**对 MTS 的启发**：Per-block 自包含索引适合不可变数据。但 MTS 有可变 Shard，需要更集中的 Series 管理。

---

### 2.3 OpenTSDB（HBase-based TSDB）

```
HBase Tables:
  ├── tsdb-uid/            → 指标名/TagK/TagV → UID 映射（双向）
  ├── tsdb/                → UID 编码的行键 → 数据点
  └── tsdb-tree/           → 元数据层次结构

关键决策：
- 全局 UID 系统：metric、tagk、tagv 都分配唯一整数 ID
- UID 缓存：内存中保留映射，减少 HBase 访问
- 数据行键 = metric_UID + timestamp + tagk_UID + tagv_UID
```

**对 MTS 的启发**：全局 UID 分配思想（MTS 已有 SID，但仅限 Measurement 作用域）。

---

### 2.4 VictoriaMetrics（高性能 Go TSDB）

```
Partition
  ├── indexdb/             → 倒排索引（tag→metric→tsid）
  │   ├── tag->metric_ids  → 标签到指标名列表
  │   └── metric_tsid      → 指标到 TSID 映射
  └── storage/             → 数据按 TSID 排序存储

关键决策：
- TSID（TimeSeriesID）：基于 metric+labels 的 hash，全局唯一
- 无 Schema 管理：字段动态推断类型（auto-schema）
- 倒排索引按分区组织，查询时需扫描多个分区
```

**对 MTS 的启发**：Hash-based TSID 避免全局分配协调；但 MTS 的 SID 是顺序分配的（更紧凑），各有优劣。

---

### 2.5 etcd / bbolt（嵌入式 KV 存储引擎）

```
bbolt DB 文件
  ├── Bucket 嵌套 → 天然适合 database/measurement 层级
  ├── ACID 事务 + MVCC → 一致性与无锁读
  ├── 内置 WAL + 崩溃恢复 → 无需额外 fsync 策略
  └── 单个文件 → 零文件膨胀

关键决策：
- 顺序 key 的 cursor 遍历效率极高（B+ 树）
- 写事务串行化，读事务无锁并发
- mmap 映射文件，页级缓存由 OS 管理
```

**对 MTS 的启发**：用 bbolt 替代多 JSON 文件，天然获得事务、崩溃恢复和有序遍历能力。

---

## 三、设计方案

### 3.1 磁盘布局

```
{dataDir}/
  ├── metadata.db           ← 单文件，包含所有元数据（bbolt）
  └── {database}/           ← 实际数据目录（结构不变）
      └── {measurement}/
          └── shards/
              ├── {startTime}_{endTime}/
              │   ├── data/               ← SSTable 文件
              │   │   ├── L0/sst_0/
              │   │   ├── L1/sst_1/
              │   │   └── _manifest.json  ← Level Manifest
              │   └── wal/                ← WAL 文件
              └── ...
```

### 3.2 bbolt Bucket/Key 设计

```
root
 ├─ {db1}                          ← Bucket (database name)
 │   ├─ {meas1}                    ← Bucket (measurement name)
 │   │   ├─ "_schema"              ← Key → JSON(Schema)
 │   │   ├─ "_retention"           ← Key → int64 nanoseconds (varint)
 │   │   ├─ "_created"             ← Key → int64 unix nano (varint)
 │   │   ├─ "series"               ← Bucket
 │   │   │   ├─ {sid_be_8bytes}    ← Key → JSON({"k":"v",...})
 │   │   │   └─ "_next_sid"        ← Key → uint64 (varint)
 │   │   ├─ "tag_index"            ← Bucket
 │   │   │   ├─ "{tag_key}\x00{tag_value}"  ← Key (prefix)
 │   │   │   │   └─ {sid_be_8bytes}         ← Key → empty
 │   │   │   └─ ...
 │   │   └─ "shards"               ← Bucket
 │   │       └─ {shard_id}         ← Key → JSON(ShardInfo)
 │   └─ {meas2} ...
 └─ {db2} ...
```

### 3.3 Key 编码说明

| 数据 | Key 编码 | Value 编码 | 说明 |
|------|---------|-----------|------|
| Schema | `_schema` | JSON | 低频读写 |
| Retention | `_retention` | varint (8 bytes) | 简单数值 |
| Created time | `_created` | varint (8 bytes) | db/meas 注册时间 |
| Series tags | `{sid_be}` | JSON `{"k":"v",...}` | SID 用 BigEndian 编码 |
| Next SID | `_next_sid` | varint | 自增计数器 |
| Tag 倒排 | `{key}\x00{val}` prefix, `{sid_be}` sub-key | 空 | 按 tag 查 SID 列表 |
| Shard info | `{shard_id}` | JSON | shard_id 即 `{start}_{end}` |

以 `_` 开头的 key 为元数据键。`_created` 存在即表示 database/measurement 已注册。
`ListDatabases()` 遍历 root 下所有非 `_` 开头的 Bucket name。
`DropDatabase()` 调用 `DeleteBucket()` 递归删除整个子树。

### 3.4 混合缓存策略

| 路径 | 策略 | 说明 |
|------|------|------|
| 高频读（GetTags、GetSIDsByTag） | 内存缓存 + RLock | 微秒级响应 |
| 所有写路径 | bbolt 写事务 → 更新内存缓存 | 保证持久化 |
| 低频读（ListDatabases、GetSchema、QueryShards） | bbolt 读事务 | 无需额外缓存 |
| 启动恢复 | 遍历 bbolt Bucket 树重建缓存 | 仅 series 数据需缓存 |

### 3.5 事务策略

| 操作类型 | 事务方式 | 说明 |
|---------|---------|------|
| 读 | `db.View()` | 只读事务，并发友好 |
| 单写 | `db.Update()` | 独立写事务，自动提交 |
| Close | `db.Close()` | 释放文件锁 |

bbolt 每次 Update 提交自动 fsync，不需要显式 Persist()/Sync()。bbolt 自身 MVCC + 序列化更新保证并发安全。

### 3.6 核心接口与类型

```go
// ===================================
// Manager — 全局元数据聚合入口
// ===================================

type Manager struct {
    catalog    *catalogStore
    series     *seriesStore
    shardIndex *shardIndex
    db         *bolt.DB
    mu         sync.RWMutex
    closeOnce  sync.Once
}

func NewManager(dataDir string) (*Manager, error)
func (m *Manager) Catalog() Catalog
func (m *Manager) Series() SeriesStore
func (m *Manager) Shards() ShardIndex
func (m *Manager) Load() error         // 从 bbolt 重建内存缓存
func (m *Manager) Sync() error         // 强制 fsync
func (m *Manager) Close() error

// ===================================
// Catalog — 目录管理
// ===================================

type Catalog interface {
    CreateDatabase(name string) error
    DropDatabase(name string) error
    ListDatabases() []string
    DatabaseExists(name string) bool

    CreateMeasurement(database, name string) error
    DropMeasurement(database, name string) error
    ListMeasurements(database string) ([]string, error)
    MeasurementExists(database, name string) bool

    GetSchema(database, measurement string) (*Schema, error)
    SetSchema(database, measurement string, s *Schema) error

    GetRetention(database, measurement string) (time.Duration, error)
    SetRetention(database, measurement string, d time.Duration) error
}

type Schema struct {
    Version   int64      `json:"version"`
    Fields    []FieldDef `json:"fields"`
    TagKeys   []string   `json:"tag_keys"`
    UpdatedAt int64      `json:"updated_at"`
}

type FieldDef struct {
    Name     string `json:"name"`
    Type     int32  `json:"type"`
    Nullable bool   `json:"nullable,omitempty"`
    Default  string `json:"default,omitempty"`
}

// ===================================
// SeriesStore — Series 管理
// ===================================

type SeriesStore interface {
    AllocateSID(database, measurement string, tags map[string]string) (uint64, error)
    GetTags(database, measurement string, sid uint64) (map[string]string, bool)
    GetSIDsByTag(database, measurement string, tagKey, tagValue string) []uint64
    SeriesCount(database, measurement string) int
}

// seriesStore 内部结构：
type seriesStore struct {
    db       *bolt.DB
    cache    sync.Map   // key: "db/meas/{sid}" → map[string]string
    nextSIDs sync.Map   // key: "db/meas" → uint64
}

// ===================================
// ShardIndex — Shard 时间范围索引
// ===================================

type ShardIndex interface {
    RegisterShard(database, measurement string, info ShardInfo) error
    UnregisterShard(database, measurement string, shardID string) error
    QueryShards(database, measurement string, startTime, endTime int64) []ShardInfo
    ListShards(database, measurement string) []ShardInfo
    UpdateShardStats(database, measurement, shardID string, sstableCount int, totalSize int64) error
}

type ShardInfo struct {
    ID           string `json:"id"`
    StartTime    int64  `json:"start_time"`
    EndTime      int64  `json:"end_time"`
    DataDir      string `json:"data_dir"`
    SSTableCount int    `json:"sstable_count"`
    TotalSize    int64  `json:"total_size"`
    CreatedAt    int64  `json:"created_at"`
}
```

### 3.7 关键设计决策

#### 决策 1：选择 bbolt 而非其他嵌入式 KV

| 候选 | 优点 | 缺点 |
|------|------|------|
| **bbolt** | etcd 使用，久经考验；Bucket 嵌套天然匹配层级模型；纯 Go；单个文件 | 写事务串行化（元数据写入量低，不是问题） |
| Badger | LSM 树，高写入吞吐 | CGO 依赖，更重，层级模型需自行编码 |
| SQLite | SQL 查询灵活 | 需要 CGO，schema 定义开销大 |
| Pebble | CockroachDB 使用，高性能 | key 扁平，无原生 Bucket 嵌套 |

bbolt 的 Bucket 嵌套直接映射 `database → measurement → {catalog, series, shards}`，是自然的选择。

#### 决策 2：SID 作用域 → Measurement 级别（维持现状）

- 紧凑（同 measurement 下 SID 从 0 开始递增）
- 不需要 hash 碰撞处理
- 与当前数据模型兼容

#### 决策 3：Tag 倒排索引持久化到 bbolt

Tag 倒排索引（tag→SID）直接持久化在 bbolt 的 `tag_index` bucket 中。由于 bbolt 的事务保证，倒排索引与 series 数据天然一致，无需在 Load 时重建。启动时仅需为高频查询重建内存缓存。

#### 决策 4：Series 高频读路径加内存缓存

`GetTags`（按 SID 查 tags）是写路径和查询路径中最高频的操作。使用 `sync.Map` 做读缓存，写事务完成后同步更新缓存，保证缓存与 bbolt 的一致性。

#### 决策 5：Schema 管理 → 显式注册 + 写入时校验

- 已注册 measurement：写入时校验字段类型
- 未注册 measurement：首次写入时自动注册 schema

#### 决策 6：WAL replay 顺序保证

WAL 不存 SID，replay 时重新分配。Manager 必须在任何 WAL replay 之前完成 `Load()`。

### 3.8 模块依赖关系

```
                 types/
                   ▲
                   │
               metadata/
                   ▲
                   │
        ┌──────────┼──────────┐
        │          │          │
     engine     shard      query
        │          │
        └──────────┘
           (engine 持有 *Manager，传给 shard)
```

**依赖规则**：
1. `metadata` 包依赖 `types`、`bbolt` 和标准库
2. `shard` 包依赖 `metadata` 包
3. `engine` 包持有 `*metadata.Manager`，在构造 `ShardManager` 时传入
4. `api` 包通过 `engine` 间接使用 metadata

### 3.9 文件布局

```
internal/storage/metadata/
  ├── manager.go              # Manager 聚合结构（New/Load/Sync/Close）
  ├── manager_test.go
  ├── catalog.go              # Catalog 接口
  ├── catalog_impl.go         # catalogStore 实现（bbolt 操作）
  ├── catalog_test.go
  ├── series.go               # SeriesStore 接口
  ├── series_impl.go          # seriesStore 实现（bbolt + 内存缓存）
  ├── series_test.go
  ├── shard_index.go          # ShardIndex 接口
  ├── shard_index_impl.go     # shardIndex 实现（bbolt 操作）
  └── shard_index_test.go
```

移除的文件：`persist.go`、`migrate.go` 及 `measurement/` 包下的 `meas_meta.go`、`meas_meta_query.go`、`db_meta.go` 及其测试文件。

---

## 四、测试计划

### 4.1 单元测试

所有测试用例基于临时 bbolt DB 文件，`t.Cleanup()` 中关闭并清理。接口不变，测试用例结构基本保留。

| 测试文件 | 覆盖内容 |
|---------|---------|
| `manager_test.go` | Manager 构建、Load、Sync、Close 生命周期 |
| `catalog_test.go` | Database/Measurement CRUD、Schema、Retention |
| `series_test.go` | SID 分配、tag 查询、跨 measurement 隔离、缓存一致性 |
| `shard_index_test.go` | Shard 注册、时间范围查询、统计更新、注销 |

覆盖率目标：≥ 90%。

### 4.2 集成测试

`engine_test.go`：dataDir 指向临时目录，bbolt DB 自动创建在其中。

### 4.3 E2E 测试

`tests/e2e/` 下 16 个测试用例无需修改，通过 engine API 间接使用 metadata。

---

## 五、对比总结

| 维度 | 旧方案（JSON 文件） | 新方案（bbolt） |
|------|---------------------|-----------------|
| 存储文件 | `_metadata/` 下多个 JSON 文件 | 单个 `metadata.db` |
| 事务 | 无（多文件写入可能不一致） | ACID 事务保证 |
| 崩溃恢复 | 依赖 tmp + fsync + rename | bbolt 内置 WAL |
| 持久化 | 全量序列化 → atomicWrite | 每次写事务自动 fsync |
| 加载 | 遍历目录逐文件读取 | 遍历 Bucket 树 |
| 并发 | RWMutex 保护内存 map | MVCC + 无锁读事务 |
| 删除 DB | 递归删除目录 + 文件 | `DeleteBucket()` 单操作 |
| 迁移 | 需 migrate.go 处理旧格式 | 不需要（开发阶段） |
| 依赖 | 无外部依赖 | `go.etcd.io/bbolt` |
