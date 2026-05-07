# MTS 全局元数据系统重设计方案

> 2026-05-07 | 综合参考 InfluxDB IOx / Prometheus / OpenTSDB / VictoriaMetrics

## 一、现状痛点总结

| # | 问题 | 严重程度 |
|---|------|----------|
| 1 | `MemoryMetaStore` 与 `MeasurementMetaStore` 双轨并行，前者死代码 | 中 |
| 2 | `MetaStore` 接口无人实现，`MeasurementMetaStore` 无接口抽象 | 中 |
| 3 | 两种持久化格式（二进制 MTSH vs JSON），不一致 | 中 |
| 4 | `MeasurementMeta.NextSid` (int64) 与 `MeasurementMetaStore.nextSID` (uint64) 类型不一致 | 低 |
| 5 | Database→Measurement 映射纯内存，无持久化 | 中 |
| 6 | Measurement Schema (`FieldSchema`) 无生产级使用方，SSTable 自管 `_schema.json` | 中 |
| 7 | `Shard.sidCache` 写入但不用于读取，冗余 | 低 |
| 8 | Tag Index 无去重保护（`MemoryMetaStore` 路径） | 低 |
| 9 | WAL 不存 SID，replay 依赖 MetaStore 状态正确 | 中 |
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

## 三、设计方案

### 3.1 整体架构

```
┌──────────────────────────────────────────────────┐
│ metadata 包（全局统一入口）                          │
│                                                    │
│  Catalog                                            │
│  ├── DatabaseRegistry    → database 增删查          │
│  ├── MeasurementRegistry → measurement 增删查       │
│  └── SchemaStore         → field/tag schema 管理    │
│                                                    │
│  SeriesStore                                       │
│  ├── SIDAllocator        → Series ID 分配           │
│  ├── TagIndex            → 标签倒排索引              │
│  └── TagCache            → SID→Tags 正向缓存         │
│                                                    │
│  ShardIndex                                        │
│  ├── ShardCatalog        → 时间窗口→Shard 映射       │
│  └── FileManifest        → SSTable 文件清单          │
│                                                    │
│  Persistence                                       │
│  ├── manifest.json       → 全局清单（database/meas） │
│  ├── schema.json         → 每 measurement 的 schema │
│  └── series/             → 每 measurement 的 series │
│                                                    │
└──────────────────────────────────────────────────┘
         ▲            ▲            ▲
         │            │            │
    ┌────┴────┐ ┌─────┴─────┐ ┌───┴────┐
    │ Engine  │ │ ShardMgr  │ │  API   │
    └─────────┘ └───────────┘ └────────┘
```

### 3.2 核心接口

```go
// Catalog 管理数据库和 Measurement 的目录。
type Catalog interface {
    CreateDatabase(name string) error
    DropDatabase(name string) error
    ListDatabases() []string
    DatabaseExists(name string) bool

    CreateMeasurement(database, name string) error
    DropMeasurement(database, name string) error
    ListMeasurements(database string) ([]string, error)
    MeasurementExists(database, name string) bool

    // Schema 管理
    GetSchema(database, measurement string) (*Schema, error)
    SetSchema(database, measurement string, schema *Schema) error
    GetRetention(database, measurement string) (time.Duration, error)
    SetRetention(database, measurement string, d time.Duration) error
}

// SeriesStore 管理 Series（唯一标签组合）。
type SeriesStore interface {
    // SID 分配：若 tags 已存在返回已有 SID，否则分配新 SID
    AllocateSID(database, measurement string, tags map[string]string) (uint64, error)
    // 根据 SID 获取标签
    GetTags(database, measurement string, sid uint64) (map[string]string, bool)
    // 根据标签查询所有匹配的 SID
    GetSIDsByTag(database, measurement string, tagKey, tagValue string) []uint64
}

// ShardIndex 管理 Shard 的时间范围索引。
type ShardIndex interface {
    RegisterShard(database, measurement string, info ShardInfo) error
    UnregisterShard(database, measurement string, shardID string) error
    GetShards(database, measurement string, startTime, endTime int64) []ShardInfo
    GetShardInfo(database, measurement string, shardID string) (ShardInfo, bool)
}

// GlobalMeta 聚合所有元数据子系统。
type GlobalMeta struct {
    Catalog     Catalog
    Series      SeriesStore
    Shards      ShardIndex
}

func NewGlobalMeta(dataDir string) (*GlobalMeta, error)
func (g *GlobalMeta) Close() error
func (g *GlobalMeta) Persist() error
```

### 3.3 Schema 类型定义

```go
// Schema 定义 Measurement 的字段和标签结构。
type Schema struct {
    Version   int64       `json:"version"`
    Fields    []FieldDef  `json:"fields"`
    TagKeys   []string    `json:"tag_keys"`
    UpdatedAt int64       `json:"updated_at"`
}

type FieldDef struct {
    Name string    `json:"name"`
    Type FieldType `json:"type"`
    // 字段元数据（可选）
    Nullable bool   `json:"nullable,omitempty"`
    Default  string `json:"default,omitempty"`  // 默认值（JSON 编码）
}
```

### 3.4 关键设计决策

#### 决策 1：SID 作用域 → Measurement 级别（维持现状）

**理由**：不同于 OpenTSDB 的全局 UID 和 VictoriaMetrics 的 hash TSID，MTS 的 measurement-scoped SID 优势是：
- 紧凑（同 measurement 下 SID 从 0 开始递增）
- 不需要 hash 碰撞处理
- 与当前数据模型兼容，迁移成本低

**取舍**：跨 measurement 查询时 SID 不唯一，但这类查询极少。

#### 决策 2：Schema 管理 → 显式注册，写入时校验

**理由**：InfluxDB IOx 的 schema-on-write 模式适合分析场景。MTS 作为嵌入式 TSDB，采用显式 schema + 写入时自动补全的混合模式：
- 已注册 measurement：写入时校验字段类型
- 未注册 measurement：首次写入时自动注册 schema（auto-register）

**取舍**：自动注册方便开发但弱化类型安全。未来可加配置开关。

#### 决策 3：持久化 → 统一 JSON + atomic write

**理由**：当前双格式（二进制 MTSH + JSON）增加维护成本。JSON 的 trade-off：
- 优点：人类可读、跨语言、版本兼容好
- 缺点：比二进制大。对元数据量级（每 measurement 数千 series）可忽略

使用 `tmp + fsync + rename` 模式保证原子性（当前 `MeasurementMetaStore` 已实现）。

#### 决策 4：Catalog 持久化 → manifest.json

新增全局 manifest 文件记录 database→measurement 层次：

```json
{
  "version": 1,
  "databases": {
    "metrics": {
      "measurements": ["cpu", "memory", "disk"],
      "created_at": 1715000000000000000
    }
  }
}
```

#### 决策 5：WAL replay 中的 SID → MetaStore 状态

当前 WAL 不存 SID，replay 时重新分配。保持此行为但要求：
- SeriesStore 必须在任何 WAL replay 之前从持久化文件加载
- `Close()` 先持久化 SeriesStore 再刷 WAL（确保 SID 状态 ≥ WAL 位置）

### 3.5 模块依赖关系

```
                 types/
                   ▲
                   │ (imports FieldType, Schema, etc.)
                   │
               metadata/
                   ▲
                   │ (depends on)
        ┌──────────┼──────────┐
        │          │          │
     engine     shard      query
        │          │
        └──────────┘
           (engine 通过 shard 间接使用 metadata)
```

**依赖规则**：
1. `metadata` 包只依赖 `types` 包
2. `shard` 包依赖 `metadata` 包（通过 GlobalMeta）
3. `engine` 包持有 `*metadata.GlobalMeta`，传给 `shard.ShardManager`
4. `api` 包通过 `engine` 间接使用 metadata

### 3.6 迁移计划

**Phase 1：接口层**（不改行为）
1. 在 `metadata` 包定义接口和类型
2. 创建 `GlobalMeta` 聚合结构
3. 实现 `ShimCatalog`、`ShimSeriesStore` — 包装现有 `MeasurementMetaStore` 和 `DatabaseMetaStore`
4. 修改 `Engine` 持有 `*metadata.GlobalMeta`，内部仍用旧实现

**Phase 2：统一实现**
1. 将 `MeasurementMetaStore` 核心逻辑迁移到 `metadata.SeriesStore`
2. 实现 `Catalog` 的持久化逻辑（manifest.json）
3. 清理 `MemoryMetaStore` 和 `MetaStore` 接口
4. 将 `MeasurementMeta` proto 类型替换为 `metadata.Schema`

**Phase 3：收缩集成**
1. `ShardManager` 不再直接管理 MetaStore
2. `Shard` 通过 `GlobalMeta` 访问 Series
3. 移除 `ShardConfig.MetaStore` 字段

### 3.7 文件布局

```
internal/storage/metadata/
  ├── catalog.go           # Catalog 接口与实现
  ├── catalog_test.go
  ├── series.go            # SeriesStore 接口与实现
  ├── series_test.go
  ├── shard_index.go       # ShardIndex 接口与实现
  ├── shard_index_test.go
  ├── global.go            # GlobalMeta 聚合结构
  ├── global_test.go
  ├── persist.go           # 统一持久化（manifest.json, schema, series）
  ├── persist_test.go
  ├── migrate.go           # 从旧格式迁移工具
  └── migrate_test.go
```

---

## 四、对比总结

| 维度 | 当前架构 | 新架构 |
|------|----------|--------|
| 入口 | Engine.dbMetaStores + ShardManager.metaStores | `metadata.GlobalMeta` 统一入口 |
| 接口 | 无（`MetaStore` 死接口） | `Catalog` / `SeriesStore` / `ShardIndex` 三大接口 |
| 持久化 | 双格式（二进制+JSON） | 统一 JSON + atomic write |
| Database 元数据 | 纯内存，重启丢失 | manifest.json 持久化 |
| Schema | 分散在 proto + SSTable | 统一管理，写入时可校验 |
| WAL replay | 依赖 MetaStore 内存状态 | 显式加载→replay→持久化顺序保证 |
| 可测试性 | 紧耦合具体实现 | 接口隔离，可 mock |
| 死代码 | MemoryMetaStore + MetaStore 接口 | 全部移除 |
