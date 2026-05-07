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

### 3.1 磁盘目录布局

```
{dataDir}/
  │
  ├── _metadata/                          ← 元数据专用目录（与数据隔离）
  │   ├── manifest.json                   ← 全局目录：database→measurement 映射
  │   └── {database}/                     ← 每 database 一个子目录
  │       └── {measurement}/              ← 每 measurement 一个子目录
  │           ├── schema.json             ← 字段定义 + 标签键
  │           ├── series.json             ← tags→SID 映射 + 倒排索引
  │           └── shards.json             ← 时间窗口→shard 映射
  │
  └── {database}/                         ← 实际数据目录（结构不变）
      └── {measurement}/
          └── shards/
              ├── {startTime}_{endTime}/   ← 例如 0000000000000000000_0000000864000000000
              │   ├── data/               ← SSTable 文件（由 Shard 内部管理）
              │   │   ├── L0/sst_0/       ← Level Compaction 的 L0
              │   │   ├── L1/sst_1/       ← Level Compaction 的 L1
              │   │   └── _manifest.json  ← Level Manifest（compaction 内部用）
              │   └── wal/                ← WAL 文件
              └── ...
```

**核心原则**：
- 元数据 (`_metadata/`) 与数据 (`{database}/`) **物理分离**，元数据可独立备份/检查
- 元数据目录镜像数据目录的 database/measurement 层次
- Shard 内部的 SSTable 文件清单仍由 `LevelManifest` 管理（compaction 策略细节属于 Shard 内部实现）
- Manager 的 `ShardIndex` 只关心 Shard 级别：时间窗口、数据目录路径、创建时间

---

### 3.2 各文件的存储内容

#### 3.2.1 `manifest.json` — 全局目录

**路径**：`{dataDir}/_metadata/manifest.json`

**内容**：所有 database 和 measurement 的存在性、schema 版本、retention 配置。

```json
{
  "version": 1,
  "databases": {
    "metrics": {
      "measurements": {
        "cpu": {
          "schema_version": 1,
          "retention_ns": 2592000000000000,
          "created_at": 1715000000000000000
        },
        "memory": {
          "schema_version": 2,
          "retention_ns": 0,
          "created_at": 1715001000000000000
        }
      },
      "created_at": 1715000000000000000
    }
  }
}
```

**更新时机**：
- `CreateDatabase` → 写入新 database 条目
- `DropDatabase` → 删除 database 及下属所有 measurement 条目
- `CreateMeasurement` → 写入新 measurement 条目
- `DropMeasurement` → 删除 measurement 条目
- `SetRetention` → 更新 `retention_ns` 字段

#### 3.2.2 `schema.json` — Measurement Schema

**路径**：`{dataDir}/_metadata/{database}/{measurement}/schema.json`

**内容**：字段定义（名称 + 类型）和标签键列表。版本号支持 schema 演进检测。

```json
{
  "version": 2,
  "fields": [
    {"name": "usage",    "type": 2, "nullable": false, "default": "0.0"},
    {"name": "temp",     "type": 2, "nullable": true,  "default": null},
    {"name": "hostname", "type": 3, "nullable": false, "default": "\"\""}
  ],
  "tag_keys": ["host", "region", "dc"],
  "updated_at": 1715100000000000000
}
```

**FieldType 枚举**：`1=int64, 2=float64, 3=string, 4=bool`

**更新时机**：
- 显式注册：`Manager.SetSchema(db, meas, schema)`
- 自动注册（可选）：首次写入时自动推断字段类型

#### 3.2.3 `series.json` — Series 索引

**路径**：`{dataDir}/_metadata/{database}/{measurement}/series.json`

**内容**：Series ID 分配状态 + tags→SID 映射。Tag 倒排索引在 `Load` 时自动重建。

```json
{
  "next_sid": 42,
  "series": {
    "0":  ["host", "server1", "region", "us-east"],
    "1":  ["host", "server2", "region", "us-west"],
    "2":  ["host", "server1", "region", "us-west", "dc", "nyc1"]
  }
}
```

**更新时机**：
- `AllocateSID` → 标记 dirty，`Persist` 时批量写入
- `Persist` → 在 Close、定期持久化、或主动调用时触发

注意：tag 倒排索引（tagKey\x00tagValue → []SID）**不持久化**，在 `Load` 时从 series 数据重建。这减少了持久化数据量和一致性问题。

#### 3.2.4 `shards.json` — Shard 目录

**路径**：`{dataDir}/_metadata/{database}/{measurement}/shards.json`

**内容**：该 measurement 下所有 shard 的时间窗口和路径信息。

```json
{
  "shards": [
    {
      "id":          "0000000000000000000_0000000864000000000",
      "start_time":  0,
      "end_time":    864000000000000,
      "data_dir":    "metrics/cpu/shards/0000000000000000000_0000000864000000000",
      "sstable_count": 3,
      "total_size":  52428800,
      "created_at":  1715000000000000000
    },
    {
      "id":          "0000000864000000000_0000001728000000000",
      "start_time":  864000000000000,
      "end_time":    1728000000000000,
      "data_dir":    "metrics/cpu/shards/0000000864000000000_0000001728000000000",
      "sstable_count": 5,
      "total_size":  104857600,
      "created_at":  1715086400000000000
    }
  ]
}
```

**更新时机**：
- Shard 创建 → 追加新条目
- Shard 删除（retention） → 删除条目并清理 `data_dir`
- SSTable 数量/大小变更 → 定期更新（非严格实时）

### 3.3 整体架构图

```
┌──────────────────────────────────────────────────┐
│ metadata 包（全局统一入口）                          │
│                                                    │
│  Manager（聚合入口）                                 │
│  ├── Catalog      → database/meas 目录 + schema    │
│  ├── SeriesStore  → SID 分配 + 标签索引             │
│  └── ShardIndex   → 时间窗口→Shard 映射             │
│                                                    │
│  持久化层:                                           │
│  ├── _metadata/manifest.json    ← Catalog          │
│  ├── _metadata/{db}/{meas}/schema.json  ← Schema   │
│  ├── _metadata/{db}/{meas}/series.json ← Series   │
│  └── _metadata/{db}/{meas}/shards.json ← Shards   │
│                                                    │
└──────────────────────────────────────────────────┘
         ▲            ▲            ▲
         │            │            │
    ┌────┴────┐ ┌─────┴─────┐ ┌───┴────┐
    │ Engine  │ │ ShardMgr  │ │  API   │
    └─────────┘ └───────────┘ └────────┘
```

### 3.4 核心接口与类型

```go
// ===================================
// Manager — 全局元数据聚合入口
// ===================================

// Manager 是所有元数据子系统的聚合入口。
// Engine 持有 *Manager 实例并将其传递给 ShardManager 和 API 层。
type Manager struct {
    catalog     Catalog
    seriesStore SeriesStore
    shardIndex  ShardIndex
    dataDir     string
    metaDir     string  // {dataDir}/_metadata
    mu          sync.RWMutex
}

func NewManager(dataDir string) (*Manager, error)
func (m *Manager) Catalog() Catalog
func (m *Manager) Series() SeriesStore
func (m *Manager) Shards() ShardIndex
func (m *Manager) Close() error      // 持久化所有脏数据后释放资源
func (m *Manager) Persist() error    // 强制持久化所有子系统

// ===================================
// Catalog — 目录管理
// ===================================

type Catalog interface {
    // Database 操作
    CreateDatabase(name string) error
    DropDatabase(name string) error
    ListDatabases() []string
    DatabaseExists(name string) bool

    // Measurement 操作
    CreateMeasurement(database, name string) error
    DropMeasurement(database, name string) error
    ListMeasurements(database string) ([]string, error)
    MeasurementExists(database, name string) bool

    // Schema 操作
    GetSchema(database, measurement string) (*Schema, error)
    SetSchema(database, measurement string, s *Schema) error

    // Retention 操作
    GetRetention(database, measurement string) (time.Duration, error)
    SetRetention(database, measurement string, d time.Duration) error
}

// Schema 定义 Measurement 的字段和标签结构。
type Schema struct {
    Version   int64       `json:"version"`
    Fields    []FieldDef  `json:"fields"`
    TagKeys   []string    `json:"tag_keys"`
    UpdatedAt int64       `json:"updated_at"`
}

type FieldDef struct {
    Name     string `json:"name"`
    Type     int32  `json:"type"`  // FieldType 枚举值
    Nullable bool   `json:"nullable,omitempty"`
    Default  string `json:"default,omitempty"` // JSON 编码的默认值
}

// ===================================
// SeriesStore — Series 管理
// ===================================

type SeriesStore interface {
    // AllocateSID 分配或查找 SID。tags 不存在则分配新 SID。
    AllocateSID(database, measurement string, tags map[string]string) (uint64, error)
    // GetTags 根据 SID 获取标签（返回副本）。
    GetTags(database, measurement string, sid uint64) (map[string]string, bool)
    // GetSIDsByTag 根据标签键值查找匹配的 SIDs。
    GetSIDsByTag(database, measurement string, tagKey, tagValue string) []uint64
    // SeriesCount 返回 measurement 的 series 总数。
    SeriesCount(database, measurement string) int
}

// ===================================
// ShardIndex — Shard 时间范围索引
// ===================================

type ShardIndex interface {
    // RegisterShard 注册新 Shard（创建时调用）。
    RegisterShard(database, measurement string, info ShardInfo) error
    // UnregisterShard 注销 Shard（retention 删除时调用）。
    UnregisterShard(database, measurement string, shardID string) error
    // QueryShards 返回与时间范围相交的所有 Shard。
    QueryShards(database, measurement string, startTime, endTime int64) []ShardInfo
    // ListShards 返回该 measurement 的所有 Shard（按时间排序）。
    ListShards(database, measurement string) []ShardInfo
    // UpdateShardStats 更新 SSTable 数量和总大小（非实时，定期调用）。
    UpdateShardStats(database, measurement, shardID string, sstableCount int, totalSize int64) error
}

// ShardInfo 描述单个 Shard 的元数据。
type ShardInfo struct {
    ID           string `json:"id"`            // "{startTime}_{endTime}"
    StartTime    int64  `json:"start_time"`
    EndTime      int64  `json:"end_time"`
    DataDir      string `json:"data_dir"`      // 相对路径（从 dataDir 算起）
    SSTableCount int    `json:"sstable_count"`
    TotalSize    int64  `json:"total_size"`
    CreatedAt    int64  `json:"created_at"`
}
```

### 3.5 关键设计决策

#### 决策 1：SID 作用域 → Measurement 级别（维持现状）

**理由**：不同于 OpenTSDB 的全局 UID 和 VictoriaMetrics 的 hash TSID，MTS 的 measurement-scoped SID 优势是：
- 紧凑（同 measurement 下 SID 从 0 开始递增）
- 不需要 hash 碰撞处理
- 与当前数据模型兼容，迁移成本低

#### 决策 2：Tag 倒排索引不持久化

**理由**：倒排索引（tag→SID）可从 `series` 数据完全重建。不持久化带来：
- 减少持久化数据量约 40%
- 消除 `MemoryMetaStore` 中存在的重复条目风险
- Load 时重建成本低（O(series_count × avg_tag_count)，对于 10w series 约 50ms）

#### 决策 3：持久化 → 统一 JSON + atomic write

**理由**：当前双格式（二进制 MTSH + JSON）增加维护成本。JSON 的 trade-off：
- 优点：人类可读、跨语言、版本兼容好
- 缺点：比二进制大。对元数据量级（每 measurement 数万 series）可忽略

原子写入策略：`tmp + fsync + rename`。

#### 决策 4：Shard 内部文件清单不上升至 Manager

Shard 内部 SSTable 的文件组织（Level/L0/L1 等）由 `LevelCompactionManager` 通过自己的 `_manifest.json` 管理。Manager 的 `ShardIndex` 只关心 Shard 级别的时间窗口和统计信息。这保持了 compaction 策略的可替换性。

#### 决策 5：Schema 管理 → 显式注册 + 写入时校验

- 已注册 measurement：写入时校验字段类型
- 未注册 measurement：首次写入时自动注册 schema（auto-register，可配置关闭）

#### 决策 6：WAL replay 顺序保证

WAL 不存 SID，replay 时重新分配。要求：
- Manager 必须在任何 WAL replay 之前完成 `Load()`
- `Close()` 先 Persist Manager，再关闭 Shard（确保 SID 状态 ≥ WAL 位置）

### 3.6 模块依赖关系

```
                 types/
                   ▲
                   │ (imports FieldType, Schema types only)
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
1. `metadata` 包只依赖 `types` 包（和标准库）
2. `shard` 包依赖 `metadata` 包（通过 `*Manager` 访问 Series/ShardIndex）
3. `engine` 包持有 `*metadata.Manager`，在构造 `ShardManager` 时传入
4. `api` 包通过 `engine` 间接使用 metadata

### 3.7 文件布局

```
internal/storage/metadata/
  ├── manager.go           # Manager 聚合结构（New/Close/Persist）
  ├── manager_test.go
  ├── catalog.go           # Catalog 接口与实现
  ├── catalog_test.go
  ├── series.go            # SeriesStore 接口与实现
  ├── series_test.go
  ├── shard_index.go       # ShardIndex 接口与实现
  ├── shard_index_test.go
  ├── persist.go           # 持久化辅助（atomic write, load/save）
  ├── persist_test.go
  ├── migrate.go           # 从旧 MeasurementMetaStore JSON 迁移
  └── migrate_test.go
```

### 3.8 迁移计划

**Phase 1：接口层**（不改行为）
1. 在 `metadata` 包定义接口和核心类型
2. 实现 `Manager` 聚合结构（纯内存，不持久化）
3. 实现 `ShimCatalog`、`ShimSeriesStore` — 包装现有 `MeasurementMetaStore` 和 `DatabaseMetaStore`
4. 修改 `Engine` 持有 `*metadata.Manager`，内部仍用旧实现

**Phase 2：统一持久化**
1. 实现 `_metadata/manifest.json` 的读写
2. 实现 `schema.json`、`series.json`、`shards.json` 的读写
3. 清理 `MemoryMetaStore` 和其 `MetaStore` 接口
4. 删除 `MeasurementMetaStore` 的持久化逻辑（迁移到 metadata 包）

**Phase 3：收缩集成**
1. `ShardManager` 不再直接管理 MetaStore
2. `Shard` 通过 `Manager` 访问 Series
3. 移除 `ShardConfig.MetaStore` 字段
4. 清理 `types/` 中的 `MeasurementMeta` proto 类型（或降级为内部类型）

---

## 四、端到端测试计划

所有 e2e 测试用例位于 `tests/e2e/metadata/` 目录下，覆盖 Manager 全部子系统的功能、持久化、并发和迁移场景。

### 测试目录结构

```
tests/e2e/metadata/
  ├── main.go                  ← 测试入口（或按子测试分文件）
  ├── catalog_crud_test.go     ← Catalog CRUD 用例
  ├── schema_test.go           ← Schema 注册与验证用例
  ├── series_test.go           ← SID 分配与标签查询用例
  ├── shard_index_test.go      ← Shard 注册与查询用例
  ├── persistence_test.go      ← 持久化与恢复用例
  ├── concurrent_test.go       ← 并发安全用例
  ├── integration_test.go      ← 全链路集成用例
  ├── migration_test.go        ← 旧格式迁移用例
  └── run.sh                   ← 一键执行脚本
```

### 4.1 Catalog CRUD 测试（catalog_crud_test.go）

| 用例 | 操作 | 预期 |
|------|------|------|
| `CreateDatabase` | 创建 `metrics` 数据库 | 成功，`ListDatabases` 返回包含 `metrics` |
| `CreateDatabase_Duplicate` | 再次创建 `metrics` | 返回错误（已存在） |
| `CreateDatabase_EmptyName` | 创建空名称数据库 | 返回错误 |
| `DropDatabase` | 删除 `metrics` | 成功，下属 measurement 也被删除 |
| `DropDatabase_NotFound` | 删除不存在的数据库 | 返回错误 |
| `ListDatabases_Empty` | 无数据库时列出 | 返回空列表 |
| `CreateMeasurement` | 在 `metrics` 下创建 `cpu` | 成功，`ListMeasurements` 返回包含 `cpu` |
| `CreateMeasurement_DBNotFound` | 在不存在的 DB 下创建 | 返回错误 |
| `DropMeasurement` | 删除 `cpu` | 成功，关联的 schema/series/shard 元数据也删除 |
| `DatabaseExists/MeasurementExists` | 检查存在性 | 正确反映创建/删除后的状态 |
| `ListMeasurements_Alphabetical` | 创建 `cpu`, `disk`, `memory` | 按字母序返回 `["cpu","disk","memory"]` |

### 4.2 Schema 测试（schema_test.go）

| 用例 | 操作 | 预期 |
|------|------|------|
| `SetSchema_New` | 为 measurement 设置初始 schema | 成功，version=1 |
| `SetSchema_Update` | 更新已有 schema（加字段） | version 递增，updated_at 更新 |
| `SetSchema_Incompatible` | 修改已有字段的类型 | 返回错误（类型不兼容） |
| `GetSchema_NotFound` | 获取未注册 measurement 的 schema | 返回 nil 或错误 |
| `SetSchema_EmptyFields` | 设置空字段列表的 schema | 成功（允许无字段 measurement） |
| `Schema_Persistence` | 设置 schema → 重启 Manager → 读取 | schema 一致（包括 version, fields, tag_keys） |
| `Schema_TagKeys` | 设置包含 tag_keys 的 schema | tag_keys 正确存储并返回 |

### 4.3 Series 测试（series_test.go）

| 用例 | 操作 | 预期 |
|------|------|------|
| `AllocateSID_New` | 为新 tags 分配 SID | 返回 SID=0 |
| `AllocateSID_SecondNew` | 分配另一个新 tags | 返回 SID=1（递增） |
| `AllocateSID_SameTags` | 对相同 tags 再次分配 | 返回已有 SID（不递增） |
| `AllocateSID_DifferentOrder` | tags 键值相同但顺序不同 | 返回已有 SID（顺序无关） |
| `AllocateSID_ManySeries` | 分配 10,000 个不同 series | 全部成功，SID 连续，SeriesCount=10000 |
| `GetTags` | 通过 SID 获取 tags | 返回正确的 tags 副本 |
| `GetTags_NotFound` | 查询不存在的 SID | 返回 false |
| `GetSIDsByTag` | 按 tag 过滤 | 返回所有匹配的 SIDs |
| `GetSIDsByTag_NotFound` | 查询不存在的 tag | 返回空列表 |
| `GetSIDsByTag_Multiple` | 同一 tag 值关联多个 series | 返回所有匹配的 SIDs |
| `Series_Persistence` | 分配 1000 series → 重启 → 查询 | SID 延续（next_sid=1000），所有 tags 可查 |
| `Series_DifferentMeasurements` | 两个 measurement 各自分配 SID | 各自从 0 开始，互不干扰 |

### 4.4 ShardIndex 测试（shard_index_test.go）

| 用例 | 操作 | 预期 |
|------|------|------|
| `RegisterShard` | 注册一个 shard（时间窗口 0-1h） | 成功 |
| `RegisterShard_Overlap` | 注册重叠时间窗口的 shard | 成功（允许重叠） |
| `RegisterShard_SameID` | 注册相同 ID 的 shard | 返回错误 |
| `QueryShards_Exact` | 查询与 shard 完全重叠的时间范围 | 返回该 shard |
| `QueryShards_Partial` | 查询部分重叠的时间范围 | 返回相交的 shard |
| `QueryShards_NoMatch` | 查询无匹配的时间范围 | 返回空列表 |
| `QueryShards_Multiple` | 注册 3 个不重叠 shard，查询跨越 2 个 | 返回 2 个 shard（按时间排序） |
| `QueryShards_AdjacentBoundary` | shard=[100,200)，查询 [200,300) | 返回空（边界不重叠） |
| `UnregisterShard` | 注销一个 shard | 成功，`QueryShards` 不再返回 |
| `UnregisterShard_NotFound` | 注销不存在的 shard | 返回错误 |
| `ListShards` | 注册 3 个 shard（乱序） | 按 start_time 升序返回 |
| `UpdateShardStats` | 更新 shard 的 SSTable 数量和大小 | stats 正确更新 |
| `Shards_Persistence` | 注册 3 shard → 重启 → 查询 | 全部 shard 可恢复 |

### 4.5 持久化测试（persistence_test.go）

| 用例 | 操作 | 预期 |
|------|------|------|
| `Persist_EmptyManager` | 新 Manager 直接 Persist | 成功（写入空 manifest） |
| `Persist_Load_FullCycle` | 创建 DB/Meas → 分配 Series → 注册 Shard → Persist → 新 Manager Load | 所有数据完全恢复 |
| `Persist_DirtyDetection` | 无变更时 Persist | 跳过写入（不产生临时文件） |
| `Persist_PartialFailure` | 中途 kill 进程（留下 .tmp 文件） | 下次 Load 自动清理 .tmp，数据为上次完整快照 |
| `Persist_CorruptFile` | series.json 被手动损坏 | Load 返回明确错误，不 panic |
| `Persist_LargeDataset` | 100,000 series → Persist → Load | 正确恢复，Load 时间 < 2s |
| `Close_AutoPersist` | 有脏数据时调用 Close | 自动 Persist，再次 Load 数据完整 |
| `Close_NoDirty` | 无脏数据时调用 Close | 不触发 Persist，正常关闭 |

### 4.6 并发安全测试（concurrent_test.go）

| 用例 | 操作 | 预期 |
|------|------|------|
| `Concurrent_SIDAllocation` | 100 goroutine 各分配 1000 series（不同 tags） | SID 总数 = 100,000，无重复，无 panic |
| `Concurrent_SameTagsAllocation` | 100 goroutine 对相同 tags 分配 SID | 所有 goroutine 返回相同 SID |
| `Concurrent_CatalogOps` | 并行创建/删除/列出不同 database | 无竞态，无 panic，状态一致 |
| `Concurrent_ShardRegister` | 100 goroutine 并行注册 shard | 所有 shard 正确注册，ListShards 返回完整列表 |
| `Concurrent_ReadWrite` | 读 goroutine 查询 + 写 goroutine 注册 | 读到一致快照，写操作不阻塞读 |
| `Concurrent_Persist` | Persist 期间并发 AllocateSID | 持久化完成后 dirty 标记正确，无数据丢失 |

### 4.7 全链路集成测试（integration_test.go）

| 用例 | 操作 | 预期 |
|------|------|------|
| `FullWriteReadCycle` | Manager → 创建 DB/Meas → 写入 1000 points → 查询 | 数据完整，tags 正确，schema 匹配 |
| `RestartRecovery` | 写入 10,000 points → 正常关闭 → 重启 Engine → 查询 | 所有数据可恢复，SID 延续，tags 正确 |
| `KillRecovery` | 写入 10,000 points → kill -9 → 重启 Engine | WAL replay 恢复数据，Manager 状态与数据一致 |
| `RetentionCleanup` | 写入 3 个 shard（跨 3 天） → 设置 retention=1 天 → 触发清理 | 前 2 天 shard 的元数据和数据都被删除 |
| `MultiMeasurement` | 同一 DB 下写入 `cpu` 和 `memory` measurement | 各自 SID 独立，各自 schema 独立，查询互不干扰 |
| `TagFilterQuery` | 写入不同 tags 的 series → 按 tag 过滤查询 | 返回正确的 series，不匹配的不出现 |

### 4.8 迁移测试（migration_test.go）

| 用例 | 操作 | 预期 |
|------|------|------|
| `Migrate_OldJsonFormat` | 使用旧 `meta.json` 文件 → 运行迁移 | 数据完整迁移到 `_metadata/` 下 |
| `Migrate_Idempotent` | 对已迁移数据再次迁移 | 跳过不重复迁移，不丢数据 |
| `Migrate_MissingSource` | 源文件不存在 | 返回可忽略的错误 |
| `Migrate_CorruptSource` | 源文件 JSON 格式损坏 | 返回明确错误，不会部分迁移 |

### 执行方式

```bash
# 编译并执行所有 metadata e2e 测试
cd tests/e2e/metadata && go build && ./metadata

# 或单独执行某一类
./metadata -test catalog      # 只跑 catalog 用例
./metadata -test series       # 只跑 series 用例
./metadata -test persistence  # 只跑持久化用例
```

### 验收标准

- 全部 50+ 测试用例通过
- golangci-lint 0 issues
- 测试后自动清理临时数据目录
- Race detector: `go test -race` 无竞态报告

---

## 五、对比总结

| 维度 | 当前架构 | 新架构 |
|------|----------|--------|
| 入口 | Engine.dbMetaStores + ShardManager.metaStores | `metadata.Manager` 统一入口 |
| 接口 | 无（`MetaStore` 死接口） | `Catalog` / `SeriesStore` / `ShardIndex` 三大接口 |
| 持久化 | 双格式（二进制+JSON），散落各处 | 统一 JSON + atomic write，集中 `_metadata/` |
| 目录布局 | 元数据与数据混放（meta.json 在数据目录下） | `_metadata/` 物理分离，可独立备份 |
| Database 元数据 | 纯内存，重启丢失 | manifest.json 持久化 |
| Schema | 分散在 proto + SSTable | 统一管理，写入时可校验，版本化演进 |
| Shard 元数据 | 无全局视图 | ShardIndex 集中管理时间窗口索引 |
| WAL replay | 依赖 MetaStore 内存状态 | 显式 Load → replay → Persist 顺序保证 |
| 可测试性 | 紧耦合具体实现 | 接口隔离，可 mock，e2e 全覆盖 |
| 死代码 | MemoryMetaStore + MetaStore 接口 | 全部移除 |
