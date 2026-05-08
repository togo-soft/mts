# MTS 元数据系统 Bolt 重设计方案

日期：2026-05-08

## 1. 动机

当前元数据系统将数据分散在多个小 JSON 文件中（`manifest.json`、`schema.json`、`series.json`、`shards.json`），文件数量随 database/measurement 线性增长。持久化使用 `tmp + fsync + rename` 原子写入模式，缺乏事务支持，多文件写入可能不一致。

本方案用 etcd 使用的 bbolt 存储引擎（`go.etcd.io/bbolt`）替代所有 JSON 文件，实现单文件存储、ACID 事务、崩溃恢复，同时保持三层接口（Catalog、SeriesStore、ShardIndex）不变。

## 2. 架构概览

```
当前:
  {dataDir}/
    _metadata/
      manifest.json
      {db}/{meas}/
        schema.json
        series.json
        shards.json

重构后:
  {dataDir}/
    metadata.db    ← 单个 bbolt 文件，包含所有元数据
```

### 混合缓存策略

- 高频读路径（GetTags、GetSIDsByTag）→ 内存缓存 + RLock，微秒级
- 所有写路径 → 直接 bbolt 写事务，写完更新内存缓存
- 低频读路径（ListDatabases、GetSchema、QueryShards）→ bbolt 读事务，无需额外缓存
- 启动恢复 → 遍历 bbolt Bucket 树重建内存缓存

### 旧代码清理

`measurement.MeasurementMetaStore` 和 `measurement.DatabaseMetaStore` 的全部持久化和内存管理逻辑被 bbolt 替代，这两个类型将被移除。`persist.go`（atomicWrite/atomicRead）和 `migrate.go` 也将移除。项目处于开发阶段，不考虑旧格式迁移。

## 3. Bucket/Key 设计

### Bucket 层级树

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
 │   │   │   │   └─ {sid_be_8bytes}         ← Key → empty (存在即关联)
 │   │   │   └─ ...
 │   │   └─ "shards"               ← Bucket
 │   │       └─ {shard_id}         ← Key → JSON(ShardInfo)
 │   └─ {meas2} ...
 └─ {db2} ...
```

### Key 编码说明

| 数据 | Key 编码 | Value 编码 | 说明 |
|------|---------|-----------|------|
| Schema | `_schema` | JSON | 低频读写 |
| Retention | `_retention` | varint (8 bytes) | 简单数值 |
| Created time | `_created` | varint (8 bytes) | db/meas 注册时间 |
| Series tags | `{sid_be}` | JSON `{"k":"v",...}` | SID 用 BigEndian 编码 |
| Next SID | `_next_sid` | varint | 自增计数器 |
| Tag 倒排 | `{key}\x00{val}` prefix, `{sid_be}` sub-key | 空 | 按 tag 查 SID 列表 |
| Shard info | `{shard_id}` | JSON | shard_id 即 `{start}_{end}` |

以 `_` 开头的 key 为元数据键，与数据键区分。`_created` 存在即表示 database/measurement 已注册。`ListDatabases()` 遍历 root 下所有非 `_` 开头的 Bucket name。`DropDatabase()` 调用 `DeleteBucket()` 递归删除整个子树。

## 4. 核心组件

### Manager

```go
type Manager struct {
    catalog    *catalogStore
    series     *seriesStore
    shardIndex *shardIndex
    db         *bolt.DB       // 唯一数据源
    mu         sync.RWMutex
    closeOnce  sync.Once
}
```

生命周期：
- `New(dataDir)` → 打开/创建 `{dataDir}/metadata.db`
- `Load()` → 从 bbolt 重建 seriesStore 内存缓存
- `Persist()` → 变更为 `Sync()`，调用 `bolt.DB.Sync()` 强制 fsync
- `Close()` → 关闭 bbolt DB

### Catalog（catalogStore）

- 每次操作开启 bbolt 写/读事务，直接操作 bucket key
- 不再需要内存 manifest、dirty 标记、atomicWrite

### SeriesStore（seriesStore）

```go
type seriesStore struct {
    db       *bolt.DB
    cache    sync.Map  // key: "db/meas/{sid}" → map[string]string
    nextSIDs sync.Map  // key: "db/meas" → uint64
}
```

- `AllocateSID`：bbolt 写事务 → 递增 `_next_sid` → 写 series bucket → 写 tag_index bucket → 更新内存缓存
- `GetTags`：先查 `cache.Load()`，miss 则 bbolt 读事务 → 填充缓存
- `GetSIDsByTag`：bbolt 读事务扫描 tag_index prefix，不缓存（低频操作）
- `SeriesCount`：bbolt 读事务统计 series bucket 的 key 数量（不含 `_next_sid`）

### ShardIndex（shardIndex）

- 写事务操作 shards bucket，读事务直接遍历 bucket 获取列表
- 不再需要内存 shards map、dirty 标记

## 5. 事务策略

| 操作类型 | 事务方式 | 说明 |
|---------|---------|------|
| 读 | `db.View()` | 只读事务，并发友好 |
| 单写 | `db.Update()` | 独立写事务，自动提交 |
| Close | `db.Close()` | 释放文件锁 |

不需要 `db.Batch()`（元数据写入无高频场景）。不需要显式 `Persist()`，bbolt 每次 Update 提交自动 fsync。bbolt 自身 MVCC + 序列化更新保证并发安全。

## 6. 错误处理

- bbolt 特有错误映射：`bolt.ErrBucketNotFound` → `ErrDatabaseNotFound` / `ErrMeasurementNotFound`，`bolt.ErrBucketExists` → `ErrDatabaseExists` / `ErrMeasurementExists`
- 所有 bbolt 操作返回的 error 必须处理（接受变量或显式 `_`）

## 7. 文件变更

### 接口不变
- `internal/storage/metadata/catalog.go`
- `internal/storage/metadata/series.go`
- `internal/storage/metadata/shard_index.go`

### 重写
- `internal/storage/metadata/catalog_impl.go`
- `internal/storage/metadata/series_impl.go`
- `internal/storage/metadata/shard_index_impl.go`
- `internal/storage/metadata/manager.go`

### 移除
- `internal/storage/metadata/persist.go`
- `internal/storage/metadata/migrate.go`
- `internal/storage/measurement/meas_meta.go`
- `internal/storage/measurement/meas_meta_query.go`
- `internal/storage/measurement/meas_meta_test.go`
- `internal/storage/measurement/meas_meta_bench_test.go`
- `internal/storage/measurement/meta_extra_test.go`
- `internal/storage/measurement/db_meta.go`
- `internal/storage/measurement/db_meta_test.go`

### 新增依赖
- `go.etcd.io/bbolt v1.4.3`（纯 Go，零传递依赖）

## 8. 测试策略

- 每个测试用例创建临时 bbolt DB 文件，`t.Cleanup()` 中关闭并清理
- 接口不变，测试用例结构基本保留，实现改为操作 bbolt
- 目标覆盖率：≥ 90%
- `engine_test.go`：dataDir 指向临时目录即可
- `tests/e2e/`：无需修改
