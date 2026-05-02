# micro-ts 时序数据库设计规格

## 1. 概述

micro-ts 是一个高性能的时序数据库（Time Series Database），采用多值模型和 Columnar LSM-tree 存储引擎，在高写入吞吐和高效范围查询之间取得平衡。

### 1.1 核心特性

- **多值模型（Multi-value Model）**：同一时间点可包含多个字段值
- **Columnar LSM-tree 存储引擎**：列式存储，优化压缩和查询
- **高写入吞吐**：Multi-Writer Shard 分片，支持水平扩展
- **高效范围查询**：支持字段映射投影读取

### 1.2 技术栈

- 语言：Go 1.26
- API：gRPC
- 存储：内存映射（mmap）+ 无压缩（后续可扩展压缩）
- 并发：Multi-Writer Shard 分片

---

## 2. 数据模型

### 2.1 层级结构

```
Database
  └── Measurement（指标组，如 "cpu", "memory"）
        ├── Tags（标签，索引用）
        ├── Timestamp（纳秒时间戳）
        └── Fields（多个字段值）
```

### 2.2 数据类型

```go
// Point 是写入的基本单位
// Fields 使用 map[string]any，通过类型断言区分真正的数据类型
type Point struct {
    Database    string
    Measurement string
    Tags        map[string]string
    Timestamp   int64         // 纳秒
    Fields      map[string]any // 字段值，支持 int64/float64/string/bool
}
```

### 2.3 字段类型

| 类型        | Go 类型   | 存储格式                                |
|-----------|---------|-------------------------------------|
| Timestamp | int64   | Delta-of-Delta + varint（Gorilla 风格） |
| Int64     | int64   | Delta + varint                      |
| Float64   | float64 | Gorilla XOR 编码                      |
| String    | string  | 字典编码 + LZ4（重复率高的场景优化）               |
| Bool      | bool    | 位图（Bit Array）                       |

---

## 3. 存储架构

### 3.1 分片策略

按 `(Database, Measurement, TimeRange)` 三元组分片：

- 时间范围分片：每个 Shard 覆盖固定时间窗口（默认 1 小时）
- Shard 命名：`{database}/{measurement}/{start_time}_{end_time}`

**示例：**
```
data/
├── db1/
│   └── cpu/
│       ├── meta/                 # Measurement 级元信息
│       │   ├── meta.pb          # MeasurementMeta
│       │   ├── series.bin       # Series 数据
│       │   └── tag_index.bin    # Tag 倒排索引
│       ├── 1709424000000000000_1709427600000000000/  # Shard 1
│       │   ├── wal/
│       │   └── data/
│       └── 1709427600000000000_1709431200000000000/  # Shard 2
│           ├── wal/
│           └── data/
```

**元数据布局优化：**
- `meta/` 目录放在 Measurement 层，而非 Shard 层
- 优势：series 信息不重复，sid 全局管理，跨 shard 查询更简单

### 3.2 存储布局

**整体结构：**
```
measurement/
├── meta/                       # 元信息存储（MetaStore 接口）
│   ├── meta.pb               # MeasurementMeta
│   ├── series.bin           # Series 数据
│   └── tag_index.bin        # Tag 倒排索引
└── {shard_time_range}/       # Shard 数据目录
    ├── wal/                  # WAL（Write-Ahead Log）
    └── data/                 # SSTable 列式存储
```

**SSTable（列式存储文件）即 `data/` 目录下的文件：**

```
shard/
└── data/
    ├── _timestamps.bin       # 时间戳列（SSTable）
    ├── _sids.bin            # Series ID 列（SSTable）
    └── fields/
        ├── {field_key}.bin  # 字段列（SSTable）
        └── {field_key}.idx  # 字段索引
```

**说明：**
- SSTable = Structured Storage Table，列式存储的物理文件格式
- `data/` 目录下的每个列文件就是一个 SSTable
- flush 时 MemTable 数据排序后写入 SSTable
- mmap 访问，操作系统按需加载页面

**列式文件格式（自定义）：**

每个 `.bin` 文件采用**块结构**，支持 mmap 高效随机访问：

```
┌─────────────────────────────────────────────────────────┐
│                     .bin 文件结构                        │
├─────────────────────────────────────────────────────────┤
│ [8 bytes: magic]     魔数，固定值 0x5453455250454346    │
│ [4 bytes: version]   版本号，当前为 1                   │
│ [4 bytes: block_count] 数据块数量                       │
│ [8 bytes: min_value] 该列最小值（用于索引）              │
│ [8 bytes: max_value] 该列最大值（用于索引）              │
├─────────────────────────────────────────────────────────┤
│                    Block 1                              │
│ [4 bytes: block_size]   该块数据大小                     │
│ [N bytes: data]         压缩后的列数据                   │
├─────────────────────────────────────────────────────────┤
│                    Block 2                              │
│ [4 bytes: block_size]   该块数据大小                     │
│ [N bytes: data]         压缩后的列数据                   │
├─────────────────────────────────────────────────────────┤
│                      ...                                │
├─────────────────────────────────────────────────────────┤
│ [4 bytes: index_offset]  索引块在文件中的偏移量           │
└─────────────────────────────────────────────────────────┘
```

**各列数据类型压缩编码：**

| 类型        | 压缩编码                    | 说明               |
|-----------|-------------------------|------------------|
| Timestamp | Delta-of-Delta + varint | Gorilla 风格时间戳压缩  |
| Int64     | Delta + varint          | 存储差值，用变长编码       |
| Float64   | Gorilla XOR             | 相邻值 XOR 后压缩      |
| String    | 字典编码 + LZ4              | 重复字符串用字典索引       |
| Bool      | 位图                      | 每 8 个值打包成 1 byte |
| Sid       | Delta + varint          | 同 Int64          |

**索引文件（.idx）结构：**

```
┌─────────────────────────────────────────────────────────┐
│                     .idx 文件结构                        │
├─────────────────────────────────────────────────────────┤
│ [4 bytes: version]      版本号，当前为 1                 │
│ [8 bytes: min_value]    列最小时间戳（纳秒）              │
│ [8 bytes: max_value]    列最大时间戳（纳秒）              │
│ [4 bytes: row_count]    总行数                          │
├─────────────────────────────────────────────────────────┤
│                    Block Index                          │
│ [block_count × 20 bytes]                               │
│   [8 bytes: first_value]  该块第一个值                    │
│   [8 bytes: last_value]  该块最后一个值                  │
│   [4 bytes: offset]      该块在 .bin 文件中的偏移量       │
├─────────────────────────────────────────────────────────┤
│ [4 bytes: index_size]   索引块大小                       │
└─────────────────────────────────────────────────────────┘
```

**二分查找定位行：**
1. 加载 .idx 文件的 Block Index 到内存
2. 根据查询值二分查找目标 block
3. 从 .bin 文件读取对应 block
4. 在 block 内线性扫描找到目标行

**Block 大小：** 默认 64KB，平衡 mmap 和查找效率

**元数据存储（MetaStore 接口 + 内存实现）：**

```go
// MetaStore 元数据存储接口（可扩展对接其他存储中间件）
type MetaStore interface {
    // 获取 Measurement 元信息
    GetMeta(ctx context.Context) (*MeasurementMeta, error)
    // 更新 Measurement 元信息
    SetMeta(ctx context.Context, meta *MeasurementMeta) error
    
    // 根据 sid 获取 tags
    GetSeries(ctx context.Context, sid uint64) ([]byte, error)  // 二进制格式
    // 添加或更新 series
    SetSeries(ctx context.Context, sid uint64, tags []byte) error
    // 获取所有 series
    GetAllSeries(ctx context.Context) (map[uint64][]byte, error)
    
    // 获取 tag 对应的所有 sid
    GetSidsByTag(ctx context.Context, tagKey, tagValue string) ([]uint64, error)
    // 添加 tag -> sid 映射
    AddTagIndex(ctx context.Context, tagKey, tagValue string, sid uint64) error
    
    // 持久化到磁盘
    Persist(ctx context.Context) error
    // 从磁盘加载
    Load(ctx context.Context) error
    // 关闭
    Close() error
}

// 默认内存实现（MemoryMetaStore）
type MemoryMetaStore struct {
    mu       sync.RWMutex
    meta     *MeasurementMeta
    series   map[uint64][]byte  // sid -> tags 二进制
    tagIndex map[string][]uint64  // tagKey\x00tagValue -> sids
    dirty    bool
}
```

**文件格式：**

| 文件              | 内容              | 格式       |
|-----------------|-----------------|----------|
| `meta.pb`       | MeasurementMeta | Protobuf |
| `series.bin`    | 所有 series 数据    | 自定义二进制   |
| `tag_index.bin` | tag 倒排索引        | 自定义二进制   |

**Series 二进制格式：**
```
[1 byte: tag_count][klen][key_bytes][vlen][value_bytes][klen][key_bytes][vlen][value_bytes]...
```
- 示例：`02 04 host 07 server1 06 region 02 us` 表示 2 个 tag
- 优点：紧凑、顺序解析 O(n)、无冗余

**Tag 倒排索引格式：**
```
[key_size (varint)][key_bytes][value_size (varint)][value_bytes]...
```
- Key 示例：`host\x00server1`
- Value：`02 00 00 00 05 00 00 00 0A`（5个sids: 2, 10）

**持久化策略：**
- 内存优先，所有操作在内存中完成
- 后台 goroutine 定期将脏数据（dirty）刷盘
- 服务启动时从磁盘加载
- 可通过配置禁用持久化（纯内存模式）

**扩展其他存储：**
实现 `MetaStore` 接口即可对接任意存储：
- Redis、BadgerDB、LevelDB、RocksDB 等
- 实现者只需替换初始化时的 store 实例

### 3.3 列式存储优势

1. **字段映射查询**：只读取请求的字段列，减少 IO
2. **压缩效率高**：同类型数据连续存储，压缩率高
3. **向量化计算**：列式数据利于 SIMD 加速聚合

### 3.4 元信息格式（MeasurementMeta）

```protobuf
message MeasurementMeta {
    int64 version = 1;
    // database/measurement 由路径保证，不重复存储
    FieldSchema field_schema = 2;       // 字段 schema
    repeated string tag_keys = 3;        // tag 键列表
    int64 next_sid = 4;                 // 下一个可用的 sid
}

// 自定义 FieldSchema（避免 protobuf map 开销）
message FieldSchema {
    repeated FieldDef fields = 1;        // 变长数组，更紧凑
}

message FieldDef {
    string name = 1;
    ValueType type = 2;                 // 枚举类型，1 byte
}

enum ValueType {
    INT64 = 0;
    FLOAT64 = 1;
    STRING = 2;
    BOOL = 3;
}
```

**优化说明：**
- 移除 `database`/`measurement`（由路径保证）
- 移除 `total_row_count`（各 shard 独立维护，汇总查询时计算）
- 使用 `FieldSchema` 替代 `map<string, string>`，更紧凑
- 使用枚举 `ValueType` 替代字符串类型，更省空间

---

## 4. 写入路径

### 4.1 写入流程

```
写入请求 → Shard Router → WAL → MemTable → (flush) → SSTable
                      │
                      └── 内存中按 (measurement + timestamp) 排序
```

### 4.2 Multi-Writer 模型

- 每个 Shard 拥有独立的写入 goroutine
- 写入请求通过 channel 发送给对应 Shard
- 跨 Shard 写入完全并行，无锁竞争

### 4.3 Write-Ahead Log (WAL)

- 路径：`{shard}/wal/{sequence}.log`
- 内容：Protocol Buffer 编码的原始写入请求
- 刷盘策略：每条写入后立即 fsync（可配置异步模式）
- 崩溃恢复：从 WAL 重放未刷入 SSTable 的数据

### 4.4 MemTable

- 内存结构：按 (measurement + timestamp) 排序的跳表
- 阈值：达到 10000 条或 64MB 时触发 flush
- flush 过程：排序 → 生成 SSTable → 清空 MemTable

---

## 5. 查询路径

### 5.1 查询接口

```go
// PointRow 是查询结果的一行
type PointRow struct {
    Timestamp int64            // 纳秒
    Tags      map[string]string // 通过 sid 反查的 tag 值
    Fields    map[string]any   // 字段值
}

// QueryRangeRequest 定义范围查询
type QueryRangeRequest struct {
    Database    string
    Measurement string
    StartTime   int64    // 纳秒
    EndTime     int64    // 纳秒
    Fields      []string // 字段映射：只返回指定字段
    Tags        map[string]string // tag 过滤
    Offset      int64   // 分页偏移量
    Limit       int64   // 分页限制，0 表示不限制
}

// QueryRangeResponse 返回查询结果
type QueryRangeResponse struct {
    Database    string
    Measurement string
    StartTime   int64
    EndTime     int64
    TotalCount  int64   // 符合条件的数据总条数
    HasMore     bool    // 是否还有更多数据
    Rows        []PointRow
}
```

### 5.2 查询流程

```
QueryRangeRequest
    │
    ├── 1. 时间范围定位
    │      计算与请求范围相交的 Shard 列表
    │
    ├── 2. Tag 过滤（可选）
    │      ├── 有 Tags 条件 → 查 BoltDB tag_index bucket → 获取 sid 列表
    │      └── 无 Tags 条件 → 跳过
    │
    ├── 3. 并发读取 Shard 数据
    │      对每个相交的 Shard 并发执行：
    │      ├── mmap 加载 .idx 文件的 Block Index
    │      ├── 二分查找定位目标时间范围的 block
    │      ├── mmap 读取 .bin 文件对应 block
    │      ├── 根据 sid 过滤（如果有 tag 条件）
    │      └── 应用字段投影
    │
    ├── 4. 结果合并
    │      按时间排序合并多 Shard 结果
    │
    └── 5. 分页处理
           应用 Offset + Limit
```

### 5.3 跨 Shard 查询

跨 Shard 查询时（如时间范围跨越多个 Shard）：

1. 并发读取所有相交的 Shard
2. 收集每个 Shard 返回的 `PointRow`
3. 按 `Timestamp` 全局排序
4. 应用分页（Offset + Limit）

---

## 6. API 设计

### 6.1 gRPC 服务定义

```protobuf
syntax = "proto3";

package microts.v1;

service MicroTS {
    // 写入单个数据点
    rpc Write(WriteRequest) returns (WriteResponse);

    // 批量写入
    rpc WriteBatch(WriteBatchRequest) returns (WriteBatchResponse);

    // 范围查询
    rpc QueryRange(QueryRangeRequest) returns (QueryRangeResponse);

    // 列出所有 Measurement
    rpc ListMeasurements(ListMeasurementsRequest) returns (ListMeasurementsResponse);

    // 健康检查
    rpc Health(HealthRequest) returns (HealthResponse);
}
```

### 6.2 消息定义

```protobuf
// 写入请求
message WriteRequest {
    string database = 1;
    string measurement = 2;
    map<string, string> tags = 3;
    int64 timestamp = 4;  // 纳秒
    map<string, FieldValue> fields = 5;
}

// 批量写入请求
message WriteBatchRequest {
    repeated WriteRequest points = 1;
}

// 写入响应
message WriteResponse {
    bool success = 1;
    string error = 2;
}

// 字段值（oneof 避免类型冲突）
message FieldValue {
    oneof value {
        int64 int_value = 1;
        double float_value = 2;
        string string_value = 3;
        bool bool_value = 4;
    }
}

// 范围查询请求
message QueryRangeRequest {
    string database = 1;
    string measurement = 2;
    int64 start_time = 3;  // 纳秒
    int64 end_time = 4;    // 纳秒
    repeated string fields = 5;  // 字段映射
    map<string, string> tags = 6;  // tag 过滤
    int64 offset = 7;  // 分页偏移量
    int64 limit = 8;   // 分页限制，0 表示不限制
}

// 范围查询响应
message QueryRangeResponse {
    string database = 1;
    string measurement = 2;
    int64 start_time = 3;
    int64 end_time = 4;
    int64 total_count = 5;  // 符合条件的数据总条数
    bool has_more = 6;      // 是否还有更多数据
    repeated Row rows = 7;  // 查询结果
}

// 一行数据
message Row {
    int64 timestamp = 1;           // 纳秒
    map<string, string> tags = 2; // tag 值
    map<string, FieldValue> fields = 3; // 字段值
}

// 列出 Measurement 请求
message ListMeasurementsRequest {
    string database = 1;
}

// 列出 Measurement 响应
message ListMeasurementsResponse {
    repeated string measurements = 1;
}

// 健康检查请求
message HealthRequest {
}

// 健康检查响应
message HealthResponse {
    bool healthy = 1;
    string version = 2;
}
```

---

## 7. 目录结构

```
micro-ts/
├── cmd/
│   └── server/
│       └── main.go           # gRPC 服务入口
├── microts.go                # Library 入口（对外 API）
├── internal/
│   ├── api/
│   │   ├── grpc.go          # gRPC 服务实现
│   │   └── pb/              # 生成的 protobuf 代码
│   ├── storage/
│   │   ├── engine.go        # 存储引擎入口
│   │   ├── measurement/     # Measurement 级元数据
│   │   │   ├── meta.go    # MemoryMetaStore 实现
│   │   │   └── store.go   # MetaStore 接口定义
│   │   └── shard/
│   │       ├── manager.go   # Shard 管理器
│   │       ├── shard.go    # 单个 Shard
│   │       ├── memtable.go # MemTable 实现
│   │       └── wal.go     # WAL 实现
│   ├── query/
│   │   └── executor.go     # 查询执行器
│   └── types/
│       └── types.go        # 核心类型定义
├── data/                    # 数据目录（运行时创建）
├── docs/
│   └── specs/
│       └── 2026-05-02-micro-ts-design.md
├── go.mod
├── go.sum
└── README.md
```

## 8. 使用方式

### 8.1 服务模式（gRPC）

启动 gRPC 服务，独立运行：

```go
// cmd/server/main.go
func main() {
    server := microts.NewServer(microts.Config{
        DataDir: "./data",
    })
    server.ListenAndServe(":50051")
}
```

### 8.2 库模式（Library）

直接嵌入到其他服务中调用：

```go
// 作为库使用
db, err := microts.Open(microts.Config{
    DataDir: "./data",
})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 写入
err = db.Write(context.Background(), &microts.Point{
    Database:    "db1",
    Measurement: "cpu",
    Tags:        map[string]string{"host": "server1"},
    Timestamp:   time.Now().UnixNano(),
    Fields:      map[string]any{"usage": 85.5},
})

// 查询
resp, err := db.QueryRange(context.Background(), &microts.QueryRangeRequest{
    Database:    "db1",
    Measurement: "cpu",
    StartTime:   startNano,
    EndTime:     endNano,
    Fields:      []string{"usage"},
    Limit:       100,
})
```

**Library API（microts.go）：**

| 方法                                                                    | 说明             |
|-----------------------------------------------------------------------|----------------|
| `Open(cfg Config) (*DB, error)`                                       | 打开数据库          |
| `DB.Write(ctx, *Point) error`                                         | 写入单个点          |
| `DB.WriteBatch(ctx, []*Point) error`                                  | 批量写入           |
| `DB.QueryRange(ctx, *QueryRangeRequest) (*QueryRangeResponse, error)` | 范围查询           |
| `DB.ListMeasurements(ctx, string) ([]string, error)`                  | 列出 Measurement |
| `DB.Close() error`                                                    | 关闭数据库          |

---

## 9. 文件权限

**安全原则：对其他人（others）不允许任何操作。**

| 类型    | 权限   | Owner | Group | Others | 说明         |
|-------|------|-------|-------|--------|------------|
| 目录    | 0700 | rwx   | ---   | ---    | 只有所有者可读写执行 |
| 普通文件  | 0600 | rw-   | ---   | ---    | 只有所有者可读写   |
| 可执行文件 | 0700 | rwx   | ---   | ---    | 只有所有者可执行   |

**权限说明：**
- `0xxx` 格式：八进制表示
- `0700` = `rwx------`（owner: 读/写/执行，group/others: 无权限）
- `0600` = `rw-------`（owner: 读/写，group/others: 无权限）
- `0700` = `rwx------`（可执行文件，owner: 读/写/执行，group/others: 无权限）

---

## 10. 错误处理

### 10.1 错误分类

| 错误类型                   | 说明              | 处理方式             |
|------------------------|-----------------|------------------|
| ErrDatabaseNotFound    | 数据库不存在          | 返回错误，提示创建        |
| ErrMeasurementNotFound | Measurement 不存在 | 返回空结果            |
| ErrTimeRangeInvalid    | 时间范围无效          | 返回错误，start < end |
| ErrFieldTypeMismatch   | 字段类型冲突          | 返回错误，同一字段需同类型    |
| ErrShardNotFound       | Shard 不存在       | 返回空结果            |

### 10.2 错误码

```go
const (
    ErrCodeOK           = 0
    ErrCodeNotFound     = 404
    ErrCodeInvalidArg   = 400
    ErrCodeInternal     = 500
)
```

---

## 11. 后续扩展

### 11.1 可选功能（后续实现）

1. **压缩支持**：ZSTD/Snappy 列压缩
2. **Compaction**：Tiered Compaction 合并旧 SSTable
3. **TTL**：数据过期自动删除
4. **副本复制**：Raft 协议支持
5. **查询聚合**：内置 avg/min/max/count 等聚合

### 11.2 性能调优参数

```go
type Config struct {
    // Shard 时间窗口，最小 1 小时（防止小窗口导致过多文件）
    // 默认 1 小时，单位纳秒
    ShardDuration time.Duration

    // MemTable 刷盘阈值（最小 1MB）
    // 默认 64MB
    MemTableSize int64

    // WAL 刷盘策略："always" | "periodic" | "async"
    // 默认 "always"（最高可靠性）
    WALSyncPolicy string

    // 数据目录
    DataDir string
}
```

---

## 12. 验收标准

### 12.1 功能验收

- [ ] 支持单点写入和批量写入
- [ ] 支持按时间范围查询
- [ ] 支持字段映射（只查询指定字段）
- [ ] 支持 Tag 过滤
- [ ] 支持多值模型（同一时间点多个字段）
- [ ] 支持多 Database 和 Measurement
- [ ] 支持分页查询（Offset + Limit）
- [ ] 崩溃后能从 WAL 恢复

### 12.2 性能验收

- [ ] 单 Shard 写入 QPS > 100,000
- [ ] 范围查询延迟 < 100ms（100万条数据）
- [ ] 字段投影减少 50%+ 不必要 IO

### 12.3 代码质量

- [ ] 测试覆盖率 >= 90%
- [ ] 通过 golangci-lint 检查
- [ ] 通过 goimports-reviser 格式化
