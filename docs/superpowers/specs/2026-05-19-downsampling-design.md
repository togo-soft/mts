# 时序数据降采样（Downsampling）设计文档

> **状态**: 已确认  
> **日期**: 2026-05-19  

## 目标

实现分层降采样功能，支持用户按 database 配置灵活的多级聚合规则，通过独立后台服务定期生成降采样数据，降低长期存储成本并加速大范围查询。

## 配置模型

### Proto 定义

```protobuf
message DownsampleRule {
  int64 window_nanos = 1;          // 聚合窗口（纳秒）
  repeated string functions = 2;   // 聚合函数: avg,max,min,sum,count,first,last
  int64 retention_nanos = 3;       // 该层级数据保留期（纳秒）
}

message DownsampleConfig {
  bool enabled = 1;                // 是否启用降采样
  int64 check_interval_nanos = 2;  // 扫描检查间隔（纳秒），默认 5 分钟
  repeated DownsampleRule rules = 3; // 降采样规则链（窗口从小到大）
}
```

### CreateDatabaseRequest 扩展

```protobuf
message CreateDatabaseRequest {
  string database = 1;
  int64 retention_period_nanos = 2;     // 原始数据保留期
  DownsampleConfig downsample_config = 3; // 降采样配置（可选）
}
```

### 配置示例

```
原始数据保留: 7 天
降采样规则:
  - window=5m, functions=[avg,max,min], retention=30d
  - window=1h, functions=[avg,max,min], retention=365d
```

效果：0-7天查原始数据，7-30天查5分钟聚合，30-365天查1小时聚合。

### 存储

bbolt database bucket 下新增 `_downsample` 键，存储序列化的 `DownsampleConfig`。Catalog 接口新增：

```go
GetDownsampleConfig(database string) (*DownsampleConfig, error)
SetDownsampleConfig(database string, cfg *DownsampleConfig) error
```

## 查询 API

`QueryRangeRequest` 新增字段：

```protobuf
int64 downsample_window_nanos = 9; // 0=原始数据，非0=指定降采样窗口
```

| 值 | 行为 |
|----|------|
| 0（默认） | 查询原始数据 |
| 300_000_000_000 | 查询 5m 降采样数据 |
| 3_600_000_000_000 | 查询 1h 降采样数据 |

调用方显式指定窗口，引擎不自动选择分辨率。如果指定窗口的降采样数据不存在，返回空结果。

## DownsampleService 架构

### 文件存储

降采样数据存储为独立 SSTable，复用现有格式：

```
{db}/{meas}/{shardStart}_{shardEnd}/
  ├── data/L0/...                ← 原始数据
  └── downsampled/
        ├── 300000000000/        ← 5m 降采样
        │     └── sst_N.bin
        └── 3600000000000/       ← 1h 降采样
              └── sst_N.bin
```

### 字段命名约定

原始字段 `usage` 聚合后输出 `avg_usage`、`max_usage`、`min_usage` 等。`count` 函数统计窗口内数据点数，输出为 `_count` 字段。

### 执行流程

```
DownsampleService (每 check_interval_nanos 触发)
  │
  ├─ 1. 遍历 catalog 中所有 database
  ├─ 2. 读取 DownsampleConfig，若 enabled=false 则跳过
  ├─ 3. 遍历该 database 下所有 measurement 的所有 Shard
  ├─ 4. 对每个 Shard+window 组合判断:
  │      ├─ 已完成降采样?（_downsample_done 标记） → 跳过
  │      ├─ Shard 完全在 raw retention 内? → 跳过
  │      └─ 超出 raw retention? → 执行降采样
  │
  └─ 5. 降采样流程:
         ├─ 读取 Shard 原始 SSTable（流式迭代器）
         ├─ 按 window 分桶，逐桶计算聚合函数
         ├─ 写入 downsampled/{window}/sst_N.bin
         └─ 写入 _downsample_done 标记文件
```

### 关键属性

- **幂等性**: `_downsample_done` 标记文件防止重复处理
- **流式处理**: 复用 ShardIterator 逐批读取，不一次性加载全 Shard
- **错误容忍**: 单 Shard 失败不影响其他 Shard，记录日志后继续
- **与 RetentionService 协调**: 原始数据等对应窗口降采样完成后才被删除

## 聚合函数

| 函数 | 输出字段名 | 说明 |
|------|-----------|------|
| avg | `avg_{field}` | 窗口内平均值 |
| max | `max_{field}` | 窗口内最大值 |
| min | `min_{field}` | 窗口内最小值 |
| sum | `sum_{field}` | 窗口内总和 |
| count | `_count` | 窗口内原始数据点数 |
| first | `first_{field}` | 窗口内第一个值 |
| last | `last_{field}` | 窗口内最后一个值 |

## 查询路径改造

Engine 查询时根据 `downsample_window_nanos` 定位到 `downsampled/{window}/` 目录，复用现有 SSTable Reader 和 ShardIterator 读取。

## 改动范围

| 层 | 文件 | 改动 |
|---|------|------|
| Proto | `proto/mts.proto` | 新增消息类型，扩展 Request |
| 类型生成 | `types/mts.pb.go` | 重新生成 |
| Catalog | `metadata/catalog.go` + `catalog_impl.go` | 新增 downsample config 读写 |
| Engine 接口 | `engine/interfaces.go` | Catalog 接口增加方法 |
| Engine | `engine_catalog.go` + `engine_query.go` | CreateDatabase 传参，查询支持降采样 |
| DownsampleService | **新建** `internal/storage/downsample/` | 核心降采样服务 |
| gRPC API | `internal/api/grpc.go` | 传递新参数 |
| 公共 API | `mts.go` | CreateDatabase + QueryRange 参数 |
| E2E 测试 | **新建** `tests/e2e/downsample_test/` | 端到端验证 |

## 不包含

- rate/irate/derivative/difference 等计算类函数（本次仅实现聚合类：avg/max/min/sum/count/first/last）
- 自动分辨率选择
- 跨 measurement 降采样
