# micro-ts 端到端测试套件设计规格

## 1. 概述

本规格定义 micro-ts 时序数据库的端到端测试套件，包括性能测试和数据完整性测试两个测试组。

### 1.1 测试文件结构

```
tests/
├── bench_test.go       # 性能测试 (1K, 10K, 100K, 1M)
└── integrity_test.go   # 数据完整性测试 (100K, 10 fields)
```

### 1.2 核心架构

```
microts.go (Library API)
└── Engine (存储引擎)
    ├── ShardManager
    │   └── Shard[]
    │       ├── WAL
    │       ├── MemTable
    │       └── SSTable
    └── MemoryMetaStore (per measurement)
```

---

## 2. 存储引擎实现

### 2.1 Engine 结构

```go
// Engine 存储引擎
type Engine struct {
    cfg           *Config
    shardManager  *shard.Manager
    metaStores    map[string]*measurement.MemoryMetaStore  // key: "db/measurement"
    mu            sync.RWMutex
}
```

### 2.2 Engine 接口

```go
// Write 写入单个点
func (e *Engine) Write(point *types.Point) error

// WriteBatch 批量写入
func (e *Engine) WriteBatch(points []*types.Point) error

// Query 范围查询
func (e *Engine) Query(req *types.QueryRangeRequest) (*types.QueryRangeResponse, error)

// ListMeasurements 列出 Measurement
func (e *Engine) ListMeasurements(database string) ([]string, error)

// Close 关闭
func (e *Engine) Close() error
```

### 2.3 数据目录结构

```
{DataDir}/
└── {database}/
    └── {measurement}/
        └── meta/                    # MeasurementMeta
        └── {shard_time_range}/     # Shard 数据
            ├── wal/
            └── data/
```

### 2.4 刷盘策略

MemTable 同步刷盘：每次 Write 后检查是否达到阈值（10000 条或 64MB），达到则刷盘。

---

## 3. 数据完整性测试

### 3.1 测试规格

| 项目       | 值            |
|----------|--------------|
| 数据规模     | 100,000 条数据点 |
| Field 数量 | 10 个         |

### 3.2 Field 定义

| Field 名        | 类型      | 生成方式             |
|----------------|---------|------------------|
| field_float_1  | float64 | 伪随机 [0, 1000)    |
| field_float_2  | float64 | 伪随机 [0, 1000)    |
| field_float_3  | float64 | 伪随机 [0, 1000)    |
| field_float_4  | float64 | 伪随机 [0, 1000)    |
| field_float_5  | float64 | 伪随机 [0, 1000)    |
| field_int_1    | int64   | 伪随机 [0, 100000)  |
| field_int_2    | int64   | 伪随机 [0, 100000)  |
| field_int_3    | int64   | 伪随机 [0, 100000)  |
| field_string_1 | string  | 伪随机字符串 (长度 8-16) |
| field_bool_1   | bool    | 伪随机              |

### 3.3 数据生成策略

- **Timestamp**: 递增，步长 1 秒
- **Tags**: 固定 `host=server1`
- **Fields**: 使用固定种子 (42) 的 math/rand，生成可复现数据

### 3.4 校验方式

```go
// 写入后读取，逐字段校验
func verifyPointRow(t *testing.T, expected *types.Point, actual *types.PointRow) {
    if expected.Timestamp != actual.Timestamp {
        t.Errorf("timestamp mismatch: expected %d, got %d", expected.Timestamp, actual.Timestamp)
    }
    for k, v := range expected.Fields {
        if actual.Fields[k] != v {
            t.Errorf("field %s mismatch: expected %v, got %v", k, v, actual.Fields[k])
        }
    }
}
```

### 3.5 测试用例

```go
func TestIntegrity_100K_Points(t *testing.T) {
    // 1. 生成 100K 数据点
    // 2. 写入数据库
    // 3. 读取所有数据
    // 4. 逐字段逐值校验
}
```

---

## 4. 性能测试

### 4.1 数据规模

| 规模   | 数据点数      |
|------|-----------|
| 1K   | 1,000     |
| 10K  | 10,000    |
| 100K | 100,000   |
| 1M   | 1,000,000 |

### 4.2 性能指标

| 指标     | 说明                         | 测量方式                     |
|--------|----------------------------|--------------------------|
| 写入 TPS | 每秒写入点数                     | 写入耗时 / 数据量               |
| 写入内存增量 | Write 前后 MemStats.Alloc 差值 | `runtime.ReadMemStats()` |
| 写入磁盘大小 | 数据目录大小                     | `dirSize()` 遍历文件         |
| 查询耗时   | QueryRange 端到端延迟           | `time.Since()`           |
| 查询内存增量 | Query 前后 MemStats.Alloc 差值 | `runtime.ReadMemStats()` |

### 4.3 测试用例

```go
func BenchmarkWrite_1K(b *testing.B)
func BenchmarkWrite_10K(b *testing.B)
func BenchmarkWrite_100K(b *testing.B)
func BenchmarkWrite_1M(b *testing.B)

func BenchmarkQuery_1K(b *testing.B)
func BenchmarkQuery_10K(b *testing.B)
func BenchmarkQuery_100K(b *testing.B)
func BenchmarkQuery_1M(b *testing.B)
```

### 4.4 测试流程

```
1. 强制 GC: runtime.GC()
2. 记录 MemStats
3. 执行写入/查询操作
4. 记录 MemStats
5. 计算差值
6. 清理数据
```

---

## 5. 测试环境要求

### 5.1 目录权限

- 测试数据目录：使用 `t.TempDir()`
- 目录权限：0700
- 文件权限：0600

### 5.2 清理策略

每个测试用例使用独立的临时目录，测试完成后自动清理：

```go
func TestMain(m *testing.M) {
    os.Exit(m.Run())
    // 清理已在 t.TempDir() 中自动完成
}
```

---

## 6. 实现计划

### Phase 1: 存储引擎实现

1. 创建 `internal/storage/engine.go`
2. 实现 `Engine` 结构和接口
3. 连接 ShardManager、MetaStore
4. 实现同步刷盘逻辑

### Phase 2: 数据完整性测试

1. 创建 `tests/` 目录
2. 实现数据生成器
3. 实现 `integrity_test.go`
4. 验证 100K 数据读写正确性

### Phase 3: 性能测试

1. 实现性能指标测量工具
2. 实现 `bench_test.go`
3. 运行基准测试，输出性能报告

---

## 7. 验收标准

### 7.1 数据完整性测试

- [ ] 100K 数据点写入后读取完全一致
- [ ] 所有 10 个字段类型正确
- [ ] Timestamp 递增有序

### 7.2 性能测试

- [ ] 1K 写入 TPS > 10,000
- [ ] 1M 写入 TPS > 50,000
- [ ] 100K 查询耗时 < 500ms
- [ ] 内存测量数据有效（非零）

### 7.3 测试环境

- [ ] 测试数据目录测试后清理
- [ ] 无临时文件残留
