# micro-ts

高性能时序数据库，支持高吞吐写入和快速查询。

## 特性

- 内存跳表（MemTable）加速写入
- SSTable 持久化存储
- WAL 预写日志保证数据安全
- gRPC 接口
- 支持按时间范围查询

## 快速开始

```go
package main

import (
    "context"
    "log"
    "micro-ts"
)

func main() {
    db, err := microts.Open(microts.Config{
        DataDir: "/tmp/microts",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 写入数据点
    point := &microts.Point{
        Database:    "metrics",
        Measurement: "cpu",
        Tags:        map[string]string{"host": "server1"},
        Timestamp:   time.Now().UnixNano(),
        Fields:      map[string]any{"usage": 85.5},
    }

    if err := db.Write(context.Background(), point); err != nil {
        log.Fatal(err)
    }
}
```

## 性能基准测试

项目包含核心组件的性能基准测试，用于跟踪性能变化。

### 运行所有基准测试

```bash
go test -bench=. -benchmem ./internal/storage/...
```

### 特定组件测试

```bash
# MemTable 性能测试
go test -bench=. -benchmem ./internal/storage/shard

# WAL 序列化性能
go test -bench=. -benchmem ./internal/storage/shard -run=BenchmarkEncode

# SSTable 读取性能
go test -bench=. -benchmem ./internal/storage/shard/sstable

# Measurement MetaStore 性能
go test -bench=. -benchmem ./internal/storage/measurement
```

### 保存性能基线

```bash
# 保存当前性能基线
go test -bench=. -benchmem ./internal/storage/shard > benchmark/baseline_shard.txt
go test -bench=. -benchmem ./internal/storage/measurement > benchmark/baseline_meas.txt
```

### 性能指标说明

| 组件 | 关键指标 | 当前基线 |
|------|----------|----------|
| MemTable 写入 | 565 ns/op, 776 B/op, 5 allocs | 支持 177万+ tps |
| WAL 序列化 | 286 ns/op, 336 B/op, 8 allocs | 核心写入路径 |
| WAL 反序列化 | 584 ns/op, 1176 B/op, 21 allocs | 恢复场景 |

## 测试

```bash
# 运行所有测试
go test ./...

# 运行 e2e 测试
go run tests/e2e/write_1k/main.go
go run tests/e2e/query_1k/main.go
```

## License

MIT
