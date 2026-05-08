# micro-ts

[![Go Version](https://img.shields.io/badge/go-1.26+-00ADD8.svg)](https://golang.org/)
[![Go Report Card](https://goreportcard.com/badge/codeberg.org/micro-ts/mts)](https://goreportcard.com/report/codeberg.org/micro-ts/mts)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> 高性能微时序数据库，专为高吞吐写入和快速时间范围查询设计。

## 特性

- ⚡ **高性能写入**：内存跳表（MemTable）实现亚微秒级写入延迟
- 💾 **分层存储**：SSTable 持久化存储，支持高效磁盘检索
- 🛡️ **数据安全**：WAL 预写日志保证崩溃恢复能力
- 🗄️ **元数据管理**：bbolt 嵌入式 KV 存储，ACID 事务保证一致性
- 🔌 **gRPC 接口**：高性能远程访问接口
- 🔍 **时间范围查询**：支持纳秒级精度的时间范围查询
- 🧪 **基准测试验证**：通过完整性能基准测试，写入性能达 177万+ tps

## 架构

```
┌─────────────────────────────────────────────────────────┐
│                      gRPC API Layer                      │
├─────────────────────────────────────────────────────────┤
│                      Engine Layer                        │
│         (Query Planner | Write Coordinator)              │
├─────────────────────────────────────────────────────────┤
│                     Storage Layer                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │  MemTable   │  │  Shard      │  │  SSTable        │  │
│  │  (跳表)      │──│  Manager    │──│  (Block + Index)│  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
│         │                │                               │
│         └────────────────┘                               │
│                   WAL                                    │
│         (Write-Ahead Log)                                │
└─────────────────────────────────────────────────────────┘
```

### 核心组件

| 组件 | 描述 | 职责 |
|------|------|------|
| **MemTable** | 内存跳表索引 | 热点数据快速写入和查询 |
| **Shard Manager** | Shard 生命周期管理 | 按时间窗口分片，自动 Flush |
| **SSTable** | 有序字符串表 | 磁盘持久化，支持块压缩和稀疏索引 |
| **WAL** | 预写日志 | 崩溃恢复，保证数据一致性 |
| **Metadata** | 元数据管理 | bbolt 单文件存储，Catalog/Series/ShardIndex 三层管理 |

## 快速开始

### 安装

```bash
go get codeberg.org/micro-ts/mts
```

### 最小工作示例

```go
package main

import (
    "context"
    "log"
    "time"

    microts "micro-ts"
)

func main() {
    // 打开数据库
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

    // 查询数据
    resp, err := db.QueryRange(context.Background(), &microts.QueryRangeRequest{
        Database:    "metrics",
        Measurement: "cpu",
        StartTime:   time.Now().Add(-time.Hour).UnixNano(),
        EndTime:     time.Now().UnixNano(),
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("查询到 %d 条记录", len(resp.Rows))
}
```

### 启动 gRPC 服务

```bash
go run cmd/server/main.go
# 服务监听 :50051
```

## 配置说明

### 数据库配置

```go
config := microts.Config{
    // 数据目录（必需）
    DataDir: "/var/lib/microts",

    // Shard 时间窗口，最小 1 小时，默认 7 天
    ShardDuration: 24 * time.Hour,

    // MemTable 配置
    MemTableCfg: &microts.MemTableConfig{
        // 最大内存大小（字节），默认 64MB
        MaxSize: 64 * 1024 * 1024,

        // 最大条目数，默认 3000
        MaxCount: 3000,

        // 空闲时间阈值，默认 1 分钟
        IdleDuration: time.Minute,
    },
}
```

### 配置建议

| 场景 | MaxSize | MaxCount | ShardDuration |
|------|---------|----------|---------------|
| 高频写入（IoT）| 128MB | 10000 | 1h |
| 中频写入（监控）| 64MB | 3000 | 24h |
| 低频写入（日志）| 32MB | 1000 | 7d |

## 性能基准

项目包含完整性能基准测试，用于跟踪性能变化。

### 核心性能指标

| 组件 | 指标 | 性能数据 | 说明 |
|------|------|----------|------|
| **MemTable 写入** | 565 ns/op, 776 B/op, 5 allocs | **177万+ tps** | 核心写入路径 |
| **WAL 序列化** | 286 ns/op, 336 B/op, 8 allocs | **350万+ tps** | 持久化写入 |
| **WAL 反序列化** | 584 ns/op, 1176 B/op, 21 allocs | 171万+ tps | 崩溃恢复 |

### 运行基准测试

```bash
# 运行所有基准测试
go test -bench=. -benchmem ./internal/storage/...

# 特定组件测试
# MemTable 性能测试
go test -bench=. -benchmem ./internal/storage/shard

# WAL 序列化性能
go test -bench=. -benchmem ./internal/storage/shard -run=BenchmarkEncode

# SSTable 读取性能
go test -bench=. -benchmem ./internal/storage/shard/sstable

# Metadata 性能
go test -bench=. -benchmem ./internal/storage/metadata
```

### 保存性能基线

```bash
# 保存当前性能基线
go test -bench=. -benchmem ./internal/storage/shard > benchmark/baseline_shard.txt
go test -bench=. -benchmem ./internal/storage/metadata > benchmark/baseline_metadata.txt
```

## 开发

### 目录结构

```
.
├── cmd/server/         # gRPC 服务入口
├── internal/
│   ├── api/            # gRPC API 实现
│   ├── engine/         # 查询引擎和写入协调
│   ├── query/          # 查询执行器
│   └── storage/        # 存储层
│       ├── metadata/   # 元数据 (bbolt)
│       └── shard/      # Shard 管理
│           ├── compression/  # 压缩算法
│           └── sstable/      # SSTable 实现
├── types/              # 公共类型定义
├── tests/e2e/          # 端到端测试
└── benchmark/          # 基准测试结果
```

### 运行测试

```bash
# 运行所有测试
go test ./...

# 运行端到端测试
go run tests/e2e/write_1k/main.go
go run tests/e2e/query_1k/main.go
```

### 贡献指南

1. Fork 本仓库
2. 创建功能分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'feat: add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

**代码规范：**
- 所有方法需达到 ≥90% 代码行覆盖率
- 使用 `golangci-lint` 进行代码检查
- 使用 `goimports-reviser` 格式化导入
- 禁止在 `for` 循环中使用 `defer`

## License

[Apache 2.0](LICENSE) © 2026 micro-ts Authors
