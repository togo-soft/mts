# micro-ts

[![Go Version](https://img.shields.io/badge/go-1.26+-00ADD8.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> 高性能微时序数据库，专为高吞吐写入和快速时间范围查询设计。

## 特性

- ⚡ **高性能写入**：全局 WAL + 全局 MemTable，单点写入路径极短
- 💾 **分层存储**：MemTable → unordered SSTable → stable L0 SSTable，逐级下沉
- 🔄 **异步 Compaction**：后台自动将无序数据分拣排序写入有序层
- 🛡️ **崩溃恢复**：WAL 预写日志 + checkpoint 机制，重启自动恢复
- 🗄️ **元数据管理**：bbolt 嵌入式 KV 存储，ACID 事务保证一致性
- 🔌 **gRPC 接口**：高性能远程访问接口
- 🔍 **时间范围查询**：纳秒级精度，合并内存/无序/有序三层数据
- 🗜️ **数据压缩**：支持 Snappy / LZ4 块压缩

## 架构

```
                              ┌──────────────────────┐
                              │     gRPC API Layer    │
                              └──────────┬───────────┘
                                         │
                              ┌──────────▼───────────┐
                              │    Engine (引擎)      │
                              │  写入协调 | 查询合并   │
                              └──────────┬───────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                          │
    ┌─────────▼────────┐    ┌───────────▼──────────┐    ┌──────────▼─────────┐
    │  全局 WAL         │    │  全局 MemTable       │    │  ShardManager       │
    │  (预写日志)       │    │  (内存有序跳表)      │    │  (Shard 生命周期)   │
    └─────────┬────────┘    └───────────┬──────────┘    └──────────┬─────────┘
              │ 写后即返回              │ 背压触发刷盘              │
              │                         │                          │
              │              ┌──────────▼──────────┐               │
              │              │  FlushCoordinator    │               │
              │              │  (异步刷盘编排)      │               │
              │              └──────────┬──────────┘               │
              │                         │                          │
              │              ┌──────────▼──────────┐               │
              │              │  unordered/          │               │
              │              │  (未排序 SSTable)    │               │
              │              └──────────┬──────────┘               │
              │                         │                          │
              │              ┌──────────▼──────────┐    ┌──────────▼─────────┐
              │              │  UnorderedCompactor  │───▶│  stable/L0/         │
              │              │  (分拣排序 → L0)     │    │  (有序 SSTable)     │
              │              └─────────────────────┘    └─────────────────────┘
              │
    ┌─────────▼────────┐
    │  Metadata (bbolt) │
    │  Catalog | Series │
    │  | ShardIndex     │
    └──────────────────┘
```

### 核心组件

| 组件 | 职责 |
|------|------|
| **Engine** | 入口协调层：写入、查询合并、崩溃恢复 |
| **全局 WAL** | 单 WAL 实例，所有写入先入 WAL 保证持久性 |
| **全局 MemTable** | 单 MemTable 实例，跳表排序，双 buffer（active/passive）无锁 swap |
| **FlushCoordinator** | 定时检查 MemTable 水位，触发 swap → 写入 unordered → 清理 WAL |
| **unordered/** | 存放从 MemTable 落盘但未排序的 SSTable 文件 |
| **UnorderedCompactor** | 扫描 unordered 文件，按 (db, measurement, shard) 分拣排序后写入 stable L0 |
| **ShardManager** | 管理 shard 目录生命周期，提供 L0 写入路径 |
| **SSTable** | 有序不可变字符串表，支持块压缩（Snappy/LZ4）和稀疏索引 |
| **Metadata** | bbolt 单文件存储，管理 Catalog、Series、ShardIndex |

## 写流程

```
Point ──▶ engine.Write()
           │
           ├─ 1. 校验 & 自动创建 db/measurement
           ├─ 2. 分配 SID (Series ID)
           ├─ 3. Point → MemPoint (序列化 FieldData, 池化零拷贝)
           ├─ 4. MemPoint → WAL 序列化 (version + db + meas + ts + sid + fieldData)
           ├─ 5. WAL 写入 (LZ4 压缩, small payload <80B 跳过)
           │      └─ 写缓冲聚合 (1MB), 定时 fsync
           ├─ 6. MemTable.Write (跳表插入, O(log N))
           │
           └─ 背压检查: MemTable 满?
                  ├─ 是 → flushWithRetry (最多 10 次, 间隔 50ms)
                  └─ 否 → 返回
```

### 刷盘流程 (FlushCoordinator, 每 1s 检查)

```
触发条件: FlushMemorySize/FlushPointCount 达到 2x / FlushIdle 超时

active MemTable ──Swap──▶ passive ──▶ unordered.Write()
                                         │
                                         ├─ 按 (db, meas) 分组
                                         ├─ 每组写入 SSTable (unordered/db/meas/sst_N.bin)
                                         └─ 写入成功后释放 FieldData 回池
                              │
                              ├─ ClearPassive()
                              └─ WAL TruncateBefore (删除已刷盘的 WAL segment)
```

### Compaction 流程 (UnorderedCompactor, 每 500ms)

```
unordered/sst_*.bin ──▶ 扫描所有文件
                         │
                         ├─ 读取每个 SSTable 的全部数据
                         ├─ 按 (db, measurement, shardStart) 分组
                         ├─ 每组内按 Timestamp 排序
                         ├─ 写入 stable/{db}/{meas}/{shardStart}_{shardEnd}/data/L0/
                         └─ 删除源 unordered 文件
```

通过 L0 Compaction，后续的 Level Compaction (L0→L1→...) 由 ShardManager 内部的 Compaction 机制处理。

## 读流程

```
QueryRange ──▶ engine.Iterator()
                │
                ├─ 1. 收集三层数据源:
                │      ├─ 全局 MemTable (未刷盘的热数据, 已排序)
                │      ├─ unordered SSTable (已刷盘但未排序, 按文件读取后排序)
                │      └─ stable Shard SSTable (已 compaction 的有序数据)
                │
                ├─ 2. 构建 MergeIterator:
                │      合并三层 Iterator, 按 Timestamp 归并输出
                │
                └─ 3. 流式返回:
                       Next() 按需拉取, 避免全量加载
```

### 数据流路径

```
写入时:  WAL ──▶ MemTable ──▶ unordered ──▶ stable L0 ──▶ L1...(Level Compaction)
查询时:  结果 = Merge(MemTable, unordered, stable)
```

## 崩溃恢复

```
启动 ──▶ 1. 加载 Metadata (bbolt)
        ├─ 2. 恢复 unordered seq (扫描最大序列号)
        ├─ 3. 发现已有 Shard (填充 ShardManager 缓存)
        ├─ 4. WAL.Replay()
        │      ├─ 加载 checkpoint, 跳过已持久化的旧 segment
        │      ├─ 反序列化每条 WAL 记录 → MemPoint
        │      ├─ MemTable 接近满时先刷盘
        │      └─ 写入全局 MemTable
        ├─ 5. MemTable.Sort() (重建跳表顺序)
        └─ 6. TruncateBefore (清理已回放的 WAL segment)
```

## 目录结构

```
{dataDir}/
├── wal/                          # 全局 WAL segment
│   ├── {gen}_{seq}.wal           # segment 文件 (含 LZ4 压缩)
│   └── checkpoint                # 已持久化标记
├── metadata.db                   # bbolt 元数据
├── unordered/                    # 未排序 SSTable
│   └── {db}/{meas}/sst_{N}.bin
├── {db}/{meas}/                  # 有序 SSTable
│   └── {shardStart}_{shardEnd}/
│       └── data/
│           └── L{N}/sst_{M}.bin  # Level Compaction 层级
└── internal/
    ├── engine/                   # 引擎 (写入、查询、恢复)
    ├── query/                    # 查询执行器 (归并迭代器)
    ├── storage/
    │   ├── wal/                  # WAL 实现
    │   ├── memtable/             # 内存跳表
    │   ├── unordered/            # 未排序 SSTable 管理
    │   ├── compaction/           # Compaction 策略 (含 UnorderedCompactor)
    │   ├── shard/                # Shard 管理 & SSTable
    │   │   ├── sstable/          # SSTable 读写
    │   │   └── compression/      # Snappy/LZ4
    │   └── metadata/             # bbolt 元数据
    ├── api/                      # gRPC 服务
    └── metrics/                  # 内部指标
```

## 快速开始

### 安装

```bash
go get codeberg.org/micro-ts/mts
```

### 基本使用

```go
package main

import (
    "context"
    "log"
    "time"

    microts "codeberg.org/micro-ts/mts"
)

func main() {
    db, err := microts.Open(microts.Config{
        DataDir: "/tmp/microts",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 写入
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

    // 批量写入
    points := []*microts.Point{point1, point2, point3}
    if err := db.WriteBatch(context.Background(), points); err != nil {
        log.Fatal(err)
    }

    // 范围查询
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

## 配置说明

```go
config := microts.Config{
    DataDir:       "/var/lib/microts",
    ShardDuration: 24 * time.Hour,       // Shard 时间窗口 (默认 7d)

    MemTableCfg: &microts.MemTableConfig{
        FlushMemorySize: 64 * 1024 * 1024,   // 内存阈值 (默认 64MB)
        FlushPointCount: 50000,              // 条目阈值 (默认 10000)
        FlushIdleNanos: int64(time.Minute),  // 空闲刷盘时间 (默认 1min)
    },

    CompactionCfg: &microts.CompactionConfig{
        MaxSstableCount: 4,               // 触发 compaction 阈值
        CheckInterval:   time.Hour,       // 检查间隔
    },

    CompressionAlgorithm: microts.CompressionLZ4,  // none/snappy/lz4
    RetentionPeriod:      30 * 24 * time.Hour,     // 数据保留期 (0=永久)
}
```

### 配置建议

| 场景 | FlushMemorySize | FlushPointCount | FlushIdle | ShardDuration |
|------|---------|----------|-------------|---------------|
| 高频写入 (IoT) | 128MB | 100000 | 30s | 1h |
| 中频写入 (监控) | 64MB | 50000 | 1min | 24h |
| 低频写入 (日志) | 32MB | 10000 | 5min | 7d |

## 性能

| 规模 | TPS | 内存占用 | 磁盘占用 |
|------|-----|---------|---------|
| 1K | ~552K | ~7 MB | ~0.4 MB |
| 10K | ~555K | ~10 MB | ~0.8 MB |
| 100K | ~568K | ~38 MB | ~5.5 MB |
| 1M | ~228K | ~188 MB | ~42 MB |

1M 数据点: TotalAlloc ~3.3GB, GC 周期 ~26 次, 磁盘压缩比 ~1.8x (LZ4)。

### 运行基准测试

```bash
go test -bench=. -benchmem ./internal/storage/...
```

### E2E 测试

```bash
# 写入性能
cd tests/e2e/write_1m && go run main.go

# 数据完整性
cd tests/e2e/integrity && go run main.go

# 崩溃恢复
cd tests/e2e/restart_recovery && go run main.go
```

## 开发

### 运行测试

```bash
go test ./...                        # 全部单元测试
cd tests/e2e/{test_dir} && go run main.go  # 单个 E2E 测试
```

### 代码规范

- 代码行覆盖率 ≥ 90%
- `golangci-lint` 代码检查
- `goimports-reviser` 格式化导入
- 禁止在 `for` 循环中使用 `defer`
- 目录权限 `0700`，文件权限 `0600`

### Git GPG 签名免交互配置

使用 GPG 签名提交时，默认会触发交互式密码输入。以下配置可实现自动化：

**1. 配置 gpg-agent 使用 loopback 模式**

```bash
echo "allow-loopback-pinentry" >> ~/.gnupg/gpg-agent.conf
```

**2. 重启 gpg-agent**

```bash
killall gpg-agent 2>/dev/null || true
gpg-agent --homedir ~/.gnupg --daemon
```

**3. 配置 Git 使用 GPG 签名**

```bash
git config --global gpg.program gpg
git config --global commit.gpgsign true
git config --global user.signingkey <YOUR_GPG_KEY_ID>
```

**4. 设置环境变量自动提供密码**

```bash
export GPG_TTY=$(tty)
export PINENTRY_USER_DATA="<YOUR_PASSPHRASE>"
```

或直接在命令行中指定：

```bash
git commit -s -S -m "提交信息"
```

**5. 验证配置**

```bash
git log --show-signature -1
```

## License

[Apache 2.0](LICENSE) © 2026 micro-ts Authors
