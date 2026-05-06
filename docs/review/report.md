# MTS 项目详细检视报告

**项目**: Micro Time-Series (MTS)
**检视日期**: 2026-05-06
**Go 版本**: 1.26+
**检视人员**: 资深 Go 工程师 / 时序数据库专家

---

## 一、项目概述

### 1.1 项目简介

**MTS (Micro Time-Series)** 是一个用 Go 实现的高性能时序数据库，专为微服务环境设计。

**架构图**:

```
┌─────────────────────────────────────────────────────────────┐
│                      gRPC API Layer                          │
│                   (internal/api/grpc.go)                     │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                      Engine Layer                            │
│    Write: WAL → MemTable → (flush) → SSTable              │
│    Query: ShardManager → Shard → MemTable + SSTable        │
│                  (internal/engine/engine.go)                │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌───────────────────────┬───────────────────────────────────┐
│    ShardManager       │           MetaStore                 │
│  - Shard lifecycle    │  - Series ID allocation            │
│  - Time-window routing│  - Tag index                       │
│  - Retention policy   │  - Persistence                     │
└───────────────────────┴───────────────────────────────────┘
                              ↓
┌───────────┬───────────┬───────────┬───────────────────────┐
│  MemTable │    WAL    │  SSTable  │   RetentionService   │
│  - SkipList│ - Durable │ - Columnar│  - TTL cleanup       │
│  - In-memory│ - Append │ - Indexed │                       │
└───────────┴───────────┴───────────┴───────────────────────┘
```

### 1.2 核心组件

| 组件 | 文件位置 | 职责 |
|------|---------|------|
| Engine | `internal/engine/engine.go` | 核心协调器，管理写入和查询 |
| ShardManager | `internal/storage/shard/manager.go` | Shard 生命周期管理 |
| Shard | `internal/storage/shard/shard.go` | 单个时间窗口数据管理 |
| MemTable | `internal/storage/shard/memtable.go` | 内存写入缓冲区 |
| WAL | `internal/storage/shard/wal.go` | 预写日志，崩溃恢复 |
| SSTable | `internal/storage/shard/sstable/` | 持久化列式存储 |
| MetaStore | `internal/storage/measurement/meas_meta.go` | Series ID 和标签管理 |
| QueryIterator | `internal/query/iterator.go` | 流式查询迭代器 |

---

## 二、代码质量评估

### 2.1 Lint 和测试

| 指标 | 状态 |
|------|------|
| golangci-lint | ✅ 0 issues |
| 单元测试 | ✅ 全部通过 |
| 代码覆盖 | ⚠️ 见下文 |

**测试覆盖率**:

| 包 | 覆盖率 |
|----|--------|
| internal/api | 88.1% |
| internal/engine | 90.2% |
| internal/query | 98.6% |
| internal/storage | 63.3% |
| internal/storage/measurement | 75.5% |
| internal/storage/shard | 76.2% |
| internal/storage/shard/compression | 96.6% |
| internal/storage/shard/sstable | 79.7% |

### 2.2 优点

1. **清晰的模块化设计**: 各组件职责明确，接口清晰
2. **完善的错误处理**: 大部分关键路径有错误处理
3. **并发安全**: 正确使用 RWMutex 保护共享状态
4. **文档完整**: 每个公共函数都有详细的 GoDoc 注释
5. **安全意识**: 路径遍历检查、权限设置 (0700/0600)

### 2.3 需要改进的地方

#### P1: 设计缺陷

| 问题 | 严重程度 | 文件 | 说明 |
|------|---------|------|------|
| 无 SSTable Compaction | 中 | sstable/ | 只增不减，无法合并旧文件 |
| WAL 全量加载到内存 | 中 | wal.go | 大 WAL 文件可能导致 OOM |
| MetaStore 无持久化验证 | 低 | meas_meta.go | 持久化失败静默忽略 |

#### P2: 潜在问题

| 问题 | 严重程度 | 文件 | 说明 |
|------|---------|------|------|
| ShardIterator 非线程安全 | 低 | iterator.go | 文档已说明 |
| 单 WAL 文件无滚动 | 低 | wal.go | 文件无限增长 |
| 无优雅关闭 | 低 | engine.go | 正在运行的查询会被中断 |

#### P3: 测试覆盖不足

| 文件/包 | 当前覆盖 | 建议覆盖 |
|---------|---------|---------|
| internal/storage | 63.3% | 80%+ |
| internal/storage/measurement | 75.5% | 85%+ |
| internal/storage/shard | 76.2% | 85%+ |

---

## 三、架构分析

### 3.1 写入路径

```
Write Request
    ↓
Engine.Write()
    ↓
ShardManager.GetShard() → 创建/获取 Shard
    ↓
Shard.Write()
    ├── serializePoint() → 序列化
    ├── WAL.Write() → 持久化日志
    ├── MetaStore.AllocateSID() → 分配 Series ID
    ├── MemTable.Write() → 写入内存
    └── ShouldFlush() → 检查是否需要刷盘
        ↓ (如果需要)
    flushLocked() → MemTable → SSTable
```

**优点**:
- WAL 保证持久性
- MemTable 批量刷盘减少 IO
- 双检锁避免并发问题

**问题**:
- 刷盘触发阈值固定（64MB 或 3000 条）
- 无后台 compaction 合并小文件

### 3.2 读取路径

```
Query Request
    ↓
Engine.Query()
    ↓
ShardManager.GetShards() → 获取时间范围内所有 Shard
    ↓
QueryIterator.New() → 为每个 Shard 创建 ShardIterator，加入最小堆
    ↓
迭代:
    ├── heap.Pop() → 取最小时间戳
    ├── 返回当前行
    └── Next() → 推进迭代器
```

**优点**:
- 使用最小堆实现多路归并
- 支持流式处理大数据集
- BlockIndex 减少不必要的 IO

**问题**:
- ShardIterator 将整个时间范围数据预加载到内存
- GetShards 不发现未加载到内存的 Shard

### 3.3 元数据管理

**Series ID 分配**:
```go
1. 计算 tagsHash
2. 查找 tagHashIndex 确认是否存在
3. 如果存在，返回现有 SID
4. 如果不存在，分配新 SID (nextSID++)
5. 更新 series, tagHashIndex, tagIndex
```

**优点**:
- O(1) 查找已存在 Series
- 标签倒排索引支持快速过滤
- FNV-1a 哈希减少碰撞

---

## 四、深入分析

### 4.1 WAL 实现

**文件**: `internal/storage/shard/wal.go`

**优点**:
- 4KB 缓冲减少系统调用
- 支持定期同步 (StartPeriodicSync)
- SafeMkdirAll 确保目录安全创建

**问题**:
```go
// ReplayWAL 将所有 WAL 数据一次性加载到内存
func ReplayWAL(walDir string) ([]*types.Point, error) {
    var points []*types.Point
    for _, f := range files {
        data, err := os.ReadFile(f)  // 整个文件读入内存
        // ...
        points = append(points, p...)  // 累积到 slice
    }
    return points, nil
}
```

**风险**: 对于大 WAL 文件（如写入密集场景），可能导致内存压力。

### 4.2 SSTable 格式

**文件**: `internal/storage/shard/sstable/`

**目录结构**:
```
sst_{seq}/
├── _timestamps.bin    # 时间戳列
├── _sids.bin         # Series ID 列
├── _index.bin        # Block 索引
├── _schema.json      # 字段类型定义
└── fields/
    ├── {field1}.bin
    └── {field2}.bin
```

**BlockIndex**:
```go
type BlockIndexEntry struct {
    FirstTimestamp int64   // Block 首时间戳
    LastTimestamp  int64   // Block 尾时间戳
    Offset         uint32  // 在 timestamps.bin 的偏移
    RowCount       uint32  // 行数
}
```

**优点**:
- 列式存储，字段数据按列组织
- Block 索引支持二分查找
- 只读时间戳/sid 可确定数据位置

**问题**:
- 无 compaction，小文件会积累
- 字段数据仍需全读（无法块级索引）

### 4.3 Retention 服务

**文件**: `internal/storage/shard/retention.go`

```go
func (s *RetentionService) cleanup() {
    cutoff := time.Now().Add(-s.retention).UnixNano()
    shards := s.manager.GetAllShards()

    for _, shard := range shards {
        if shard.EndTime() < cutoff {
            // 删除过期 Shard
            s.manager.DeleteShard(key)
        }
    }
}
```

**问题**:
- 删除粒度是整个 Shard，不是单个数据点
- 如果 ShardDuration 大于 RetentionPeriod，清理会有空窗期

---

## 五、安全分析

### 5.1 已实现的安全措施

| 措施 | 实现位置 | 说明 |
|------|---------|------|
| 路径遍历防护 | `internal/storage/util.go` | `isPathSafe()` 检查 `..` |
| 目录权限 0700 | `storage.SafeMkdirAll()` | 只有 owner 可访问 |
| 文件权限 0600 | `storage.SafeCreate()` | 只有 owner 可读写 |
| 输入验证 | `engine.Write()` | Database/Measurement/Timestamp 检查 |

### 5.2 缺失的安全措施

| 措施 | 状态 | 说明 |
|------|------|------|
| 认证 | ❌ | 无用户认证机制 |
| 授权 | ❌ | 无权限控制 |
| 加密 | ❌ | 数据文件未加密 |
| 审计日志 | ❌ | 无操作日志 |

---

## 六、性能分析

### 6.1 已有的优化

| 优化 | 实现位置 | 效果 |
|------|---------|------|
| MemTable 跳表 | `memtable.go` | O(log n) 写入 |
| WAL 缓冲 | `wal.go` | 减少系统调用 |
| BlockIndex | `sstable/index.go` | 二分查找定位 Block |
| 定期排序标记 | `memtable.go:128` | 避免重复排序 |
| 流式迭代器 | `query/iterator.go` | 支持大数据集 |

### 6.2 性能瓶颈

| 瓶颈 | 影响 | 建议 |
|------|------|------|
| 固定 BlockSize (64KB) | 无法适应不同数据分布 | 可配置化 |
| 无 compaction | 小文件累积，查询变慢 | 实现 Level compaction |
| WAL 全量 replay | 大数据量启动慢 | 实现增量 replay |
| 单 Shard 热点 | 高写入时单 Shard 成为瓶颈 | 支持分片并行写入 |

---

## 七、可选功能（未来增强）

| 功能 | 优先级 | 工作量 | 说明 |
|------|--------|--------|------|
| SSTable Compaction | 高 | 中 | 合并小文件，减少查询 IO |
| 增量 WAL Replay | 中 | 中 | 支持大 WAL 文件启动 |
| 标签压缩 | 低 | 小 | 共享标签存储 |
| SQL 查询支持 | 中 | 大 | Executor 目前是 placeholder |
| 备份/恢复 | 低 | 中 | 数据导出导入工具 |
| 集群支持 | 低 | 大 | 多节点数据分片 |

---

## 八、测试建议

### 8.1 单元测试覆盖提升

**目标**: 核心模块达到 85%+

| 包 | 当前 | 目标 | 建议补充测试 |
|----|------|------|------------|
| internal/storage | 63.3% | 85% | 更多 util 测试 |
| measurement | 75.5% | 85% | MetaStore 边界条件 |
| shard | 76.2% | 85% | Shard 并发测试 |

### 8.2 集成测试

建议添加:
- Shard 迁移测试
- MetaStore 持久化恢复测试
- Retention 并发安全测试

### 8.3 E2E 测试

已添加:
- `retention_test` - Retention 过期清理
- `persistence_test` - MetaStore 持久化

建议添加:
- 大数据量写入/查询测试
- 并发写入压力测试
- 异常恢复测试

---

## 九、总结

### 9.1 整体评价

| 维度 | 评分 | 说明 |
|------|------|------|
| 架构设计 | ⭐⭐⭐⭐ | 清晰的模块化设计，职责分明 |
| 代码质量 | ⭐⭐⭐⭐ | Lint 0 issues，注释完善 |
| 错误处理 | ⭐⭐⭐ | 大部分路径有处理，少数边缘情况 |
| 性能 | ⭐⭐⭐⭐ | 已有多种优化，还有提升空间 |
| 测试 | ⭐⭐⭐ | 核心模块覆盖良好，部分可提升 |
| 安全性 | ⭐⭐⭐ | 基础安全措施到位，缺少企业级功能 |

### 9.2 关键优势

1. **清晰的数据流**: WAL → MemTable → SSTable 架构清晰
2. **完善的持久化**: MetaStore 和数据都有持久化保证
3. **索引优化**: BlockIndex 减少查询 IO
4. **流式查询**: 支持大数据集处理
5. **Retention 机制**: 自动清理过期数据

### 9.3 改进建议

**短期** (1-2 周):
1. 提升 internal/storage 测试覆盖率
2. 实现 WAL 增量 replay 支持
3. 添加更多集成测试

**中期** (1-2 月):
1. 实现 SSTable Compaction
2. 支持标签压缩减少存储
3. 添加 Executor 完整实现

**长期** (3-6 月):
1. 集群支持
2. 备份恢复工具
3. SQL 查询支持

---

## 附录 A: 文件结构

```
mts/
├── mts.go                          # 主入口，重导出类型
├── microts/                         # 公共 API
├── internal/
│   ├── engine/engine.go            # 核心引擎
│   ├── api/grpc.go                # gRPC 服务
│   ├── query/
│   │   ├── iterator.go            # 流式迭代器
│   │   └── executor.go            # 查询执行器 (placeholder)
│   └── storage/
│       ├── measurement/            # 元数据管理
│       │   ├── meas_meta.go       # Series ID 管理
│       │   ├── db_meta.go         # 数据库元数据
│       │   └── store.go           # 存储抽象
│       └── shard/
│           ├── shard.go           # Shard 实现
│           ├── manager.go         # Shard 管理
│           ├── memtable.go       # 内存表
│           ├── wal.go            # 预写日志
│           ├── retention.go       # 过期清理
│           └── sstable/          # SSTable 实现
│               ├── writer.go
│               ├── reader.go
│               ├── index.go
│               └── iterator.go
├── types/                          # Protobuf 生成类型
└── tests/e2e/                     # 端到端测试
    └── pkg/framework/             # 测试框架
```

---

**报告结束**
