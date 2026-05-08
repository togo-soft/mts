# Engine 启动时主动 WAL Replay 设计

## 目标

Engine 初始化后、metadata 加载到内存时，遍历所有已注册的 shard 并回放其 WAL 日志，而不是等到第一次查询时才触发。

## 架构

```
engine.New()
  └─ mgr.Load()                          // 加载 series 缓存
  └─ shard.NewShardManager()             // ShardManager 创建
        └─ discoverAndReplayWAL()         // 构造函数末尾自动触发
              └─ 遍历 catalog 中所有 db/measurement
                    └─ 查询 shardIndex 获取已注册 shard
                          └─ 对每个 shard 创建 Shard 实例并 ReplayWAL()
```

## 核心改动

### 1. ShardManager 增加发现方法

在 `internal/storage/shard/manager.go` 中：

**新增私有方法 `discoverAndReplayWAL()`（后台运行）**

```go
func NewShardManager(...) *ShardManager {
    sm := &ShardManager{...}
    // 后台触发主动发现，不阻塞构造函数
    sm.discoveryWg.Add(1)
    go func() {
        defer sm.discoveryWg.Done()
        if err := sm.discoverAndReplayWAL(); err != nil {
            slog.Warn("failed to discover and replay WAL", "error", err)
        }
    }()
    return sm
}
```

**discoverAndReplayWAL 实现**

```go
func (m *ShardManager) discoverAndReplayWAL() error {
    // 1. 遍历 catalog 获取所有 db
    databases := m.manager.ListAllDatabases()
    for _, db := range databases {
        // 2. 遍历每个 db 的 measurement
        measurements, _ := m.manager.ListMeasurements(db)
        for _, meas := range measurements {
            // 3. 查询 shardIndex 获取已注册 shard
            shards := m.manager.Shards().ListShards(db, meas)
            for _, info := range shards {
                // 4. 创建 Shard 实例并回放 WAL
                m.mu.Lock()
                key := m.makeKey(db, meas, info.StartTime)
                if _, ok := m.shards[key]; ok {
                    m.mu.Unlock()
                    continue
                }
                m.mu.Unlock()

                s := m.loadShardFromIndex(db, meas, info)
                if s != nil {
                    m.mu.Lock()
                    m.shards[key] = s
                    m.mu.Unlock()
                }
            }
        }
    }
    return nil
}
```

### 2. Shard 创建时注册到 shardIndex

在 `GetShard()` 中，shard 创建成功后注册到 shardIndex：

```go
func (m *ShardManager) GetShard(db, measurementName string, timestamp int64) (*Shard, error) {
    // ... existing code ...

    s = NewShard(ShardConfig{...})
    if err := s.ReplayWAL(); err != nil {
        slog.Warn("failed to replay WAL for new shard", "key", key, "error", err)
    }
    m.shards[key] = s

    // 新增：注册到 shardIndex
    if err := m.manager.Shards().RegisterShard(db, measurementName, ShardInfo{
        ID:        key,
        StartTime: startTime,
        EndTime:   endTime,
        DataDir:   shardDir,
    }); err != nil {
        slog.Warn("failed to register shard", "key", key, "error", err)
    }

    return s, nil
}
```

### 3. Manager 增加遍历接口

在 `internal/storage/metadata/manager.go` 中增加两个方法：

```go
// ListAllDatabases 返回所有数据库名称
func (m *Manager) ListAllDatabases() []string

// ListMeasurements 返回指定数据库下的所有 measurement
func (m *Manager) ListMeasurements(database string) ([]string, error)
```

实现复用到 `catalogStore.ListDatabases()` 和 `catalogStore.ListMeasurements()`。

## 并发安全

- `discoverAndReplayWAL` 在后台 goroutine 运行，不阻塞引擎启动
- 访问 `m.shards` 时需要加锁保护
- 发现 shard 时检查是否已存在，避免重复创建
- `discoverShardsLocked` 遍历文件系统发现 shard 时，仍使用原有逻辑（按需发现）

## Shutdown 处理

Engine Close 时需要等待后台 WAL replay 完成：

```go
func (e *Engine) Close() error {
    // ... 现有逻辑 ...
    e.queryWg.Wait()  // 现有等待查询完成的逻辑

    // 新增：等待 WAL replay 完成
    e.shardManager.WaitForDiscovery()
    return nil
}
```

在 `ShardManager` 中增加：

```go
type ShardManager struct {
    // ... 现有字段 ...
    discoveryDone chan struct{}
    discoveryWg   sync.WaitGroup
}

func (m *ShardManager) WaitForDiscovery() {
    m.discoveryWg.Wait()
}
```

## 错误处理

- WAL replay 失败只 warn 不阻塞（与现有逻辑一致）
- shardIndex 查询为空时跳过（新建数据库场景）
- catalog 遍历出错时记录 warn 并继续

## 测试

- 现有 E2E 测试 `wal_test/Test6_WALRestartRecovery` 应能通过（移除触发写入点）
- 新增单元测试验证 ShardManager 启动时的主动发现行为

## 依赖

- `ShardInfo` 字段保持不变
- 不修改 `ShardIndex` 接口
- 不修改 `catalogStore` 现有接口
