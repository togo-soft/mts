# Engine 启动时主动 WAL Replay 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Engine 初始化后自动遍历 shardIndex 中已注册的 shard 并回放 WAL，而非等到第一次查询时才触发。

**Architecture:** 在 ShardManager 构造函数中启动后台 goroutine 发现所有已注册 shard 并回放 WAL；Shard 创建时注册到 shardIndex；Engine Close 时等待发现完成。

**Tech Stack:** Go, bbolt, sync.WaitGroup

---

## File Structure

```
internal/storage/metadata/manager.go    # 新增 ListAllDatabases(), ListMeasurements()
internal/storage/shard/manager.go        # 新增 discoverAndReplayWAL(), WaitForDiscovery(), loadShardFromIndex(); 修改 GetShard()
internal/engine/engine.go               # 修改 Close() 调用 WaitForDiscovery()
tests/e2e/wal_test/main.go             # 修改 Test6_WALRestartRecovery 移除触发写入点
```

---

## Task 1: Manager 增加遍历接口

**Files:**
- Modify: `internal/storage/metadata/manager.go`

- [ ] **Step 1: 添加 ListAllDatabases 方法**

在 `Manager` 结构体中增加：

```go
// ListAllDatabases 返回所有数据库名称
func (m *Manager) ListAllDatabases() []string {
    return m.catalog.ListDatabases()
}
```

- [ ] **Step 2: 添加 ListMeasurements 方法**

```go
// ListMeasurements 返回指定数据库下的所有 measurement
func (m *Manager) ListMeasurements(database string) ([]string, error) {
    return m.catalog.ListMeasurements(database)
}
```

- [ ] **Step 3: 运行测试验证**

```bash
cd /root/projects/mts && go build ./internal/storage/metadata/...
```

---

## Task 2: ShardManager 增加发现方法

**Files:**
- Modify: `internal/storage/shard/manager.go`

- [ ] **Step 1: 添加 discoveryDone 和 discoveryWg 字段**

在 `ShardManager` 结构体中增加：

```go
type ShardManager struct {
    // ... 现有字段 ...
    discoveryDone chan struct{}
    discoveryWg   sync.WaitGroup
}
```

- [ ] **Step 2: 修改 NewShardManager 在末尾启动后台发现**

```go
func NewShardManager(dir string, shardDuration time.Duration, memTableCfg *memtable.MemTableConfig, compactionCfg *compaction.CompactionConfig, mgr *metadata.Manager) *ShardManager {
    sm := &ShardManager{
        dir:                    dir,
        shardDuration:          shardDuration,
        memTableCfg:            memTableCfg,
        compactionCfg:          compactionCfg,
        manager:                mgr,
        shards:                 make(map[string]*Shard),
        discoveredMeasurements: make(map[string]bool),
        discoveryDone:          make(chan struct{}),
    }

    // 后台触发主动发现，不阻塞构造函数
    sm.discoveryWg.Add(1)
    go func() {
        defer sm.discoveryWg.Done()
        if err := sm.discoverAndReplayWAL(); err != nil {
            slog.Warn("failed to discover and replay WAL", "error", err)
        }
        close(sm.discoveryDone)
    }()

    return sm
}
```

- [ ] **Step 3: 添加 discoverAndReplayWAL 方法**

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
                // 4. 检查是否已存在
                m.mu.Lock()
                key := m.makeKey(db, meas, info.StartTime)
                if _, ok := m.shards[key]; ok {
                    m.mu.Unlock()
                    continue
                }
                m.mu.Unlock()

                // 5. 创建 Shard 实例并回放 WAL
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

- [ ] **Step 4: 添加 loadShardFromIndex 方法**

```go
func (m *ShardManager) loadShardFromIndex(db, measurement string, info metadata.ShardInfo) *Shard {
    seriesStore := m.manager.GetOrCreateSeriesStore(db, measurement)
    s := NewShard(ShardConfig{
        DB:            db,
        Measurement:   measurement,
        StartTime:     info.StartTime,
        EndTime:       info.EndTime,
        Dir:           info.DataDir,
        SeriesStore:   seriesStore,
        MemTableCfg:   m.memTableCfg,
        CompactionCfg: m.compactionCfg,
    })
    if err := s.ReplayWAL(); err != nil {
        slog.Warn("failed to replay WAL for discovered shard", "key", info.ID, "error", err)
    }
    return s
}
```

- [ ] **Step 5: 添加 WaitForDiscovery 方法**

```go
func (m *ShardManager) WaitForDiscovery() {
    m.discoveryWg.Wait()
}
```

- [ ] **Step 6: 编译验证**

```bash
cd /root/projects/mts && go build ./internal/storage/shard/...
```

---

## Task 3: GetShard 中注册 Shard 到 shardIndex

**Files:**
- Modify: `internal/storage/shard/manager.go`

- [ ] **Step 1: 修改 GetShard 在创建成功后注册**

找到 `GetShard` 方法中 `m.shards[key] = s` 之后，添加注册逻辑：

```go
    m.shards[key] = s

    // 注册到 shardIndex
    if err := m.manager.Shards().RegisterShard(db, measurementName, metadata.ShardInfo{
        ID:        key,
        StartTime: startTime,
        EndTime:   endTime,
        DataDir:   shardDir,
    }); err != nil {
        slog.Warn("failed to register shard", "key", key, "error", err)
    }

    return s, nil
```

- [ ] **Step 2: 编译验证**

```bash
cd /root/projects/mts && go build ./internal/storage/shard/...
```

---

## Task 4: Engine Close 等待 Discovery 完成

**Files:**
- Modify: `internal/engine/engine.go`

- [ ] **Step 1: 修改 Close 调用 WaitForDiscovery**

在 `e.queryWg.Wait()` 之后添加：

```go
    e.queryWg.Wait()

    // 等待 WAL replay 完成
    e.shardManager.WaitForDiscovery()

    _ = e.shardManager.FlushAll()
```

- [ ] **Step 2: 编译验证**

```bash
cd /root/projects/mts && go build ./internal/engine/...
```

---

## Task 5: 修改 E2E 测试移除触发写入点

**Files:**
- Modify: `tests/e2e/wal_test/main.go`

- [ ] **Step 1: 修改 Test6_WALRestartRecovery 移除触发写入点**

找到 Step 4 中 `// 由于 MTS 架构限制...` 的注释和触发写入逻辑，移除并替换为直接查询验证：

```go
    // 原来：
    // 由于 MTS 架构限制，需要先写入一条数据触发 Shard 发现
    // WAL replay 会在此时恢复第一次会话的数据
    triggerTime := session1BaseTime + 200*int64(time.Millisecond)
    triggerPoint := &types.Point{...}
    if err := db2.Write(context.Background(), triggerPoint); err != nil {...}
    fmt.Printf("      写入触发点...\n")

    // 改为：
    // Shard 发现和 WAL replay 在 engine 初始化时自动完成
    // 无需触发写入点
    fmt.Printf("      等待 Shard 发现和 WAL Replay 完成...\n")
    time.Sleep(500 * time.Millisecond) // 等待后台发现完成
```

- [ ] **Step 2: 编译并运行测试验证**

```bash
cd /root/projects/mts/tests/e2e/wal_test && go build -o wal_test . && ./wal_test
```

- [ ] **Step 3: 清理测试产物**

```bash
rm -f /root/projects/mts/tests/e2e/wal_test/wal_test
```

---

## Task 6: 提交代码

- [ ] **Step 1: 提交所有修改**

```bash
git add -A && git commit -m "$(cat <<'EOF'
feat(engine): 启动时主动发现 shard 并回放 WAL

- ShardManager 构造函数启动后台 goroutine 遍历 shardIndex
- GetShard 创建时注册 shard 到 shardIndex
- Engine Close 等待 WAL replay 完成
- E2E 测试移除手动触发写入点
EOF
)"
```

---

## 验证清单

- [ ] `go build ./...` 全量编译通过
- [ ] E2E 测试 `wal_test` 全部通过（特别是 Test6）
- [ ] `golangci-lint run ./...` 无错误
- [ ] 无临时文件残留
