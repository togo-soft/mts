# MTS 项目详细检视报告

**项目**: Micro Time-Series (MTS)
**检视日期**: 2026-05-06
**Go 版本**: 1.26+

---

## 项目概述

**MTS (Micro Time-Series)** 是一个用 Go 实现的高性能时序数据库，架构如下：

```
gRPC API → Engine → ShardManager → MemTable/WAL → SSTable
                                    ↓
                              MetaStore (测量元数据)
```

### 核心组件

| 组件 | 描述 |
|------|------|
| MemTable | 内存跳表索引，热点数据快速写入和查询 |
| Shard Manager | Shard 生命周期管理，按时间窗口分片 |
| WAL | 预写日志，崩溃恢复保证 |
| SSTable | 有序字符串表，持久化存储 |
| MetaStore | 元数据存储，Series ID 映射管理 |

---

## 一、严重问题 (P0) ✅ 已修复

| 问题 | 状态 | 说明 |
|------|------|------|
| Shard.Close() 资源泄漏 | ✅ 已修复 | WAL 和 tsSidMap 在 WritePoints 错误时未清理 |
| Block index 时间计算错误 | ✅ 已修复 | 使用 buffer 最后一个字节而非实际时间戳 |
| ShardIterator 错误忽略 | ✅ 已修复 | 添加了 err 字段和 Err() 方法 |

---

## 二、逻辑问题 (P2) ✅ 已修复

| 问题 | 状态 | 说明 |
|------|------|------|
| tsSidMap 内存泄漏风险 | ✅ 已修复 | Close() 中所有路径都清理 tsSidMap |
| Series ID 无上限检查 | ✅ 已修复 | 添加溢出检查，返回 error |
| ShardIterator 非线程安全 | ✅ 已修复 | 文档修正，明确说明非线程安全 |

---

## 三、安全问题 ✅ 已修复

| 问题 | 状态 | 说明 |
|------|------|------|
| 目录权限检查缺失 | ✅ 已修复 | 添加路径遍历检查，拒绝 `..` 路径组件 |
| 缺少输入验证 | ✅ 已修复 | Write() 添加 Database/Measurement/Timestamp 验证 |

---

## 四、API 设计问题 ✅ 已修复

| 问题 | 状态 | 说明 |
|------|------|------|
| DropMeasurement 错误语义混淆 | ✅ 已修复 | 使用 errors.Is() 替代字符串匹配 |

---

## 二、设计缺陷 (P1)

### 2.1 WAL 定期同步未启用 ⚠️

**文件**: `internal/storage/shard/shard.go`

WAL 有 `StartPeriodicSync` 方法，但创建后从未调用：

```go
// NewShard 创建新的 Shard 实例
wal, err := NewWAL(walDir, 0)
if err != nil {
    wal = nil  // 只是记录警告，从未启动定期同步
}
```

**影响**: 系统崩溃时可能丢失多个同步间隔的数据

**建议**: 在 Shard 创建后启动定期同步 goroutine

---

### 2.2 SSTable ReadRange 未使用索引 ⚠️

**文件**: `internal/storage/shard/sstable/reader.go:322`

```go
// ReadRange 读取指定时间范围内的数据
// 性能考虑：
//     当前实现是全表扫描，即使指定了时间范围。
//     未来可以结合 BlockIndex 实现索引加速。
func (r *Reader) ReadRange(startTime, endTime int64) ([]*types.PointRow, error) {
    // ... 全表扫描 ...
}
```

**影响**: 查询性能差，BlockIndex 形同虚设

---

### 2.3 无数据过期 (TTL) 机制 ⚠️

SSTable 文件只增不减，没有任何 compaction 或 TTL 逻辑。

**影响**: 存储只增不减，磁盘最终会耗尽

---

### 2.4 Engine.Close() 未调用 FlushAll ⚠️

**文件**: `internal/engine/engine.go:116`

```go
func (e *Engine) Close() error {
    e.mu.Lock()
    defer e.mu.Unlock()
    for _, dbMeta := range e.dbMetaStores {
        _ = dbMeta.Close()
    }
    e.dbMetaStores = nil
    // ❌ 没有调用 e.shardManager.FlushAll()
    return nil
}
```

而 `DB.Close()` 调用了 Flush：

```go
// mts.go:566
func (db *DB) Close() error {
    _ = db.engine.Flush()  // 这里调用了
    return db.engine.Close()
}
```

**影响**: 如果 Engine.Close() 被直接调用，数据可能丢失

---

### 2.5 MetaStore 持久化未自动触发 ⚠️

MetaStore 有 `Persist()` 方法，但只在 `measurement` 包内部使用，外部写入路径从未调用。

**影响**: MetaStore 元数据（Schema、Series）变更不会自动保存到磁盘

---

## 三、逻辑问题 (P2)

### 3.1 WriteBatch 无原子性保证

```go
// internal/engine/engine.go:207
func (e *Engine) WriteBatch(ctx context.Context, points []*types.Point) error {
    for _, p := range points {
        if err := e.Write(ctx, p); err != nil {
            return fmt.Errorf("write point (timestamp=%d): %w", p.Timestamp, err)
        }
    }
    return nil
}
```

**问题**: 部分失败时已写入的点不回滚，错误信息不完整

---

### 3.2 tsSidMap 内存泄漏风险

**文件**: `internal/storage/shard/shard.go:114`

```go
type Shard struct {
    tsSidMap    map[int64]uint64  // timestamp → sid 映射
}
```

**问题**:
- 只在 `flushLocked()` 时清理
- 但 `Close()` 中如果 writer 创建失败，这个 map 不会被清理
- WAL 重放时如果出错，point 会被跳过但 tsSidMap 已更新

---

### 3.3 Shard 时间窗口计算整数溢出风险

**文件**: `internal/storage/shard/manager.go:183`

```go
func (m *ShardManager) calcShardStart(timestamp int64) int64 {
    return (timestamp / int64(m.shardDuration)) * int64(m.shardDuration)
}
```

**问题**: `timestamp` 和 `shardDuration` 都是 int64，如果 shardDuration 很大且 timestamp 接近 int64 max，可能溢出

---

### 3.4 Series ID 无上限检查

**文件**: `internal/storage/measurement/meta.go`

```go
func (m *MeasurementMetaStore) AllocateSID(tags map[string]string) uint64 {
    sid := m.meta.NextSid  // uint64，会一直增长
    m.meta.NextSid++
    // ❌ 没有上限检查
    return sid
}
```

---

## 四、并发安全问题

### 4.1 ShardIterator 非线程安全

**文件**: `internal/storage/shard/iterator.go`

```go
type ShardIterator struct {
    memIter *MemTableIterator
    rows    []*types.PointRow
    rowIdx  int
    err     error
}
```

注释说明线程安全，但 `Next()` 和 `Current()` 没有锁保护。如果并发调用会导致 data race。

---

### 4.2 ShardManager.GetShards 锁粒度问题

**文件**: `internal/storage/shard/manager.go:162`

```go
func (m *ShardManager) GetShards(...) []*Shard {
    m.mu.RLock()
    defer m.mu.RUnlock()
    // ...遍历...
    // ❌ 如果 shard 不存在，不会创建
    // 但返回空切片，没有区分"不存在"和"时间范围内无数据"
}
```

---

## 五、性能问题

### 5.1 MemTable 排序开销

**文件**: `internal/storage/shard/memtable.go:128`

```go
if m.count > 1 && m.entries[m.count-1].Point.Timestamp < m.entries[m.count-2].Point.Timestamp {
    sort.Slice(m.entries, func(i, j int) bool {
        return m.entries[i].Point.Timestamp < m.entries[j].Timestamp
    })
}
```

乱序写入时每次都触发全局排序，复杂度 O(n log n)

**建议**: 改用跳表或定期排序

---

### 5.2 固定 Block 大小

**文件**: `internal/storage/shard/sstable/writer.go:63`

```go
const BlockSize = 64 * 1024  // 64KB 固定
```

不考虑数据点大小，可能导致小数据浪费或大数据跨块

---

## 六、API 设计问题

### 6.1 gRPC 服务端错误处理不一致

**文件**: `internal/api/grpc.go`

```go
// CreateMeasurement 返回 Success=true 即使失败也继续
func (s *MicroTSService) CreateMeasurement(...) {
    _, err := s.engine.CreateMeasurement(...)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "create measurement failed: %v", err)
    }
    return &types.CreateMeasurementResponse{Success: true}, nil
    // ❌ engine.CreateMeasurement 永远不返回错误
}
```

---

### 6.2 DropMeasurement 错误语义混淆

```go
// engine.go:571
func (e *Engine) DropMeasurement(database, measurement string) (bool, error) {
    if !ok {
        return false, fmt.Errorf("database not found: %s", database)
    }
    return dbMeta.DropMeasurement(measurement), nil
}

// api/grpc.go:268
if strings.Contains(err.Error(), "not found") {  // ❌ 字符串匹配判断错误类型
    return nil, status.Errorf(codes.NotFound, ...)
}
```

**问题**: 应该用 `errors.Is()` 或自定义错误类型

---

## 七、安全问题

### 7.1 目录权限检查缺失

**文件**: `internal/storage/util.go`

```go
func SafeMkdirAll(path string, perm uint32) error {
    // 没有检查 path 是否包含 .. 路径遍历
    return os.MkdirAll(path, os.FileMode(perm))
}
```

**建议**: 添加路径安全检查，防止路径遍历攻击

---

### 7.2 缺少输入验证

**文件**: `internal/engine/engine.go`

```go
func (e *Engine) Write(ctx context.Context, point *types.Point) error {
    // ❌ 没有验证 point.Database, point.Measurement 是否为空
    // ❌ 没有验证 timestamp 是否合理（负数等）
}
```

---

## 八、测试覆盖问题

### 8.1 测试文件过多重复代码

`tests/e2e/` 下 15+ 个测试文件，大部分都是样板代码，可以抽象出公共测试框架。

### 8.2 缺少边界条件测试

- 整数溢出边界
- 空数据库/空 Measurement 查询
- 并发写入冲突

---

## 九、代码质量

### 9.1 魔法数字

**文件**: 多处

```go
BlockSize = 64 * 1024           // 应该从配置读取
estimatedSize := int64(len(m.entries)) * 1024  // 假设 1KB/entry
```

### 9.2 注释掉的代码未清理

**文件**: `internal/storage/shard/shard.go` 等多出有 `// TODO`, `// FIXME` 注释未处理

---

## 总结

| 级别 | 数量 | 说明 |
|------|------|------|
| P0 (已修复) | 3 | Close 泄漏、Index 计算错误、Iterator 错误忽略 |
| P1 (严重) | 5 | WAL 同步未启用、无 TTL、无 compaction、MetaStore 不持久化、Close 未 Flush |
| P2 (中等) | 6 | WriteBatch 无原子性、tsSidMap 泄漏、溢出风险、Series ID 无上限 |
| P3 (优化) | 6 | ReadRange 未用索引、排序开销、固定块大小、错误处理不一致 |

### 关键建议

1. **实现 WAL 定期同步** - 数据安全
2. **实现 SSTable compaction 和 TTL** - 存储管理
3. **实现 MetaStore 自动持久化** - 元数据安全
4. **使用 BlockIndex 加速 ReadRange** - 性能优化
