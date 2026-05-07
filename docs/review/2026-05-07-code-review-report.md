# MTS 项目代码检视报告

**检视日期**: 2026-05-07
**检视范围**: `/root/projects/mts`
**检视人员**: Claude Code

---

## 1. 概述

MTS（Micro-TS）是一个微时序数据库项目，采用类 LSM-tree 架构实现数据存储。本报告对项目核心代码进行系统性检视，发现若干需要关注的问题。

---

## 2. 严重问题（Critical）

### 2.1 for 循环中错误使用 defer

**文件**: `internal/storage/shard/wal.go:804`

```go
func replayWALFile(path string, startPos int64) ([]*types.Point, int64, error) {
    file, err := os.Open(path)
    if err != nil {
        return nil, 0, err
    }
    defer func() { _ = file.Close() }()  // ❌ 问题在这里

    for {
        // ...
        for read < size {
            n, err := file.Read(data[read:])
            if err != nil {
                if err.Error() == "EOF" {
                    break
                }
                return points, pos, err  // defer 会延迟执行，文件在函数结束时才关闭
            }
            read += n
            pos += int64(n)
        }
        // ...
    }
}
```

**问题分析**:
- `defer` 在 `for` 循环内部使用，每次迭代都会注册一个延迟调用
- 当函数返回时，所有 defer 会被执行，导致文件句柄累积
- 根据项目规范 CLAUDE.md，明确禁止在 for 循环中使用 defer

**影响**: 可能导致文件句柄泄露

**建议修复**: 将 defer 移到循环外，或显式管理资源

---

### 2.2 Level Compaction 中 PartInfo 大小未计算

**文件**: `internal/storage/shard/level_compaction.go:699-705`

```go
newPart := PartInfo{
    Name:    fmt.Sprintf("sst_%d", outputSeq),
    Size:    0, // TODO: 计算实际大小  ❌ 未完成
    MinTime: overlaps[0].MinTime,
    MaxTime: overlaps[len(overlaps)-1].MaxTime,
}
```

**问题分析**:
- 新增的 Part 的 Size 被设为 0，导致 Level Manifest 中的 Size 统计不准确
- `shouldCompactLevel()` 和 `levelMaxSize()` 判断会出错

**影响**: Level Compaction 的容量判断逻辑失效，可能导致：
- 该合并时不合并
- 不该合并时却合并

**建议**: 计算实际文件大小

---

## 3. 高风险问题（High）

### 3.1 资源泄露 - Compaction 错误处理

**文件**: `internal/storage/shard/level_compaction.go:729-739`

```go
readers := make([]*sstable.Reader, 0, len(inputPaths))
for _, path := range inputPaths {
    r, err := sstable.NewReader(path)
    if err != nil {
        for _, r := range readers {  // ❌ 如果 NewReader 失败，只关闭已打开的
            _ = r.Close()
        }
        return err
    }
    readers = append(readers, r)
}
```

**问题分析**:
- 如果某个 `sstable.NewReader()` 失败，之前成功打开的 readers 没有被正确关闭
- 循环中的关闭只是调用了 `_ = r.Close()`，没有检查错误

**同样的问题出现在**: `compaction.go:240-250`

---

### 3.2 Level Manifest 加载的竞态条件

**文件**: `internal/storage/shard/level_compaction.go:256-292`

```go
func (m *LevelManifest) Load() error {
    m.mu.Lock()
    defer m.mu.Unlock()
    // ...
    // 恢复 nextSeq（直接赋值，因为 Load 已持有锁）
    m.nextSeq = file.NextSeq  // ❌ 临界区内大量操作
    // ...
}
```

**问题分析**:
- `Load()` 方法持有写锁期间进行了大量操作，可能导致长时间阻塞其他读写操作
- 其他需要 `manifestMu` 的操作（如 `AddPart`、`SaveManifest`）会被阻塞

**建议**: 考虑减小临界区范围，或使用读写锁优化

---

### 3.3 Compaction 结果删除文件错误被忽略

**文件**: `internal/storage/shard/compaction.go:369-373`

```go
// 删除旧文件
for _, oldFile := range task.inputFiles {
    if err := os.RemoveAll(oldFile); err != nil {
        slog.Warn("failed to remove old sstable", "path", oldFile, "error", err)
        // ❌ 只记录警告，错误被忽略
    }
}
```

**问题分析**:
- 删除旧 SSTable 文件失败只记录警告
- 重启后可能加载已删除的文件，导致数据不一致

**建议**: 考虑将错误聚合返回，或至少在日志中标记为更高严重级别

---

### 3.4 WAL TruncateCurrent 后文件句柄位置问题

**文件**: `internal/storage/shard/wal.go:449-471`

```go
func (w *WAL) TruncateCurrent() error {
    w.mu.Lock()
    defer w.mu.Unlock()

    if err := w.flushLocked(); err != nil {
        return err
    }
    if err := w.file.Sync(); err != nil {
        return err
    }

    // 截断文件到 0
    if err := w.file.Truncate(0); err != nil {
        return err
    }
    // 重新定位到文件开头
    if _, err := w.file.Seek(0, 0); err != nil {  // ⚠️ 需要确认 Seek 成功
        return err
    }
    w.fileSize = 0
    return nil
}
```

**问题分析**:
- `TruncateCurrent` 在刷盘后截断并 seek，但 WAL 的状态被重置
- 后续写入会从文件开头开始，但如果有多次 Truncate 操作被跳过，可能有问题

**需要验证**: 多次 Truncate 是否会导致数据丢失

---

### 3.5 QueryIterator.Close() 错误被忽略

**文件**: `internal/engine/engine.go:386-388`

```go
defer func() {
    _ = qit.Close()  // ❌ 忽略 Close 错误
}()
```

**问题分析**:
- 如果 `qit.Close()` 返回错误，defer 中被忽略
- 迭代器可能没有正确释放资源

---

## 4. 中等问题（Medium）

### 4.1 ShardIterator 锁使用不一致

**文件**: `internal/storage/shard/iterator.go`

```go
func (si *ShardIterator) filterRow(row *types.PointRow) *types.PointRow {
    si.mu.RLock()  // 获取读锁
    defer si.mu.RUnlock()
    return si.filterRowLocked(row)  // 调用已持有锁的版本
}
```

**问题分析**:
- `filterRow()` 获取读锁后调用 `filterRowLocked()`
- 但 `Next()` 已经持有写锁，这种嵌套锁可能导致死锁（虽然目前看不会）
- 命名和锁设计不够清晰

**建议**: 统一锁的使用模式，避免混淆

---

### 4.2 Shard.Close() 双重逻辑

**文件**: `internal/storage/shard/shard.go:683-768`

```go
func (s *Shard) Close() error {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.levelCompaction != nil {
        if err := s.flushLocked(); err != nil {  // 使用 flushLocked
            // ...
        }
    } else {
        // 平坦 Compaction 的刷盘逻辑 - 重复实现了 flushLocked 的部分逻辑
        points := s.memTable.Flush()
        // ... 大量重复代码
    }
}
```

**问题分析**:
- `Close()` 中对 Level Compaction 和平坦 Compaction 实现了不同的刷盘逻辑
- 当 `levelCompaction != nil` 时调用 `flushLocked()`，否则重新实现同样的逻辑
- 代码重复，维护困难

**建议**: 统一使用 `flushLocked()`，或重构为更清晰的结构

---

### 4.3 临时测试二进制文件未加入 .gitignore

**文件**: 项目根目录

```
compaction_test
grpc_write_query
integrity
persistence_test
retention_test
simple_integrity
wal_test
write_1k
```

**问题**: 这些是 e2e 测试编译后的二进制文件，应该加入 `.gitignore`

---

### 4.4 ErrShortWrite 错误定义但未被充分使用

**文件**: `internal/storage/shard/wal.go:281-284`

```go
var ErrShortWrite = fmt.Errorf("short write")
```

**分析**: 这个错误在 `flushLocked()` 中被使用，但调用者没有针对这个错误的特殊处理

---

### 4.5 MetaStore.Persist() 在 Close 时的行为

**文件**: `internal/storage/measurement/meas_meta.go:348-361`

```go
func (m *MeasurementMetaStore) Close() error {
    if m.persistPath != "" && m.dirty {
        if err := m.Persist(); err != nil {
            return fmt.Errorf("persist before close: %w", err)
        }
    }
    // ...
}
```

**分析**: `Close()` 可能在持有锁的情况下调用 `Persist()`，而 `Persist()` 也会获取锁，存在锁重入问题（虽然 Go 的 RWMutex 不是递归锁，但这里用的是 RWMutex，不会有死锁）

---

## 5. 低优先级问题（Low）

### 5.1 魔法数字

**文件**: 多处

```go
// wal.go:22
const walFileSizeLimit = 64 * 1024 * 1024 // 64MB  ✓ 已命名

// wal.go:831
if size > 1024*1024*1024 { // 超过 1GB  ✓ 有注释

// compaction.go:289
const batchSize = 1000 // 每 1000 条批量写入  ✓ 已命名
```

大部分魔法数字已有命名，但部分可以进一步抽象

---

### 5.2 注释掉的测试代码

**文件**: `mts.go:15`, `mts.go:201`, `mts.go:383`

```go
//	defer db.Close()
```

**分析**: 被注释掉的 defer 调用，可能是遗留代码

---

## 6. 代码质量亮点

### 6.1 优点

1. **完善的文档注释**: 核心类型和函数都有详细的文档说明
2. **良好的错误包装**: 使用 `fmt.Errorf("...: %w", err)` 模式
3. **正确的权限设置**: 目录 0700，文件 0600，符合安全要求
4. **原子性文件操作**: 使用临时文件+rename模式
5. **定期同步机制**: WAL 的 `StartPeriodicSync` 设计合理
6. **Checkpoint 支持**: Level Compaction 和 WAL 都有增量恢复机制
7. **并发安全设计**: 使用读写锁保护共享状态

---

## 7. 架构观察

### 7.1 双 Compaction 系统并存

项目同时实现了：
- **平坦 Compaction** (`CompactionManager`)
- **Level Compaction** (`LevelCompactionManager`)

通过 `levelCompaction != nil` 开关切换。

**问题**:
- 两套系统代码重复
- `flushLocked()` 中对两种情况有不同处理
- `Close()` 中也有双重逻辑

**建议**: 考虑统一为 Level Compaction，或清晰分离职责

---

### 7.2 SSTable 读取全量加载

**文件**: `internal/storage/shard/sstable/reader.go`

```go
func (r *Reader) ReadRange(startTime, endTime int64) ([]*types.PointRow, error) {
    // 一次性读取所有数据到内存
}
```

**问题**: 对于大数据集，可能导致内存压力

**建议**: 实现流式读取接口

---

## 8. 总结

### 8.1 关键发现

| 严重程度 | 数量 | 描述 |
|---------|------|------|
| Critical | 2 | for 循环中使用 defer, PartInfo.Size 未计算 |
| High | 5 | 资源泄露风险, 竞态条件, 错误处理不一致 |
| Medium | 4 | 锁使用不一致, 代码重复, 二进制文件未忽略 |
| Low | 3 | 魔法数字, 注释代码, ErrShortWrite |

### 8.2 建议优先级

1. **立即修复**: for 循环中的 defer 问题
2. **尽快修复**: PartInfo.Size 计算、Compaction 资源泄露
3. **计划修复**: 双重 Compaction 系统重构、锁优化
4. **后续改进**: 流式读取、代码重复消除

### 8.3 测试覆盖

项目有较完善的单元测试和 e2e 测试，但仍建议：
- 增加并发场景测试（race condition 检测）
- 增加边界条件测试
- 运行 `go test -race` 检测竞态

---

## 9. 附录

### 9.1 相关文件

- `internal/storage/shard/wal.go` - WAL 实现
- `internal/storage/shard/level_compaction.go` - Level Compaction
- `internal/storage/shard/compaction.go` - 平坦 Compaction
- `internal/storage/shard/shard.go` - Shard 核心
- `internal/storage/shard/iterator.go` - 迭代器
- `internal/engine/engine.go` - 引擎

### 9.2 检测命令

```bash
# 运行 race 检测
go test -race ./internal/storage/shard/...

# 运行 linter
golangci-lint run ./...

# 运行覆盖率
go test -cover ./internal/storage/shard/...
```
