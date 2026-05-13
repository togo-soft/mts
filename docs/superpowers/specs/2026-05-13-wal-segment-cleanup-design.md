# WAL Segment 自动清理设计

## 1. 概述

**问题**：WAL segment 从创建到 Shard Close 之前永远不会被清理。每个 segment 最大 64MB，写密集型场景下数小时内可累积数十 GB，长期运行的 Shard（默认 7 天窗口）磁盘空间浪费严重。

**根因**：`TruncateCurrent()` 和 `Purge()` 已实现但从未在正常运行路径中调用。`shard_flush.go:119-121` 注释称「WAL segment 清理由 compaction 模块负责」，但 compaction 模块中完全没有 WAL 清理代码。**唯一清理时机是 Shard.Close()。**

**目标**：每次 MemTable flush 成功后自动清理 WAL segment，使 WAL 磁盘占用与 memtable 大小成比例而非随时间无限增长。

---

## 2. 安全性分析

### 2.1 核心不变式

> **WAL segment 可以安全删除，当且仅当该 segment 中的所有数据记录已持久化到 SSTable 文件中。**

### 2.2 为什么 flush 后可以安全清理全部 WAL

flush 期间的关键保证：

```
Shard.Write() 持有 s.mu.Lock()
  → WAL.Write(record)     // WAL 追加
  → memTable.Write(point) // MemTable 追加
  → flushLocked():        // 仍持有 s.mu
      → memTable.Flush()  // 窃取全部 entries
      → SSTable.WritePoints()  // 写入临时文件
      → w.Close()         // 编码+压缩+fsync → .bin 文件
  → s.mu.Unlock()
```

- `s.mu.Lock()` 保证了 flush 期间没有新的 WAL 写入
- `Flush()` 窃取了所有内存数据 → SSTable 包含了 WAL 中全部记录对应的数据
- **结论：flush 成功后，WAL 中所有 segment 的每一条记录都已持久化到 SSTable，全部 segment 可安全删除**

### 2.3 例外：WAL Replay 期间

`ReplayWAL()` 在遍历 WAL segment 时可能触发 `flushLocked()`。此时正在读取 segment 文件，不能删除它们。通过 `s.replaying` 标志跳过。

---

## 3. 设计

### 3.1 WAL 新增方法：`TruncateAfterFlush()`

在 `internal/storage/wal/wal.go` 新增：

```go
// TruncateAfterFlush 在 flush 成功后清理所有 WAL segment。
// 操作：flush buffer → 关闭当前 segment → 创建新空 segment → 删除所有旧 segment。
// 调用前需确保所有 WAL 数据已持久化到 SSTable。
func (w *WAL) TruncateAfterFlush() error {
    w.mu.Lock()
    defer w.mu.Unlock()

    // 1. 刷写缓冲 + 关闭当前 segment + 创建新空 segment
    if err := w.rotateLocked(); err != nil {
        return fmt.Errorf("rotate before truncate: %w", err)
    }

    // 2. 删除所有旧 segment（保留刚创建的新空 segment）
    entries, err := listSegments(w.dir)
    if err != nil {
        return fmt.Errorf("list segments: %w", err)
    }
    for _, e := range entries {
        if e.Gen == w.gen && e.Num == w.segNum {
            continue // 保留当前空 segment
        }
        if err := os.Remove(e.Path); err != nil {
            w.cfg.Logger.Warn("failed to remove old WAL segment",
                "path", e.Path, "error", err)
        }
    }

    return nil
}
```

**为什么用 `rotateLocked()` 而不是直接删除**：
- `rotateLocked()` 先 flush buffer → sync → close 当前 segment → 创建新 segment
- 这保证了当前 segment 的数据完整性（已 sync），然后创建一个干净的空 segment
- 新 segment 成为"当前"，旧的全部可以安全删除

### 3.2 调用位置：`flushLocked()`

在 `internal/storage/shard/shard_flush.go` 的 `flushLocked()` 末尾添加：

```go
func (s *Shard) flushLocked() error {
    // ... 现有 SSTable 写入逻辑（不变）...

    // 清理 WAL segment：flush 成功后所有 WAL 数据已持久化到 SSTable
    // replay 期间跳过，因为 ReplayWAL 正在遍历 segment 文件
    if !s.replaying && s.wal != nil {
        if err := s.wal.TruncateAfterFlush(); err != nil {
            slog.Warn("failed to truncate WAL after flush", "error", err)
        }
    }

    return nil
}
```

### 3.3 现有 `closeWithLock()` 保持不变

`closeWithLock()` 中已有的 `wal.Close()` + `wal.Purge()` 逻辑不变。由于 `flushLocked()` 已在正常操作中清理了 WAL，Close 时的 Purge 通常只需删除少量（甚至零个）segment，起到兜底作用。

---

## 4. 边界条件

| 场景 | 行为 |
|------|------|
| **正常运行中 flush** | `TruncateAfterFlush()` 删除所有旧 segment，保留新空 segment |
| **WAL replay 中 flush** | `replaying=true`，跳过清理，保护正在遍历的 segment |
| **flush 失败** | 函数提前 return error，不执行 WAL 清理，数据仍在 WAL 中 |
| **TruncateAfterFlush 失败** | 记录 Warn 日志，不使 flush 失败。WAL segment 暂时堆积，下次 flush 重试 |
| **Shard Close** | `Purge()` 作为最终兜底，删除所有剩余 segment |
| **崩溃恢复** | 重启后 WAL replay 从残存 segment 恢复数据，SSTable 中的重复数据由上层去重处理 |

### 4.1 崩溃场景详细分析

**场景 A：SSTable 写入成功，TruncateAfterFlush 之前崩溃**
- SSTable 文件完整 → 数据在 SSTable 中
- WAL segment 完整 → 重启后 WAL replay 将相同数据写入 memtable
- memtable flush → 产生新的 SSTable → 与已有 SSTable 有重叠数据 → **compaction merge 时去重**
- 结论：无数据丢失，有短暂重复（compaction 自动处理）

**场景 B：TruncateAfterFlush 执行中崩溃（部分 segment 已删除）**
- 已删除的 segment 数据已在 SSTable 中 → 安全
- 未删除的 segment 重启后 replay → 产生重复数据 → compaction 去重
- 结论：无数据丢失

**场景 C：当前空 segment 是唯一存在的 segment**
- 重启后 `Open()` 发现 1 个 segment，`segNum` 从 `Num+1` 继续
- 空 segment 在 replay 时没有有效数据记录 → 安全

---

## 5. 不改的范围

- **不改变 WAL 写入路径**：Write/WriteBatch/Rotate 逻辑完全不变
- **不改变 WAL replay 逻辑**：Replay 保持从头回放所有 segment
- **不改变 compaction 逻辑**：compaction 仍然不碰 WAL
- **不改变 `TruncateCurrent()` 和 `Purge()` 现有方法**：保留作为公共 API（测试/运维使用）

---

## 6. 涉及文件

| 文件 | 改动 |
|------|------|
| `internal/storage/wal/wal.go` | 新增 `TruncateAfterFlush()` 方法 |
| `internal/storage/wal/wal_test.go` | 新增测试：正常清理、空 WAL、并发安全 |
| `internal/storage/shard/shard_flush.go` | `flushLocked()` 末尾添加 WAL 清理调用 |

---

## 7. 测试计划

### 7.1 单元测试：`TruncateAfterFlush()`

1. **基本清理**：写入 N 条记录，调用 TruncateAfterFlush，验证只剩 1 个空 segment
2. **空 WAL**：新创建的 WAL 调用 TruncateAfterFlush，验证不报错
3. **多 segment 场景**：写入足够数据触发多次 rotate（>=3 个 segment），调用 TruncateAfterFlush，验证只剩 1 个
4. **继续写入**：TruncateAfterFlush 后继续写入，验证数据正确持久化
5. **崩溃恢复**：TruncateAfterFlush 后模拟崩溃重启，WAL replay 验证数据完整性

### 7.2 集成测试：flush → WAL cleanup

1. **单次 flush**：写入 3000 点触发 flush，验证 WAL segment 被清理
2. **多次 flush**：连续触发 3 次 flush，验证每次 flush 后 WAL 目录干净
3. **replay 路径**：验证 WAL replay 期间的 flush 不会清理 segment
4. **flush 失败**：模拟 SSTable 写入失败，验证 WAL segment 保留

### 7.3 E2E 测试

现有 E2E 测试（`persistence_test`, `restart_recovery`, `integrity`）全部通过，验证无回归。

---

## 8. 收益

| 场景 | Before | After |
|------|--------|-------|
| 单 Shard 运行 24h（1K write/s） | WAL ≈ 86GB（1344 segments × 64MB） | WAL ≈ 64KB（1 个空 segment） |
| 重启恢复时间 | Replay 1344 segments | Replay 0-1 segment |
| Close 时 Purge 耗时 | 删除 1344 个文件（可能数秒） | 删除 1 个文件（毫秒） |
