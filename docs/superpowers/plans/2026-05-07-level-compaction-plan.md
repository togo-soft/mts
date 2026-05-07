# Level Compaction 执行计划

- 计划版本：v1.0
- 创建日期：2026-05-07
- 基于设计：`docs/design/level-compaction-design.md`
- 预计工期：2-3 周

---

## 1. 目标

实现 Level Compaction 机制，替代现有的平坦 Compaction，实现：

1. 层次化的 SSTable 管理（L0 → L1 → L2 → ...）
2. 基于容量的自动触发策略
3. Checkpoint 崩溃恢复机制
4. Tombstone 删除标记处理
5. 旧格式平滑迁移

---

## 2. 阶段划分

```
Phase 1: 基础数据结构 (2-3 天)
Phase 2: Manifest 管理 (2-3 天)
Phase 3: Level Compaction 核心 (3-4 天)
Phase 4: Checkpoint 与恢复 (2 天)
Phase 5: 测试与集成 (3-4 天)
```

---

## 3. 详细任务

### Phase 1: 基础数据结构

#### 1.1 创建 level_compaction.go

**文件**: `internal/storage/shard/level_compaction.go`

**任务**:
- [ ] 定义 `LevelConfig` 结构体和默认配置
- [ ] 定义 `Level` 结构体
- [ ] 定义 `PartInfo` 结构体
- [ ] 定义 `LevelManifest` 结构体
- [ ] 实现 `NewLevelCompactionManager` 构造函数
- [ ] 实现 `levelMaxSize()` 计算函数

#### 1.2 定义 CompactionCheckpoint

**文件**: `internal/storage/shard/level_compaction.go`

**任务**:
- [ ] 定义 `CompactionCheckpoint` 结构体
- [ ] 实现 `Save()` 方法
- [ ] 实现 `Load()` 方法
- [ ] 实现 `Clear()` 方法

#### 1.3 定义 Tombstone

**文件**: `internal/storage/shard/tombstone.go` (新文件)

**任务**:
- [ ] 定义 `Tombstone` 结构体
- [ ] 定义 `TombstoneSet` 结构体
- [ ] 实现 `ShouldDelete()` 方法
- [ ] 实现 `Save()` 方法（写入 _tombstones.json）
- [ ] 实现 `Load()` 方法

---

### Phase 2: Manifest 管理

#### 2.1 实现 LevelManifest

**文件**: `internal/storage/shard/level_manifest.go` (新文件)

**任务**:
- [ ] 实现 `NewLevelManifest()` 构造函数
- [ ] 实现 `GetLevel()` 获取层次
- [ ] 实现 `AddPart()` 添加 Part
- [ ] 实现 `RemovePart()` 删除 Part
- [ ] 实现 `RemoveParts()` 批量删除
- [ ] 实现 `Save()` 持久化到 _manifest.json
- [ ] 实现 `Load()` 从文件加载
- [ ] 实现 `Init()` 初始化新 Manifest

#### 2.2 实现目录结构创建

**文件**: `internal/storage/shard/level_manifest.go`

**任务**:
- [ ] 实现 `EnsureLevelDir()` 确保层次目录存在
- [ ] 实现 `GetLevelPath()` 获取层次路径

---

### Phase 3: Level Compaction 核心

#### 3.1 实现 Part 选择策略

**文件**: `internal/storage/shard/level_compaction.go`

**任务**:
- [ ] 实现 `selectPartsForMerge()` 小文件优先选择
- [ ] 实现 `hasOverlap()` 时间范围重叠检测
- [ ] 实现 `collectOverlapParts()` 收集重叠 Parts

#### 3.2 实现 Compaction 触发检查

**文件**: `internal/storage/shard/level_compaction.go`

**任务**:
- [ ] 实现 `ShouldCompact()` 触发检查
- [ ] 实现 `shouldCompactLevel()` 单层检查
- [ ] 实现 `needCompactionLevel()` 判断层次是否需要合并

#### 3.3 实现 Compaction 流程

**文件**: `internal/storage/shard/level_compaction.go`

**任务**:
- [ ] 实现 `Compact()` 主入口
- [ ] 实现 `compactLevel()` 单层合并
- [ ] 实现 `merge()` 流式合并（复用现有 merge 逻辑）
- [ ] 实现 `commitCompaction()` 提交结果

#### 3.4 实现定时触发

**文件**: `internal/storage/shard/level_compaction.go`

**任务**:
- [ ] 实现 `StartPeriodicCheck()` 启动定时检查
- [ ] 实现 `Stop()` 停止检查
- [ ] 实现 `doPeriodicCompaction()` 定时执行

---

### Phase 4: Checkpoint 与恢复

#### 4.1 实现 Checkpoint 管理

**文件**: `internal/storage/shard/level_compaction.go`

**任务**:
- [ ] 实现 `SaveCheckpoint()` 保存进度
- [ ] 实现 `LoadCheckpoint()` 加载进度
- [ ] 实现 `ClearCheckpoint()` 清理进度

#### 4.2 实现崩溃恢复

**文件**: `internal/storage/shard/level_compaction.go`

**任务**:
- [ ] 实现 `Recover()` 启动时恢复检查
- [ ] 实现 `recoverFromCheckpoint()` 从 Checkpoint 恢复
- [ ] 实现 `cleanupIncompleteOutput()` 清理未完成的输出

#### 4.3 实现格式迁移

**文件**: `internal/storage/shard/level_migration.go` (新文件)

**任务**:
- [ ] 实现 `IsOldFormat()` 检测旧格式
- [ ] 实现 `MigrateFromOldFormat()` 迁移函数

---

### Phase 5: 测试与集成

#### 5.1 单元测试

**文件**: `internal/storage/shard/level_compaction_test.go` (新文件)

**任务**:
- [ ] 测试 LevelManifest 的增删改查
- [ ] 测试 Manifest 持久化
- [ ] 测试小文件优先选择
- [ ] 测试时间范围重叠检测
- [ ] 测试 Checkpoint 保存和加载
- [ ] 测试 Tombstone 判断逻辑
- [ ] 测试 Compaction 触发条件

#### 5.2 集成测试

**文件**: `internal/storage/shard/level_compaction_test.go`

**任务**:
- [ ] 测试 L0 → L1 合并
- [ ] 测试 L1 → L2 合并
- [ ] 测试级联合并
- [ ] 测试 Checkpoint 恢复
- [ ] 测试旧格式迁移
- [ ] 测试并发 Compaction

#### 5.3 集成到 Shard

**文件**: `internal/storage/shard/shard.go`

**任务**:
- [ ] 修改 `ShardConfig` 支持 LevelCompactionConfig
- [ ] 修改 `NewShard` 创建 LevelCompactionManager
- [ ] 修改 `Flush()` 调用新的 compaction
- [ ] 修改 `Close()` 停止 compaction 服务

#### 5.4 golangci-lint 和 goimports

**任务**:
- [ ] 运行 golangci-lint 检查
- [ ] 运行 goimports-reviser 格式化
- [ ] 修复所有 lint 问题

---

## 4. 文件清单

### 新增文件

| 文件 | 用途 |
|------|------|
| `internal/storage/shard/level_compaction.go` | Level Compaction 核心逻辑 |
| `internal/storage/shard/level_manifest.go` | Manifest 管理 |
| `internal/storage/shard/level_migration.go` | 格式迁移 |
| `internal/storage/shard/tombstone.go` | Tombstone 处理 |
| `internal/storage/shard/level_compaction_test.go` | 单元和集成测试 |

### 修改文件

| 文件 | 修改内容 |
|------|----------|
| `internal/storage/shard/shard.go` | 集成 LevelCompactionManager |
| `internal/storage/shard/shard_test.go` | 更新测试 |
| `docs/design/level-compaction-design.md` | 设计文档（已完成） |

---

## 5. 验收标准

### 功能验收

- [ ] L0 → L1 合并正常工作
- [ ] L1 → L2 合并正常工作
- [ ] 级联合并（L0 触发后可能引发 L1 → L2）正常工作
- [ ] Checkpoint 在 Compaction 中保存
- [ ] 重启后能正确恢复未完成的 Compaction
- [ ] 旧格式能平滑迁移到新格式
- [ ] Tombstone 标记的数据在查询时被正确过滤

### 性能验收

- [ ] 流式合并，内存占用稳定（不随数据量增长）
- [ ] Compaction 不阻塞正常写入
- [ ] Compaction 不阻塞正常读取

### 质量验收

- [ ] golangci-lint 0 issues
- [ ] 代码覆盖率达到 85%+
- [ ] 所有单元测试通过
- [ ] 集成测试通过

---

## 6. 风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| 现有代码耦合度高 | 修改可能破坏已有功能 | 充分测试，逐步集成 |
| Checkpoint 文件损坏 | 无法正确恢复 | Manifest 备份机制 |
| 迁移过程出错 | 数据丢失 | 迁移前备份，失败回滚 |
| 并发 compaction | 资源争用 | 单一 compaction 锁 |

---

## 7. 后续优化

- Compaction 限流（避免占用过多 IO）
- 多线程 compaction（进一步提升速度）
- Compaction 进度 API（监控）
- 自动调优（根据数据量动态调整层次配置）

---

## 8. 审核检查点

- [ ] Phase 1 完成：基础数据结构就绪
- [ ] Phase 2 完成：Manifest 管理完成
- [ ] Phase 3 完成：核心 compaction 就绪
- [ ] Phase 4 完成：恢复机制完成
- [ ] Phase 5 完成：测试通过，质量达标
