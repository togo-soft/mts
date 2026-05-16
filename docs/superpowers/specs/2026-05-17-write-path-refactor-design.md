# 写入路径重构设计

**日期**: 2026-05-17  
**状态**: 已确认  
**目标**: 移除写入路径中的数据分批逻辑，数据直接写入 WAL + MemTable；MemTable 落盘到 unordered 目录（未排序），由 compaction 定期分拣排序后写入 stable 目录。

## 1. 动机

### 当前问题

1. 写入数据时按 measurement 分组后批量写入，引入了不必要的分批逻辑
2. MemTable flush 直接写入有序 SSTable，排序职责过早介入写入路径
3. Writer 按 measurement 粒度管理，增加了复杂度和资源开销

### 目标

- 写入路径极简化：Write → WAL + MemTable，不做分批、不排序
- 排序推迟到 compaction 阶段异步完成
- 参照业界时序数据库实践（如 InfluxDB TSM 引擎）

## 2. 架构设计

### 2.1 数据流

```
Write(Point) ──→ WAL(wal_{seq}.wal) + MemTable(active)
                    ↓ Swap (满/空闲/空闲超时)
                passive → Flush → unordered/sst_{seq}.bin (列式,未排序)
                    ↓ WAL段销毁
                Compaction(500ms) → 按(db, meas, shard)分拣排序
                    ↓
                stable/{db}/{meas}/{shard}/L0/sst_{seq}.bin (≤64MB)
                    ↓ Level Compaction
                stable/{db}/{meas}/{shard}/L1/sst_{seq}.bin (≤1GB)
```

### 2.2 目录结构

```
{dataDir}/
  wal/
    wal_000001.wal          # WAL 段文件（全局，按 seq 递增）
    wal_000002.wal
  unordered/
    sst_000001.bin          # Immutable MemTable 落盘（未排序，列式格式）
    sst_000002.bin
  stable/
    {database}/
      {measurement}/
        {shardStart}_{shardEnd}/
          L0/
            sst_000003.bin  # unordered → L0（已排序，≤64MB）
          L1/
            sst_000004.bin  # L0 → L1（已排序，≤1GB）
```

### 2.3 核心原则

1. **unordered 目录** = immutable memtable 集合，所有数据平铺不分子目录
2. **stable 目录** 保留 database/measurement/shard 层级组织
3. **WAL** 全局管理，Flush 完成后销毁对应段
4. **SSTable 格式** 复用现有列式格式，通过 header flag 区分有序/无序
5. **去重** 仅在 L0→L1 compaction 时执行
6. **SSTable 序列号** 全局自增

## 3. 组件变更

### 3.1 移除

| 组件 | 原因 |
|------|------|
| `internal/storage/writer/` | 写入不再需要按 measurement 的 Writer |

### 3.2 重写

| 组件 | 变更内容 |
|------|----------|
| `internal/storage/memtable/` | 改为全局单实例，去掉按 measurement 隔离 |
| `internal/storage/wal/` | 改写为全局 WAL，段文件在 `{dataDir}/wal/` |

### 3.3 新增

| 组件 | 职责 |
|------|------|
| `internal/storage/unordered/` | 管理 `unordered/` 目录下 SSTable 文件的读写、列出、删除 |

### 3.4 修改

| 组件 | 变更内容 |
|------|----------|
| `internal/engine/` | 重写写入路径，去掉 measurement Writer，直接操作全局 WAL + MemTable |
| `internal/storage/compaction/` | 新增 unordered→L0 分拣排序阶段，保留 L0→L1 层级压缩 |
| `internal/storage/shard/` | 简化，去掉 WriteSSTable 直接写入逻辑 |
| `internal/storage/shard/sstable/` | 通过 header flag 区分有序/无序，其余格式复用 |

## 4. 写入路径详细设计

### 4.1 Engine.Write

```
Write(point):
  1. 验证 point 有效性 (nil/空字段/无效时间戳)
  2. 分配 Series ID (SID)
  3. 序列化 point → MemPoint + WAL 二进制
  4. 写 WAL (wal.Write)
  5. 写 MemTable (memtable.Write)
  6. 返回成功
```

### 4.2 Engine.WriteBatch

```
WriteBatch(points):
  1. 批量验证 points
  2. 分配 SID
  3. 逐条序列化，收集 walData 切片
  4. 批量写 WAL (wal.WriteBatch)
  5. 批量写 MemTable
  6. 返回成功
```

### 4.3 MemTable（全局单实例，双缓冲不变）

- **active**: 接收写入
- **passive**: 等待 flush
- **触发条件**: MaxSize(64MB) / MaxCount(50000) / IdleDurationNanos(1min)
- **背压**: ActiveFull() 5x 阈值阻塞写入
- **Swap()**: active ↔ passive，flush 中写入 active 不受影响

### 4.4 Flush 流程

```
FlushCoordinator.flush():
  1. MemTable.Swap() → 获取 passive 数据
  2. 使用 SSTable Writer（unordered flag）将 passive 写入 unordered/sst_{seq}.bin
  3. 成功后 MemTable.ClearPassive()
  4. WAL.TruncateFlushedSegments() → 销毁已 flush 的 WAL 段
  5. 失败时 MemTable.MergePassiveBack() 恢复数据
```

## 5. Compaction 设计

### 5.1 阶段一：Unordered → L0（分拣排序）

- **触发**: 定时 500ms（可配置）
- **流程**:
  1. 扫描 `unordered/` 下所有 `sst_*.bin`
  2. 通过 SSTable Reader 读取每个文件的数据
  3. 按 `(database, measurement, shardStart, shardEnd)` 分组
  4. 每组内按 `(timestamp, sid)` 排序
  5. 写入 `stable/{db}/{meas}/{shard}/L0/sst_{seq}.bin`
  6. 删除已处理的 unordered 文件
- **不去重**: 重复数据处理推迟到 L0→L1

### 5.2 阶段二：L0 → L1（层级压缩）

- **触发**: L0 文件数/大小达到阈值
- **L0 上限**: 64MB/文件
- **L1 上限**: 1GB/文件
- **去重**: 在此阶段执行
- **策略**: L0 取所有文件，与 L1 重叠范围的文件合并排序，写入新 L1

### 5.3 SSTable 格式区分

```
Header Flags 新增:
  0x01 - Unordered (unordered flag)
  0x00 - Sorted (默认，L0/L1)
```

## 6. 查询路径

### 6.1 迭代器合并层

```
Query → Merge(
    MemTable(active) iterator,     # 内存层
    unordered/*.bin iterators,     # 未排序层
    L0 iterators,                  # 有序层
    L1 iterators                   # 有序层
)
```

### 6.2 查询范围裁剪

- **unordered**: 全部扫描（未排序无法按 shard 裁剪）
- **L0/L1**: 按 shard 时间窗口精确裁剪
- **MemTable**: 全量扫描

## 7. 崩溃恢复

```
启动恢复:
  1. 扫描 wal/ 目录，按 seq 排序所有段文件
  2. 依次 replay 每个段到 MemTable
  3. 若 MemTable 达到阈值 → Swap → flush 到 unordered
  4. 恢复完成后，删除已 flush 的 WAL 段
  5. 启动 500ms compaction 定时器
```

## 8. 关键参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| MemTable MaxSize | 64MB | 单次 flush 数据量上限 |
| MemTable MaxCount | 50000 | 单次 flush 记录数上限 |
| MemTable IdleTimeout | 1min | 空闲触发 flush |
| MemTable Backpressure | 5x | 写入背压倍数 |
| WAL SegmentSize | 64MB | 单段 WAL 上限 |
| WAL SyncInterval | 1min | 定时 fsync |
| CompactionInterval | 500ms | unordered→L0 扫描间隔（可配置） |
| L0 MaxFileSize | 64MB | L0 单文件上限 |
| L1 MaxFileSize | 1GB | L1 单文件上限 |

## 9. 验收标准

1. 所有写入直接到 WAL + MemTable，不经过分批/分组
2. MemTable flush 产生 unordered 文件，WAL 段随之销毁
3. 500ms 定时 compaction 正确处理 unordered → L0 分拣排序
4. L0 → L1 层级压缩正常运作
5. 崩溃恢复正确 replay WAL
6. 查询正确合并 MemTable + unordered + L0 + L1
7. 所有 E2E 测试用例通过
8. 单元测试行覆盖率 ≥ 90%
