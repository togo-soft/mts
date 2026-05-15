# 写入路径内存分配热点分析与优化方案

**日期**: 2026-05-15
**测试用例**: write_1m_pprof（1M 数据点，每点 10 字段，单 tag `{"host":"server1"}`）

## 最终优化结果

| 指标 | 优化前 | 优化后 | 改善 |
|------|--------|--------|------|
| TotalAlloc | 4.65 GB | 2.08 GB | **-55.3%** |
| Inuse | 70.51 MB | 57.88 MB | **-17.9%** |
| TPS | 215K | 430K | **+100%** |
| GC Cycles | 217 | 99 | **-54.4%** |
| bbolt 分配 | 1.97 GB | 0 | **消除** |
| NewFieldValue | 593 MB | 0 | **消除** |
| deserializeFieldData | 323 MB | 0 | **消除** |

---

## 一、分配热点排名

### P0: bbolt 元数据路径 — 1.97 GB (42.4%)

| 函数 | flat | cum | 说明 |
|------|------|-----|------|
| `bolt.beginTx` | 630 MB | 868 MB | 每次 View/Update 事务分配 |
| `bolt.openBucket` | 430 MB | 676 MB | Bucket 嵌套查找 |
| `bolt.cloneBytes` | 247 MB | 247 MB | bolt 内部 key/value 拷贝 |
| `bolt.Tx.init` | 238 MB | 238 MB | 事务初始化 |
| `bolt.Cursor.search` | 199 MB | 199 MB | B+tree cursor 定位 |
| `lookupSIDReadOnly` | 0 | 1,385 MB | 每点必调，View 事务 |
| `DatabaseExists` | 0 | 396 MB | 每点必调，View 事务 |
| `MeasurementExists` | 0 | 473 MB | 每点必调，View 事务 |
| `unmarshalTags` | 56 MB | 512 MB | JSON 反序列化 tags |
| `json.Unmarshal` | 142 MB | 456 MB | encoding/json |

**根因**: 每次写入都通过 bbolt View 事务查询 metadata（DB 存在性 → Measurement 存在性 → SID 查找），即使 benchmark 只有 1 种 tag 组合。已有 `sync.Map` cache 是按 SID 索引的，无法加速 hash→SID 的查找路径。

### P1: NewFieldValue 分配 — 590 MB (12.7%)

**根因**: `deserializeFieldData` 在 flush 路径中将 MemPoint.FieldData 解码为 `[]InternalField`，每个字段都调用 `NewFieldValue()` 创建 `&FieldValue{Value: &FieldValue_*Value{...}}` 堆分配。10 字段 × 1M = 10M 次 FieldValue 分配。

### P2: serializeFieldsFromMap — 226 MB flat + 36.5 MB inuse (51.8% 驻留)

**根因**: `PointToMemPoint` 为每个点分配新的 `[]byte` 存放序列化字段数据。inuse 36.5 MB 说明这些 buffer 被 MemTable 持有。

### P3: WAL 序列化 — 242 MB + 压缩 84 MB

| 函数 | flat | 说明 |
|------|------|------|
| `serializePointForWAL` | 242 MB | 每点分配完整 WAL 记录 buffer |
| `CompressPayload` | 27 MB | LZ4 压缩缓冲区 |
| `DictEncode` | 54 MB | 字典编码分配 |
| `lz4block.init` | 55 MB | LZ4 内部表 |

### P4: SSTable 写入路径 — 177 MB + 304 MB

| 函数 | flat | cum | 说明 |
|------|------|-----|------|
| `WritePoints` | 177 MB | 200 MB | InternalPoint 批量写入 |
| `encodeFixedFieldSection` | 98 MB | 304 MB | 定长字段列式编码 |

---

## 二、优化方案

### 方案 A: SID 哈希缓存（P0，预计节省 ~1.9 GB）

**现状**: `seriesStore.lookupSIDReadOnly` 每次通过 bbolt View 事务查找。cache 仅按 SID 索引。

**方案**: 新增 `hashSidCache map[uint64]uint64`（tagHash → SID），在 `AllocateSID` 中优先查此缓存。

```go
type seriesStore struct {
    db          *bolt.DB
    cache       sync.Map // key: "db/meas/{sid}" → tags
    hashSidCache sync.Map // key: hashKey(uint64) → sid(uint64), 按 db/meas 隔离
}
```

**效果预估**: 
- write_1m_pprof（1 tag 组合）→ 消除几乎所有 1.9 GB bbolt 分配
- 多 tag 场景也大幅减少 bbolt 查询（命中率取决于 tag 基数）
- 风险：需处理 tag hash 冲突（已有二次验证逻辑）

### 方案 B: deserializeFieldData 池化 FieldValue（P1，预计节省 ~590 MB）

**现状**: 每次反序列化字段值都 `NewFieldValue()` 堆分配。

**方案**: 使用 `sync.Pool` 复用 `FieldValue` 和内部的 `FieldValue_*Value` 结构体。

或者更优方案：**跳过 InternalPoint/InternalField 中间态**，直接从 MemPoint.FieldData 写入 SSTable（类似 Plan 6B 对 PointRow 的优化）。当前 flush 路径：

```
MemPoint → deserializeFieldData → InternalPoint → Writer.WritePoints → SSTable
```

优化为：

```
MemPoint → Writer.WriteMemPoints → SSTable （直接从 FieldData 字节流编码列式数据）
```

### 方案 C: serializeFieldsFromMap Buffer 池化（P2，预计节省 ~226 MB alloc + 36 MB inuse）

**现状**: 每次 `serializeFieldsFromMap` 都 `make([]byte, 0, size)` 分配新 buffer。

**方案**: 基于 `sync.Pool` 的 buffer 池，配合 `PointToMemPoint` 使用池化 buffer：

```go
var fieldDataPool = sync.Pool{
    New: func() any {
        buf := make([]byte, 0, 512)
        return &buf
    },
}
```

需注意：buffer 移交 MemTable 后所有权转移，Swap 时归还到池。

### 方案 D: AllocateSID 批量接口（减少 bbolt 事务次数）

**现状**: `WriteBatch` 中每个 point 仍单独调用 `AllocateSID`，但都已持有 Shard 锁。

**方案**: 新增 `AllocateSIDsBatch(tagsList []map[string]string) ([]uint64, error)`，在单次 bbolt 事务中批量分配。

### 方案 E: DatabaseExists/MeasurementExists 内存缓存

**现状**: 每次写入都通过 bbolt View 事务检查 DB 和 Measurement 是否存在。

**方案**: 在 catalogStore 中维护内存 set，启动时从 bbolt 加载，写入新 DB/Meas 时更新。

---

## 三、优先级与执行顺序

| 优先级 | 方案 | 预计节省 | 复杂度 | 风险 |
|--------|------|---------|--------|------|
| P0 | A: SID hash cache | ~1.9 GB | 低 | 低（有二次验证） |
| P1 | B: 跳过 InternalPoint 中间态 | ~590 MB + 317 MB | 中 | 中（改 flush 路径） |
| P2 | E: DB/Meas 内存缓存 | ~870 MB | 低 | 低 |
| P3 | C: serializeFieldsFromMap 池化 | ~226 MB | 中 | 需管理所有权 |
| P4 | D: AllocateSID 批量接口 | 取决于批量大小 | 中 | 低 |

**建议执行顺序**: A → E → B → C → D

A+E 合计预计节省 ~2.8 GB（60%），实现简单、风险低，应优先执行。
