# 内存分配优化分析报告

> 日期：2025-05-14
> 分析范围：全项目内存分配热点
> 触发现象：大规模数据写入时内存阶梯性上涨，pprof 无泄漏

---

## 一、问题根因分析

### 1.1 为什么 pprof 看不到泄漏但内存持续上涨？

根本原因不是泄漏，而是 **频繁大块分配 + Go 运行时内存归还机制**：

| 阶段 | 说明 |
|------|------|
| **分配** | 每次写入/查询/压缩产生大量临时 `[]byte` 和 `map` |
| **GC 回收** | Go GC 正常回收，heap profile 无异常 |
| **OS 不回收** | Linux 默认 `MADV_FREE` 模式，Go 归还给 OS 的物理页被标记为"可回收"但不会被立即回收，RSS 维持高位 |
| **阶梯现象** | 每轮大批量写入 → 分配峰值 → GC 释放 → RSS 不降 → 下一轮再次叠加 |

### 1.2 阶梯性上涨的触发链路

```
WriteBatch(10K points)
  → 10K × serializeInternalPoint (make([]byte, ...))
  → 10K × CompressPayload (make([]byte, 5+len*2))
  → 10K × WAL.Write (make([]byte, recordSize))
  → 10K × MemTable.Write (make([]InternalField, ...))
  → MemTable 满了 → Flush → SSTable Writer
    → N blocks × 64KB field buffers
  → Compaction 触发
    → 百万级 de-dup map
```

每一环都产生大量临时分配，GC 后这些内存虽然"可用"但 OS 未回收，进程 RSS 保持高位。下一轮写入在此基础上继续分配，形成阶梯。

---

## 二、按影响等级分类的内存分配热点

### P0：写入热路径（每个数据点触发，影响最大）

#### 1. MemTable.Write 字段深拷贝
**文件**: `internal/storage/memtable/memtable.go:58-59`
```go
fields := make([]types.InternalField, len(ip.Fields))
copy(fields, ip.Fields)
```
**问题**: 每个点都分配新切片 + copy。如果字段数=10，每秒 100 万点写入 → 每秒 1000 万次字段分配。
**优化**: 直接持有传入的 `ip.Fields` 切片（调用方已不再使用）；或使用池化 `[]InternalField`。

#### 2. PointToInternal 字段切片分配
**文件**: `types/internal.go:19`
```go
fields := make([]InternalField, 0, len(p.Fields))
```
**问题**: 每个外部 Point 转 InternalPoint 都分配新字段切片。
**优化**: 与第 1 点合并考虑——如果 MemTable 不再 copy，此处可保持不变；否则池化。

#### 3. WAL 序列化缓冲区
**文件**: `internal/storage/shard/wal_serialize.go:36`
```go
buf := make([]byte, 0, size)
```
**问题**: 每个点都分配一个新的序列化 buffer。对于 10 字段的点大约 100-200 字节。
**优化**: 使用 `sync.Pool` 池化序列化 buffer。

#### 4. WAL 压缩缓冲区（最浪费）
**文件**: `internal/storage/wal/compress.go:19`
```go
dst := make([]byte, 5+len(payload)*2) // 2x 预分配!
```
**问题**: 每个 WAL 写入分配 2×(payload+5) 字节。如果 payload=150B，分配 305B。压缩不划算时再分配一份 `make([]byte, 5+len(payload))`（共两份！）。
**优化**: 使用 `sync.Pool` 池化压缩 buffer，按大小分级。

#### 5. WAL record 缓冲区
**文件**: `internal/storage/wal/wal.go:145,211`
```go
record := make([]byte, recordSize)
```
**问题**: `Write` 和 `WriteBatch` 每条记录分配一次。
**优化**: 在 `WriteBatch` 中可复用 buffers；使用池化。

#### 6. WAL 反序列化字段切片（Replay 路径）
**文件**: `internal/storage/shard/wal_serialize.go:109`
```go
fields := make([]types.InternalField, 0, fieldCount)
```
**问题**: Replay 时每个点分配一次。
**优化**: 池化字段切片。

---

### P1：查询热路径（每行返回触发）

#### 7. SSTable Iterator.Point() 每行 Field map
**文件**: `internal/storage/shard/sstable/iterator_next.go:44`
```go
Fields: make(map[string]*types.FieldValue),
```
**问题**: 每次 `Point()` 调用都分配新 map。查询 100 万行 = 100 万个 map 分配。
**优化**: 延迟到调用方真正需要 map 时才分配；或复用 map（clear + reuse）。

#### 8. ShardIterator.pointToRow 每行 Tags map
**文件**: `internal/storage/shard/iterator.go:100`
```go
tags := make(map[string]string)
```
**问题**: 每个查询行都分配 tags map。如果每行 tags=5，大量浪费。
**优化**: 考虑在 PointRow 中直接存储 `[]InternalTag` 而非 `map[string]string`（与 InternalPoint 同理）。

#### 9. InternalFieldsToMap
**文件**: `types/internal.go:35`
```go
m := make(map[string]*FieldValue, len(fields))
```
**问题**: 每次将内部字段转回 map 都分配新 map。
**优化**: 与第 8 点联动——如果查询路径全程使用 slice 而非 map。

#### 10. FieldValue 包装数组
**文件**: `internal/storage/shard/sstable/reader_blocks.go:77-107`
```go
func float64ValuesToFieldValues(vals []float64) []*types.FieldValue {
    result := make([]*types.FieldValue, len(vals))
    for i, v := range vals {
        result[i] = types.NewFieldValue(v)
    }
    return result
}
```
**问题**: 解码后对每个值调用 `NewFieldValue`，每个返回一个 `*FieldValue` 指针（独立小对象分配）。对于 64KB block（~8000 rows），每列分配 8000 个 `*FieldValue`。
**优化**: 考虑使用连续内存布局或池化 FieldValue。

---

### P2：SSTable 块级别分配

#### 11. 逐 block 读取缓冲区
**文件**: `internal/storage/shard/sstable/reader_blocks.go:140,177,216,282,312`
```go
data := make([]byte, size) // 每个 block、每种字段各一次
```
**问题**: 每个 block 读取都分配新 buffer，解压后可能再分配一份。
**优化**: 池化 block 读取/解压 buffer。

#### 12. Block 加载时的字段数据 map
**文件**: `internal/storage/shard/sstable/iterator_block.go:36`
```go
it.blockFieldData = make(map[string][]byte, len(fieldNames))
```
**问题**: 每个 block 加载时分配新 map。
**优化**: 复用 map（clear + refill）。

#### 13. SSTable Writer 字段缓冲区
**文件**: `internal/storage/shard/sstable/writer_field.go:42`
```go
w.fieldBufs[name] = make([]byte, 0, w.blockSize) // 64KB 每字段
```
**问题**: 如果 5 个字段 = 5 × 64KB = 320KB 分配。
**优化**: 池化这些 block buffer，写入完成后回收。

---

### P3：Compaction 路径

#### 14. De-dup map 无限增长
**文件**: `internal/storage/compaction/merge.go:110` 和 `level.go:350`
```go
seen := make(map[uint64]bool)
```
**问题**: 合并 100 万行需要 100 万 map 条目，约 32MB+。合并完成后释放但 OS 不回收。
**优化**: 根据数据时间范围特性和 SID 分布，考虑使用 bloom filter 预过滤 + 小范围精确去重。

#### 15. 合并时逐行 InternalPoint 构造
**文件**: `internal/storage/compaction/merge.go:144-150`
```go
ip := types.InternalPoint{
    Timestamp: row.Timestamp,
    Fields:    types.MapToInternalFields(row.Fields), // 分配新切片
    Sid:       row.Sid,
}
```
**问题**: 合并 100 万行 = 100 万次 `MapToInternalFields` 调用。
**优化**: 合并路径避免 map→slice 转换。

---

## 三、优化方案

### 3.1 立即可做（低风险、高收益）

#### 方案 1：消除 MemTable.Write 中的字段深拷贝
```go
// 之前
fields := make([]types.InternalField, len(ip.Fields))
copy(fields, ip.Fields)

// 之后：直接持有（调用方 PointToInternal 的返回值已不被使用）
m.active = append(m.active, ip)
```
**收益**: 消除每个写入点 1 次 slice 分配。
**风险**: 需确认所有调用方在 Write 后不再修改 ip.Fields。

#### 方案 2：WAL 序列化 buffer 池化
```go
var serialBufPool = sync.Pool{
    New: func() any {
        buf := make([]byte, 0, 256)
        return &buf
    },
}

func serializeInternalPoint(ip types.InternalPoint) ([]byte, error) {
    bufPtr := serialBufPool.Get().(*[]byte)
    buf := (*bufPtr)[:0]
    defer func() {
        *bufPtr = buf[:0]
        serialBufPool.Put(bufPtr)
    }()
    // ... append 操作 ...
    result := make([]byte, len(buf))
    copy(result, buf)
    return result, nil
}
```
**收益**: 消除每个写入点 1 次序列化 buffer 分配。
**注意**: 返回的 `[]byte` 仍需独立副本（被 WAL 的 record 持有）。

#### 方案 3：WAL 压缩 buffer 池化
```go
var compressBufPool = sync.Pool{
    New: func() any {
        buf := make([]byte, 0, 4096)
        return &buf
    },
}
```
**收益**: 消除每个 WAL 写入的压缩 buffer 分配。
**风险**: 需处理好 buffer 生命周期。

### 3.2 中期优化（需设计）

#### 方案 4：查询路径 map 分配优化

将 `PointRow.Fields` 从 `map[string]*FieldValue` 改为 `[]InternalField`，消除查询路径中的 `InternalFieldsToMap` 转换。

```go
type PointRow struct {
    Sid       uint64
    Timestamp int64
    Tags      map[string]string
    Fields    []InternalField  // 替代 map[string]*FieldValue
}
```

**收益**: 
- 每行查询消除 1 个 map 分配（InternalFieldsToMap）
- 每行 SSTable iterator 消除 1 个 map 分配
- 减少 GC 压力

**风险**: 涉及公开 API 变更（`PointRow`），需评估影响范围。

#### 方案 5：SSTable block buffer 池化

为 `reader_blocks.go` 中的逐 block 读取 buffer 引入池化：
```go
var blockReadPool = sync.Pool{
    New: func() any { return make([]byte, 0, 65536) },
}
```

#### 方案 6：Compaction de-dup 内存优化

将 `map[uint64]bool` 改为分段 bloom filter + 小窗口精确去重：
```go
// 利用数据按时间排序的特性，只需在滑动窗口内精确去重
type slidingDedup struct {
    window    map[uint64]bool
    windowSize int
}
```

### 3.3 长期优化（架构级）

#### 方案 7：引入全局 BufferPool

参考 VictoriaMetrics 的 `ByteBuffer` 设计，实现带大小分级的全局 buffer pool：
- Small: ≤ 256B
- Medium: ≤ 4KB  
- Large: ≤ 64KB
- Huge: > 64KB (rare)

#### 方案 8：GOGC 调优

在当前代码未大改前，可通过调整 `GOGC` 让 GC 更频繁触发（减少峰值但增加 CPU）：
```bash
GOGC=50  # 默认 100，降低到 50 使 GC 更频繁触发
GOMEMLIMIT=2GiB  # 设置软内存上限，Go 1.19+
```

---

## 四、优化优先级矩阵

| 优先级 | 方案 | 预期内存降幅 | 实现难度 | 风险 |
|--------|------|------------|---------|------|
| P0 | 方案 2: WAL 序列化池化 | 10-15% | 低 | 低 |
| P0 | 方案 3: WAL 压缩池化 | 10-15% | 低 | 低 |
| P0 | 方案 1: 消除 MemTable 字段 copy | 5-8% | 低 | 中 |
| P1 | 方案 4: PointRow Fields 改为 slice | 20-30% (查询) | 中 | 高 |
| P1 | 方案 5: Block buffer 池化 | 5-10% | 中 | 低 |
| P2 | 方案 6: Compaction de-dup | 15-25% (合并时) | 中 | 中 |
| P3 | 方案 8: GOGC 调优 | 即时见效 | 低 | 低 |

---

## 五、关键代码位置索引

| 文件 | 行号 | 分配类型 | 频率 | 影响 |
|------|------|---------|------|------|
| `memtable/memtable.go` | 58-59 | `[]InternalField` copy | 每写入点 | P0 |
| `types/internal.go` | 19 | `[]InternalField` 新建 | 每写入点 | P0 |
| `shard/wal_serialize.go` | 36 | `[]byte` 序列化 buffer | 每写入点 | P0 |
| `wal/compress.go` | 19 | `[]byte` 压缩 buffer (2x) | 每 WAL 写 | P0 |
| `wal/wal.go` | 145,211 | `[]byte` record buffer | 每 WAL 写 | P0 |
| `shard/wal_serialize.go` | 109 | `[]InternalField` 反序列化 | 每 replay 点 | P0 |
| `sstable/iterator_next.go` | 44 | `map[string]*FieldValue` | 每查询行 | P1 |
| `shard/iterator.go` | 100 | `map[string]string` tags | 每查询行 | P1 |
| `types/internal.go` | 35 | `map[string]*FieldValue` | 每查询行 | P1 |
| `sstable/reader_blocks.go` | 77-107 | `[]*FieldValue` wrapper | 每 block 解码 | P1 |
| `sstable/reader_blocks.go` | 140,177,216,282,312 | `[]byte` block 读取 | 每 block | P2 |
| `sstable/iterator_block.go` | 36 | `map[string][]byte` | 每 block 加载 | P2 |
| `sstable/writer_field.go` | 42 | `[]byte` field buffer (64KB) | 每字段每 block | P2 |
| `compaction/merge.go` | 110 | `map[uint64]bool` de-dup | 每次合并 | P3 |
| `compaction/level.go` | 350 | `map[uint64]bool` de-dup | 每次合并 | P3 |
| `compaction/merge.go` | 144-150 | `InternalPoint` + `MapToInternalFields` | 每合并行 | P3 |

---

## 六、总结

项目的内存阶梯性上涨**不是内存泄漏**，而是由于：

1. **写入路径每点 5-7 次堆分配**，大量临时 buffer 被 GC 回收后 RSS 不降
2. **查询路径每行 2-3 次 map 分配**，在大数据量查询时产生大量 GC 压力
3. **零 pool 使用**——所有临时 buffer 都通过 `make()` 新建，GC 频繁触发
4. **Compaction de-dup map** 在百万级合并时一次分配数十 MB

**建议执行顺序**：
1. 先实施方案 8（GOGC/GOMEMLIMIT 调优），立即缓解症状
2. 然后实施方案 2 + 3（WAL buffer 池化），这是收益最大、风险最低的优化
3. 再实施方案 1（消除 MemTable copy），需要仔细验证调用方的生命周期
4. 最后评估方案 4（PointRow 改造），需要评估公开 API 影响
