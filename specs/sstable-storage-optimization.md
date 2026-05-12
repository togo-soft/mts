# SSTable 存储优化分析报告

> 日期: 2026/05/12
> 背景: 与 InfluxDB、VictoriaMetrics 等时序数据库对比，MTS 同等数据量级下磁盘空间占用 2-3 倍甚至更高

---

## 一、当前实现概述

MTS 的 SSTable 已实现多种列式编码：

| 数据类型 | 编码方式 | 状态 |
|---------|---------|------|
| 时间戳 (int64) | Delta-of-Delta + Varint | ✅ 已实现 |
| SID (uint64) | Varint (已优化为 Delta) | ✅ 已优化 |
| float64 | Gorilla XOR | ✅ 已实现 |
| int64 | ZigZag + Varint | ✅ 已实现 |
| string | 字典编码 (回退Raw) | ✅ 已实现 |
| bool | Bitmap | ✅ 已实现 |

---

## 二、与 InfluxDB / VictoriaMetrics 的主要差距

### 1. 缺少块级通用压缩 🔴 **最关键**

**现状**: 只做了列式编码，没有在编码后应用通用压缩算法。

**InfluxDB/VM 的做法**:
- InfluxDB: 使用 Snappy 压缩 block 数据
- VictoriaMetrics: 使用 LZ4 压缩（部分场景用 zstd）

**差距**: 列式编码只能压缩同类型数据的模式，通用压缩算法可以发现跨列/跨 block 的重复模式，进一步压缩 20-40%。

```go
// 当前流程
RowData → Columnar Encoding → File

// 应该的流程
RowData → Columnar Encoding → Block Compression (LZ4/zstd) → File
```

### 2. 时间戳存储方式可优化 🟡

**现状**: 第一个时间戳存储为 Varint 编码的绝对值。

**分析**: Delta-of-Delta 对等间隔时间序列已接近最优，但第一个值（绝对时间戳）仍需较多字节。

**更优方案**: 使用块起始时间作为参考，所有时间戳都是相对值。

### 3. Block 大小固定 64KB 🟡

**现状**: `const BlockSize = 64 * 1024`

**行业做法**:
- InfluxDB OSS: 64KB (可配置)
- VictoriaMetrics: 4KB page + 32KB block (LZ4 压缩后)
- InfluxDB TSM: 64KB (Snappy 压缩后)

**问题**: 64KB 可能太小，更大的 block（如 256KB-1MB）通常能提供更好的压缩率。

### 4. 字符串字典编码的限制 🟡

**现状**: 字典编码只在 block 级别有效，无法跨 block 共享。

**VM 的做法**: 使用全局字典对 tag 值进行压缩，效果更好。

### 5. 无 Series 级别排序优化 🔴

**InfluxDB/VM 的关键优化**: 数据按 metric name + tag keys + tag values 排序后存储，相同 series 的数据物理相邻，重复值多，压缩效果好。

---

## 三、各优化项收益估算

| 优化项 | 预期收益 | 复杂度 | 优先级 |
|-------|---------|--------|--------|
| 块级 LZ4/zstd 压缩 | 20-40% 空间节省 | 中 | 🔴 高 |
| SID Delta 编码 | 50-66% SID 节省 | 低 | ✅ 已完成 |
| Reference Timestamp | 5-15% 时间戳节省 | 低 | 🟡 待定 |
| Series 排序优化 | 10-30% 整体节省 | 高 | 🔴 高 |
| 全局字典 (tag values) | 15-25% 字符串节省 | 中 | 🟡 中 |

**综合**: 如果实现所有优化，预计可达到 **2-3x** 的空间节省，与 InfluxDB/VM 水平相当。

---

## 四、已完成的优化

### SID Delta 编码

**实现日期**: 2026/05/12

**编码格式**:
```
原始 SID: [1000000, 1000001, 1000002, 1000003]

旧编码 (Varint):  [varint(1000000), varint(1000001), varint(1000002), varint(1000003)]
                 ≈ 12 字节

新编码 (Delta):  [varint(1000000), varint(1), varint(1), varint(1)]
                 ≈ 6 字节
```

**压缩效果**:
```
1000 递增 SID: Delta=1002 bytes, Varint=3000 bytes, 节省=66.6%
```

**修改文件**:
- `internal/storage/shard/compression/encode.go`: 新增 `EncodeSidsDelta`、`DecodeSidsDelta`
- `internal/storage/shard/sstable/writer_close.go`: 使用 Delta 编码
- `internal/storage/shard/sstable/reader_blocks.go`: 使用 Delta 解码

---

## 五、推荐优化顺序

### 短期（高优先级）

1. **块级通用压缩 (LZ4/zstd)** - 收益最大，实现相对简单
2. **Reference Timestamp** - 改动小，收益明确

### 中期

3. **Series 排序优化** - 需要较大改动，但收益显著
4. **全局字典编码** - 对 tag values 效果好

### 长期

5. **自适应 Block Size** - 根据数据特性动态调整

---

## 六、待深入研究的问题

1. **Block 大小选择**: 64KB vs 256KB vs 1MB 的权衡
2. **压缩算法选择**: LZ4 vs zstd 的压缩率 vs 速度权衡
3. **混合编码**: 根据数据特性自适应选择编码方式
4. **内存管理**: 更大 block 对内存的影响

---

## 七、相关文档

- [SID Delta 编码设计](./timestamp-sid-encoding/design.md)
- [压缩编码设计](./compression-encoding/design.md)
- [SSTable 文件格式](./single-file-sstable/design.md)
- [WAL 优化设计](./wal-optimization/design.md)
