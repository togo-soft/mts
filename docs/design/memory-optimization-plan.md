# 大规模写入内存优化实施计划（修订版）

## 背景

10M 数据点写入场景（ShardDuration=1h, MaxCount=50000），TPS ≈ 494K。

pprof heap profile 确认的分配来源：

| 占比 | 内存 | 位置 | 可优化 |
|------|------|------|--------|
| 62.71% | 1034MB | `types/internal.go:164` | 本质是 10M × ~103B 数据载荷 |
| 12.93% | 213MB | `memtable/memtable.go:149` | **是** — Swap 新 active slice |
| 11.82% | 195MB | `memtable/memtable.go:58` | 部分 — append 扩容 |
| 10.41% | 172MB | `wal/wal.go:87` | **是** — 每 shard WAL 64KB |

## Task 1: FieldData Arena — 已验证不适用

### 实施与结果

实现了完整的 FieldArena 方案（Arena 分配器 + MemTable 绑定 + Shard 适配），并运行 10M pprof 对比测试：

| 指标 | 优化前 | Arena 方案 | 变化 |
|------|--------|------------|------|
| HeapAlloc | 1699 MB | **3694 MB** | +117% |
| TotalAlloc | 10 GB | **13.7 GB** | +37% |
| HeapInuse | 1817 MB | **3794 MB** | +109% |
| FieldArena.Alloc | 0 | **2222 MB** | — |
| NewFieldArena | 0 | **834 MB** | — |

### 根因分析

测试创建 ~2,800 个 Shard（ShardDuration=1h，数据跨 115 天），每个 Shard 仅写入 ~3,600 点就因 idle timeout 触发 flush。

每个 Arena 独立管理：128KB 初始 → 256KB → 512KB（2x 扩容），最终容量 512KB 但实际只用 360KB，**浪费 30%**。2,800 个 Arena 叠加后浪费放大到 ~3GB。

Arena 适用条件：少量大 Shard（如 ShardDuration=24h → ~115 个 Shard），此时 Arena 数量少、利用率高。本测试的 "多小 Shard" 场景下 Arena 反效果。

### 核心发现

**1034MB FieldData 是 10M 点的实际数据载荷**（103 字节/点 × 10M = 1GB），这是 MemTable 持有的序列化字段数据，属于 LSM-tree 架构的硬性内存需求。减少此开销需要降低 Shard 数量（增大 ShardDuration）或更激进 flush（降低 MaxCount），而非代码层面的分配优化。

---

## 剩余可实施优化

### Task 2: MemTable Slice 池化 (P1)

**目标**: 消除 Swap 中 213MB 的切片分配

**方案**: sync.Pool 复用被动切片的底层数组

**变更**: `internal/storage/memtable/memtable.go` — Swap 从池取新切片，ClearPassive 归还旧切片

**预期**: 213MB → ~20MB

### Task 3: WAL 写缓冲共享池 (P1)

**目标**: 消除 WAL Open 中 172MB 的缓冲分配

**方案**: 全局 sync.Pool 替代实例预分配，Write 惰性获取，Close 归还

**变更**: `internal/storage/wal/wal.go`

**预期**: 172MB → ~10MB（仅正在写入的 Shard 持有缓冲）

### Task 4: SSTable Close 路径缓冲复用 (P2)

**目标**: 减少每 flush 的逐 block 分配（合计 ~60 万次额外分配）

**方案**: 预分配一次最大 block 缓冲 + 循环复用；CompressBlock 池化

**变更**: `internal/storage/shard/sstable/writer_close.go`, `compress.go`

**预期**: 每 SSTable Close 分配次数降低 > 60%

### Task 5: DedupFilter 池化 (P3)

**目标**: 复用每次 compaction 的 1.3MB DedupFilter

**方案**: sync.Pool + Reset 方法

**变更**: `internal/storage/compaction/dedup.go`

**预期**: 长期运行减少 GC 压力

---

## 预期综合效果（Task 2+3）

| 指标 | 优化前 | 优化后（预计） |
|------|--------|----------------|
| 峰值堆 (HeapAlloc) | ~1700 MB | ~1300 MB |
| WAL 缓冲 | 172 MB | ~10 MB |
| MemTable 切片 | 213 MB | ~20 MB |
| FieldData（不可减） | 1034 MB | 1034 MB |

FieldData 的 1034MB 为数据载荷本身，无法通过分配优化减少。如需进一步降低，需从架构层面调整 Shard 粒度或 MemTable 压缩。

---

## 执行顺序

```
Task 2 (Slice Pool) ── 独立
Task 3 (WAL Pool)   ── 独立，可与 Task 2 并行
Task 4 (SSTable)    ── 独立
Task 5 (Dedup)      ── 独立
```
