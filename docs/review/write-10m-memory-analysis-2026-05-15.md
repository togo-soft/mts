# 10M 写入路径内存分配热点分析

**日期**: 2026-05-15
**测试用例**: write_10m_pprof（10M 数据点，每点 10 字段，单 tag `{"host":"server1"}`）
**MemTable 配置**: MaxSize=256MB, MaxCount=50000

## 一、总体指标

| 指标 | 优化前 (10M) | 优化后 (10M) | 改善 |
|------|------------|------------|------|
| TotalAlloc | 16.36 GB | **10.09 GB** | **-38.3%** |
| TPS | 482K | **494K** | +2.5% |
| NumGC | 31 | **23** | -25.8% |
| HeapInuse | 1,458 MB | 1,816 MB | +24.6% |
| 每点分配 | ~1,636 B | **~1,011 B** | -38.2% |
| 磁盘占用 | 1.05 GB | 1.07 GB | ±0 |


## 二、Alloc Space 热点排名

### P0: writeMemPoint — 3,452 MB (21.1%)

| 子项 | 估算分配 | 说明 |
|------|---------|------|
| `string(data[pos:pos+kLen])` 字段名 | ~1,600 MB | 10字段×10M点，每个字段名字符串分配 |
| `string(data[pos:pos+vLen])` 字符串值 | ~300 MB | string_1 字段每次分配 |
| `make(map[string]bool)` | ~500 MB | 每点分配一个 written map |
| column buffer append 扩容 | ~1,000 MB | fieldBufs append 触发 realloc |

**根因**: `writeMemPoint` 对 FieldData 逐点解析时，每个字段名都通过 `string(data[...])` 创建新 Go 字符串。即使字段名在 10M 点中都是相同的 10 个，Go 编译器无法跨点复用。同时每个点新建 `map[string]bool` 追踪已写字段。

**优化方案**: 字段索引 + pool 化 written 追踪
- `scanFieldDataKeys` 阶段构建 `fieldIndex map[string]int`（字段名→整数索引），一次性建完
- `writeMemPoint` 使用字节级比较（循环匹配已知字段名 bytes）获取索引，避免字符串分配
- `written` 改为 pooled `[]bool`（按索引复位），避免 map 分配
- `appendFieldValue` 接受 int 索引而非 string name

预计节省: ~2,000 MB

### P1: serializePointForWAL — 2,373 MB (14.5%)

**根因**: `shard_io.go:68` 每次写入调用 `serializePointForWAL(ts, sid, fieldData)` 分配完整 WAL 记录 buffer。格式为 `Version(1B) + TS(8B) + SID(8B) + FieldData`。

WAL 批量写入路径 (`shard_io.go:152`) 虽然数据被批量 append，但每个 `serializePointForWAL` 仍独立分配。

**优化方案**: sync.Pool 池化 WAL buffer
- WAL buffer 大小 = 17 + len(FieldData)，FieldData 长度相对稳定（~200 bytes）
- 使用分级 pool（小/中/大）按 FieldData 长度取对应池
- WAL Write 完成后 buffer 归还

预计节省: ~2,000 MB

### P2: serializeFieldsFromMap — 2,279 MB (13.9%)

**根因**: `types/internal.go` 中 `serializeFieldsFromMap` 已有 sync.Pool 复用中间 buffer，但最终 `result := make([]byte, len(buf)); copy(result, buf)` 仍为每个 MemPoint 分配独立的 FieldData 副本。

**inuse 占比 69.3% (1,011 MB)**: 这些 FieldData 被活跃 MemTable 持有，属于"必要"驻留。

**优化方案**: 暂不优化（必要分配）
- MemTable 必须持有 FieldData 才能在 swap 后传递给 flush
- 当前 pool 已消除中间 buffer 分配
- 进一步的零拷贝方案（如 byte slice 引用计数 + copy-on-write）复杂度高

预计节省: 0（必要数据拷贝）

### P3: MemTable.Write — 1,145 MB (7.0%)

**根因**: `memtable.go:58` `m.active = append(m.active, mp)` — MemPoint 结构体 append 到 active slice。当 slice 扩容时，底层数组重新分配并拷贝所有已存在的 MemPoint（浅拷贝，但 slice header 需要重新分配）。

50,000 容量的 slice 多次扩容会产生可观的分配。

**优化方案**: 预分配 + 批量 append
- 已知 MemTable MaxCount=50000，Swap 时直接 `make([]types.MemPoint, 0, 50000)` 预分配
- 当前 Swap 使用 `make(..., 0, 1024)` (memtable.go:143)，应改为目标容量

预计节省: ~400 MB（减少扩容次数）

### P4: scanFieldDataKeys — 867 MB (5.3%)

**根因**: 与 writeMemPoint 相同的字符串分配问题。`scanFieldDataKeys:78` 调用 `key := string(data[pos:pos+kLen])` 逐字段创建字符串。

但 scanFieldDataKeys 仅在 Writer 创建时的第一遍扫描调用（一次性），每个点都在第一遍解析。实际上因为每批 flush 调用一次 WriteMemPoints → scanFieldDataKeys，所以每批都重新扫描。

**优化方案**: 与 writeMemPoint 相同的字段索引方案
- 第一遍扫描也使用字节比较，或在第一遍建立 intern 池
- 更好的方案：schema 已从 schemaStore 持久化，Writer 可复用上次的 schema 跳过 scan

预计节省: ~800 MB（与 writeMemPoint 优化共享基础设施）

### P5: hashCacheKey — 221 MB (1.4%)

**根因**: 每次 `loadHashSid` 调用都构建字符串键 `"db1/cpu/{hex_hash}"`。10M 写入中 hash 缓存命中率接近 100%，但每次仍分配键。

**优化方案**: 复合键避免字符串分配
- 使用 `[24]byte` 固定大小键（8B db hash + 8B meas hash + 8B tag hash）或用 struct key
- 或改用 `maphash` 两级 hash 直接索引

预计节省: ~200 MB

---

## 三、Inuse Space 分析

| 函数 | Inuse (MB) | % | 说明 |
|------|-----------|-----|------|
| serializeFieldsFromMap | 1,011 | 69.3% | MemTable 持有的 FieldData |
| MemTable.Write | 195 | 13.4% | Active MemTable slice 底层数组 |
| wal.Open | 163 | 11.2% | WAL 写入缓冲区 |
| MemTable.Swap | 62 | 4.2% | Swap 时 passive slice 分配 |

Inuse 1,458 MB 中~1,200 MB 是 MemTable 持有的活跃数据（合理），~163 MB 是 WAL buffer。

---

## 四、CPU 热点

| 函数 | flat% | cum% | 说明 |
|------|-------|------|------|
| Syscall6 (I/O) | 10.9% | 10.9% | 磁盘 I/O |
| BitWriter.WriteBit | 10.8% | 10.8% | 压缩位写入 |
| XorFloatEncode | 0.5% | 13.1% | 浮点 XOR 编码 |
| Writer.Close | 0 | 24.8% | SSTable flush+encode |
| WAL.Write | 0 | 15.6% | WAL 写入+压缩 |

CPU 瓶颈在 I/O + 压缩编码，属于正常计算密集。

---

## 五、优化优先级

| 优先级 | 方案 | 预计节省 | 风险 | 复杂度 | 说明 |
|--------|------|---------|------|--------|------|
| P0 | writeMemPoint 字段索引化 | ~2,000 MB | 低 | 中 | 消除 100M 字符串分配 |
| P1 | WAL buffer 池化 | ~2,000 MB | 低 | 低 | sync.Pool 复用 |
| P2 | scanFieldDataKeys 复用 | ~800 MB | 低 | 中 | 与 P0 共享基础设施 |
| P3 | MemTable 预分配 | ~400 MB | 低 | 低 | 调整初始容量 |
| P4 | hashCacheKey 优化 | ~200 MB | 低 | 低 | 改用固定大小键 |

**预计总节省**: ~5,400 MB (33%)，每点分配从 1,636 B 降至 ~1,100 B

**建议执行顺序**: P3 → P1 → P0+P2 → P4
