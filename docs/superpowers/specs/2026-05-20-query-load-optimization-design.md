# 查询全量数据加载性能优化设计文档

> **状态**: 设计完成，待审核
> **日期**: 2026-05-20
> **目标**: 将 1M 全量扫描从 ~630ms 显著降低，同时不改变 Operator 接口

---

## 一、背景与目标

### 1.1 当前性能基线

1M 数据点全量扫描（Iterator 路径）耗时约 630ms，分析发现主要瓶颈在于：

1. **全字段解码**：SSTable 已是列式存储，但查询时仍解码所有 3 个字段列
2. **重复 Tags 分配**：每行调用 `GetTags(sid)` 分配新 `map[string]string`，1M 行 = 1M 次分配
3. **无块级跳过**：Filter 逐行判断，即使整个块都不满足条件仍需完整解码
4. **串行 Shard 扫描**：多 Shard 场景下逐个 Shard 串行读取

### 1.2 优化目标

| 优化项 | 目标 |
|--------|------|
| 列裁剪 | 3 字段全量扫描延迟降低 40%+ |
| Tags 缓存 | 全量扫描内存分配降低 30%+ |
| Zone Map | 选择性查询（`WHERE cpu > 90`）延迟降低 80%+ |
| 并行扫描 | 多 Shard 扫描近线性加速 |

---

## 二、整体架构

四项优化分布在查询路径的不同层次，改动独立、可各自验证：

```
Query Plan (ProjectSpec / FilterSpec)
    │
    ▼
┌─────────────────────────────────────────────────┐
│ engine_query.go                                 │
│  [4] 并行 Shard 扫描 — goroutine 扇出           │
│  各 Shard 独立扫描 → channel → heap merge        │
└─────────────────────────────────────────────────┘
    │ per Shard
    ▼
┌─────────────────────────────────────────────────┐
│ shard/iterator.go                               │
│  [2] Tags 缓存 — SID→Tags 引用计数              │
│  消除 GetTags() 每行 map 分配                   │
└─────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│ sstable/iterator.go                             │
│  [1] 列裁剪 — fields 参数仅解码选中列           │
│  [3] Zone Map — 块级 min/max 跳过不匹配块       │
└─────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│ sstable/format.go (文件格式)                     │
│  BlockIndex entry 新增 ZoneMap 字段              │
└─────────────────────────────────────────────────┘
```

### 接口约束

- Operator 接口（`Open/Next/Close`）不变
- `DB.Iterator()` / `DB.Execute()` 公共 API 签名不变
- `query.Iterator` 堆归并逻辑内部可改，外部行为不变

---

## 三、优化 1：列裁剪（Column Projection Pushdown）

### 3.1 背景

SSTable 已是列式存储：timestamps、sids、各字段独立存为 section，block 内也是独立压缩的列。但当前 `Iterator.loadBlock()` 加载所有列的数据到 `blockFieldData`，`Point()` 解码所有列。

### 3.2 设计

在 Iterator 初始化时传入需要投影的字段列表 `fields []string`：
- `fields == nil` 或空：解码所有列（兼容现有行为）
- `fields` 非空：仅解码指定列，其他列不读磁盘、不解码

```
SSTable 文件布局:
┌──────────┬──────────┬──────────┬──────────┬──────────┐
│timestamps│  sids    │  cpu     │  mem     │ counter  │
│ (col)    │  (col)   │  (col)   │  (col)   │  (col)   │
└──────────┴──────────┴──────────┴──────────┴──────────┘

SELECT cpu FROM ...
  → loadBlock 只读 timestamps + sids + cpu 列
  → mem, counter 列不触碰磁盘、不解码
```

### 3.3 改动点

| 文件 | 变更 |
|------|------|
| `sstable/iterator.go` | 新增 `fields []string` 字段 |
| `sstable/iterator_block.go` | `loadBlock()` 仅读取指定列的 block 数据 |
| `sstable/iterator_next.go` | `Point()` 仅解码指定字段 |
| `sstable/merge_iterator.go` | 透传 `fields` 到子 Iterator |
| `shard/iterator.go` | 从 `QueryRangeRequest.Fields` 提取，传入 SSTable Iterator |
| `engine/engine_query.go` | `Execute()` 路径从 `ProjectSpec` 提取字段列表 |
| 相关测试文件 | 新增列裁剪正确性测试 |

### 3.4 验证

- 单元测试：`PointRow.Fields` 仅包含请求的字段，不包含未请求字段
- Benchmark：3 字段全量扫描 vs 1 字段全量扫描延迟对比

---

## 四、优化 2：Tags 缓存

### 4.1 背景

`ShardIterator.pointToRow()` 每行调用 `seriesStore.GetTags(sid)` 分配新 `map[string]string`。benchmark 中约 9 种 tag 组合，但分配了 1M 次 map。SID 分配后 tags 不可变，天然适合缓存。

### 4.2 设计

在 `SeriesStore` 内部加 `sync.Map` 缓存层。首次遇到新 SID 时查询 metadata 并写入缓存，后续直接返回共享引用。

```go
type seriesStore struct {
    // ... 现有字段
    tagCache sync.Map  // map[uint64]map[string]string
}

func (s *seriesStore) GetTags(db, meas string, sid uint64) (map[string]string, bool) {
    if v, ok := s.tagCache.Load(sid); ok {
        return v.(map[string]string), true
    }
    tags, found := s.lookupTags(db, meas, sid)
    if found {
        s.tagCache.Store(sid, tags)
    }
    return tags, found
}
```

**选用 `sync.Map` 的理由**：
- 读多写少（写仅发生在新 SID 首次出现时）
- 无锁读取，适合高频查询路径
- 后续并行扫描时各 goroutine 安全共享

**约束**：返回的 `map[string]string` 是共享引用，调用方只读，不得修改。已验证现有代码中 `PointRow.Tags` 仅被读取。

### 4.3 改动点

| 文件 | 变更 |
|------|------|
| `internal/storage/metadata/series.go` | `SeriesStore` 内部加 `sync.Map` 缓存 |
| 相关测试文件 | 验证缓存命中率 |

### 4.4 验证

- `testing.AllocsPerRun` 对比缓存前后分配次数
- 功能测试：缓存命中不影响 Tags 正确性

---

## 五、优化 3：Zone Map（块级统计 + 谓词下推）

### 5.1 背景

Filter 算子对每行执行条件判断。即使一个块中所有 cpu 值都 ≤50，`WHERE cpu > 50` 仍会完整解码该块。通过在块索引中记录 min/max，可在解码前跳过不满足条件的块。

### 5.2 文件格式变更

```
BlockIndex（现有）:
┌─────────────────────────────────────────────────┐
│ block_count (4B)                                │
│ entries[]: {ts_min, ts_max, row_count, offset}  │
└─────────────────────────────────────────────────┘

BlockIndex（新）:
┌─────────────────────────────────────────────────┐
│ block_count (4B)                                │
│ zone_map_count (2B)    ← 新增：启用的字段数量     │
│ zone_map_fields[]      ← 新增：字段名列表         │
│ entries[]: {ts_min, ts_max, row_count, offset,   │
│             zone_map[]} ← 每字段 {min(8B), max(8B)} │
└─────────────────────────────────────────────────┘
```

**Zone Map 字段语义**：
- 数值字段（float64/int64）：精确 min/max
- 字符串字段：字典序 min/max（用于 EQ 判断）
- bool 字段：不记录

**SSTable Flags 新增位**：
- `FlagHasZoneMap uint16 = 0x0002` — 表示文件包含 Zone Map
- Zone Map 写入优先级为最高（不因 compaction 而丢失）

### 5.3 查询流程

```
Filter: WHERE cpu > 50 AND mem < 200

对每个 block:
  block.zoneMap["cpu"].max ≤ 50  → 跳过该块
  block.zoneMap["mem"].min ≥ 200 → 跳过该块
  否则 → 解码该块，逐行过滤
```

Zone Map 检查在 `Iterator.loadBlock()` 层面执行——在读取和解码列数据之前决定是否跳过，避免磁盘 I/O 和解码开销。

### 5.4 改动点

| 文件 | 变更 |
|------|------|
| `sstable/format.go` | `BlockIndexEntry` 新增 ZoneMap 字段；新增 FlagHasZoneMap |
| `sstable/writer.go` | `WriteBlock()` 时计算每列 min/max |
| `sstable/iterator_block.go` | `loadBlock()` 新增 ZoneMap 检查参数，可跳过不匹配块 |
| `sstable/reader.go` | `ReadBlockIndex()` 解析 ZoneMap |
| `engine/engine_query.go` | 传递 FilterSpec 条件给 Iterator |

### 5.5 验证

- 单元测试：验证 ZoneMap 跳过正确性（已知数据 + 已知过滤条件 → 预期结果行数）
- 单元测试：ZoneMap 序列化/反序列化往返
- Benchmark：`WHERE cpu > 90`（命中率 ~10%）延迟对比

---

## 六、优化 4：并行 Shard 扫描

### 6.1 背景

`engine_query.go` 中 `createDataIterator()` 串行收集所有 Shard 的 Iterator。多 Shard 场景下，各 Shard 的 SSTable 物理上独立，可完全并行读取。

### 6.2 设计

每个 Shard 启动独立 goroutine 扫描，结果通过 buffered channel 导出，统一由 heap merge 消费。

```
串行（当前）:
  Shard 1 → iter ──┐
  Shard 2 → iter ──┼──→ heap merge → RowIterator
  Shard 3 → iter ──┘

并行（新）:
  Shard 1 → goroutine → chan ──┐
  Shard 2 → goroutine → chan ──┼──→ heap merge → RowIterator
  Shard 3 → goroutine → chan ──┘
```

**并行度控制**：
- Shard 数 ≤ 2：直接串行（goroutine 开销 > 收益）
- Shard 数 > 2：按 `min(shardCount, runtime.GOMAXPROCS(0))` 控制 goroutine 数
- 所有 goroutine 共享 `context.Context`，任意 goroutine 出错时取消所有
- `LIMIT` 到达后，通过 `context.CancelFunc` 取消其余 goroutine

**适配器模式**：不改变 `query.Iterator` 的 heap merge 逻辑，外围 `scanShards()` 收集多 channel 结果，构造 `sliceIterator` 数组交给现有 heap merge。

```go
func scanShards(ctx context.Context, shards []*Shard, req *QueryRangeRequest) []*sliceIterator {
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    var wg sync.WaitGroup
    chans := make([]chan *types.PointRow, len(shards))
    for i, shard := range shards {
        chans[i] = make(chan *types.PointRow, 256)
        wg.Add(1)
        go func(idx int, s *Shard) {
            defer wg.Done()
            defer close(chans[idx])
            scanShard(ctx, s, req, chans[idx])
        }(i, shard)
    }

    // 等所有 goroutine 结束后构建 sliceIterator
    // ...
}
```

### 6.3 改动点

| 文件 | 变更 |
|------|------|
| `internal/engine/engine_query.go` | `createDataIterator()` 改为并行模式 |
| 相关测试文件 | 多 Shard 并行扫描正确性验证 |

### 6.4 验证

- 单元测试：多 Shard 并行扫描结果行数与串行一致
- 单元测试：`LIMIT` 提前终止验证（goroutine 正确取消）
- Benchmark：多 Shard 场景延迟 vs 串行

---

## 七、实施顺序与依赖

四项优化按依赖关系和验证独立性排序：

```
Phase 1: 列裁剪 + Tags 缓存（无依赖，可并行开发）
    │
Phase 2: Zone Map（依赖列裁剪，块跳过需要知道投影哪些字段）
    │
Phase 3: 并行 Shard 扫描（依赖前三项在 Shard 级工作正确）
```

| Phase | 优化项 | 预估改动量 | 依赖 |
|-------|--------|-----------|------|
| 1a | 列裁剪 | ~6 文件 | 无 |
| 1b | Tags 缓存 | ~1 文件 | 无 |
| 2 | Zone Map | ~5 文件 | 列裁剪（fields 参数传递链） |
| 3 | 并行 Shard 扫描 | ~2 文件 | 前三项均在 Shard 级稳定 |

---

## 八、风险与边界

### 8.1 风险

| 风险 | 缓解 |
|------|------|
| Zone Map 增大 BlockIndex 体积 | 每个数值字段每块 16B，1000 块 × 3 字段 = 48KB，可忽略 |
| 并行扫描 goroutine 泄漏 | `context.WithCancel` 确保所有路径 release |
| Tags 缓存内存无限增长 | SID 数量有上限（series count），内存可控 |
| 空 fields 参数行为不一致 | `fields == nil` → 解码全部，保持向后兼容 |

### 8.2 不做的事

- 不改变 Operator 接口（`Open/Next/Close`）
- 不引入外部缓存依赖（如 Redis）
- 不实现行级 Zone Map（Bloom Filter 等）——本次仅块级
- 不改变 Tags 的返回类型（仍为 `map[string]string`）
