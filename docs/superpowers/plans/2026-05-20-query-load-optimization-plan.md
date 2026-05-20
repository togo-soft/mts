# 查询全量数据加载性能优化 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 1M 全量扫描从 ~630ms 显著降低，实施列裁剪、Tags 缓存、Zone Map 和并行 Shard 扫描四项优化。

**Architecture:** 四项优化分布在查询路径的不同层次，改动独立。Phase 1（列裁剪 + Tags 缓存）无依赖可并行；Phase 2（Zone Map）依赖 Phase 1 的 fields 传递链；Phase 3（并行扫描）依赖前三项在 Shard 级稳定。

**Tech Stack:** Go 1.24, sync.Map, container/heap, SSTable 列式存储

---

## 前置发现

通过代码分析，发现以下关键现状：

1. **SSTable Iterator 层已完整支持 projectedFields**：`Iterator.projectedFields`、`NewIterator(fields)`、`loadBlock()` 字段过滤、`NewMergeIterator(..., fields)` 均已实现。唯一缺口是 `ShardIterator` 在第 113 行调用 `NewMergeIterator` 时传 `nil`，以及 Engine 层未提取和传递 fields。

2. **Tags 缓存已部分实现**：`seriesStore.GetTags()` 已有 `sync.Map` 缓存层和 `rebuildCache()`。但每次命中缓存仍调用 `copyTags()` 分配新 map。优化方向是：缓存命中时直接返回共享引用（调用方只读）。

3. **ZoneMap 最适合作为独立 section**：不修改 BlockIndexEntry 格式以避免复杂化，而是新增 `_zone_map` section 存储每 block 每字段的 min/max。

---

## 文件结构

```
修改文件:
  internal/storage/shard/sstable/format.go        — 新增 FlagHasZoneMap
  internal/storage/shard/sstable/index.go         — 新增 ZoneMap 类型与序列化
  internal/storage/shard/sstable/writer.go        — Writer 新增 zoneMap 累积字段
  internal/storage/shard/sstable/writer_close.go  — flushBlock 计算 zone map, Close 写入 _zone_map section
  internal/storage/shard/sstable/writer_field.go  — writeMemPoint/writeInternalPoint 累积 zone map 值
  internal/storage/shard/sstable/iterator.go      — Iterator 新增 filterConds 字段
  internal/storage/shard/sstable/iterator_block.go— loadBlock 新增 ZoneMap 跳过逻辑
  internal/storage/shard/sstable/reader.go        — 新增 ReadZoneMap 方法
  internal/storage/shard/sstable/merge_iterator.go— 透传 filterConds（如需）
  internal/storage/shard/iterator.go              — ShardIterator 新增 fields 参数, 透传到 MergeIterator
  internal/engine/engine_query.go                 — Execute/createDataIterator 提取 fields 和 filter 条件并下传
  internal/query/iterator.go                      — NewIteratorWithMemTable 新增 fields 参数, 并行 Shard 扫描
  internal/query/executor.go                      — BuildPipeline 提取 ProjectSpec fields 供 Engine 使用
  internal/storage/metadata/series_impl.go        — GetTags 缓存命中时返回共享引用
  internal/storage/metadata/series_simple.go      — SimpleSeriesStore.GetTags 同样优化

新增文件:
  internal/storage/shard/sstable/zone_map.go      — ZoneMap 类型定义与序列化/反序列化

不改的文件（已满足需求）:
  internal/storage/shard/sstable/iterator_next.go — Point() 已仅解码 blockFieldData 中的字段
  mts.go                                         — 公共 API 签名不变
  types/mts.pb.go                                — 已有 FilterCondition, ProjectSpec
```

---

### Task 1: 列裁剪 — ShardIterator 透传 fields 参数

**目标:** 打通 fields 参数从 ShardIterator → sstable.NewMergeIterator 的传递链

**Files:**
- Modify: `internal/storage/shard/iterator.go:65-126`

- [ ] **Step 1: 修改 NewShardIteratorWithMemTable 签名，新增 fields 参数**

```go
// 修改前 (line 72):
func NewShardIteratorWithMemTable(shard *Shard, externalMT *memtable.MemTable, extSeriesStore SeriesStore, startTime, endTime int64, maxRows int) *ShardIterator {

// 修改后:
func NewShardIteratorWithMemTable(shard *Shard, externalMT *memtable.MemTable, extSeriesStore SeriesStore, startTime, endTime int64, maxRows int, fields []string) *ShardIterator {
```

- [ ] **Step 2: 修改 NewShardIterator，透传 nil fields**

```go
// 修改前 (line 66):
func NewShardIterator(shard *Shard, startTime, endTime int64, maxRows int) *ShardIterator {
    return NewShardIteratorWithMemTable(shard, nil, nil, startTime, endTime, maxRows)
}

// 修改后:
func NewShardIterator(shard *Shard, startTime, endTime int64, maxRows int) *ShardIterator {
    return NewShardIteratorWithMemTable(shard, nil, nil, startTime, endTime, maxRows, nil)
}
```

- [ ] **Step 3: 修改 SSTable MergeIterator 创建处，传入 fields 而非 nil**

```go
// 修改前 (line 113):
sstIter, err := sstable.NewMergeIterator(sstFiles, startTime, endTime, schema, si.shard, nil)

// 修改后:
sstIter, err := sstable.NewMergeIterator(sstFiles, startTime, endTime, schema, si.shard, fields)
```

- [ ] **Step 4: 运行现有测试验证无回归**

```bash
cd /root/projects/mts && go build ./...
cd /root/projects/mts && go test ./internal/storage/shard/... -v -count=1 2>&1 | tail -20
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/shard/iterator.go
git commit -m "feat: ShardIterator 透传 fields 参数到 SSTable MergeIterator"
```

---

### Task 2: 列裁剪 — Engine + query.Iterator 传递 fields

**目标:** 从 Engine.Execute 的 ProjectSpec 和 Iterator 的 req.Fields 提取字段列表，传递到 ShardIterator

**Files:**
- Modify: `internal/query/iterator.go:179-230` — NewIteratorWithMemTable 新增 fields 参数
- Modify: `internal/query/iterator.go:146-173` — NewIterator 新增 fields 参数
- Modify: `internal/engine/engine_query.go:265-331` — createDataIterator 提取并传递 fields
- Modify: `internal/engine/engine_query.go:233-262` — Execute 提取 ProjectSpec fields
- Modify: `internal/engine/engine_query.go:41-118` — Iterator 传递 req.Fields

- [ ] **Step 1: 修改 NewIteratorWithMemTable，新增 fields 参数并传递给 ShardIterator**

```go
// 修改前 (line 179):
func NewIteratorWithMemTable(ctx context.Context, shards []*shard.Shard, writerMT *memtable.MemTable, extSeriesStore shard.SeriesStore, req *types.QueryRangeRequest, unorderedData ...[]*types.PointRow) *Iterator {

// 修改后:
func NewIteratorWithMemTable(ctx context.Context, shards []*shard.Shard, writerMT *memtable.MemTable, extSeriesStore shard.SeriesStore, req *types.QueryRangeRequest, fields []string, unorderedData ...[]*types.PointRow) *Iterator {
```

在 NewIteratorWithMemTable 内部，将 `fields` 传递给 `shard.NewShardIteratorWithMemTable` 和 `shard.NewShardIterator`：

```go
// 修改前 (line 196):
si = shard.NewShardIteratorWithMemTable(s, writerMT, extSeriesStore, startTimeNs, endTimeNs, maxRows)

// 修改后:
si = shard.NewShardIteratorWithMemTable(s, writerMT, extSeriesStore, startTimeNs, endTimeNs, maxRows, fields)

// 修改前 (line 198):
si = shard.NewShardIterator(s, startTimeNs, endTimeNs, maxRows)

// 修改后:
si = shard.NewShardIteratorWithMemTable(s, nil, nil, startTimeNs, endTimeNs, maxRows, fields)
```

同样的修改应用于 line 209（nil shard + writerMT 场景）。

- [ ] **Step 2: 修改 NewIterator，透传 fields**

```go
// 修改前 (line 146):
func NewIterator(ctx context.Context, shards []*shard.Shard, req *types.QueryRangeRequest) *Iterator {

// 修改后:
func NewIterator(ctx context.Context, shards []*shard.Shard, req *types.QueryRangeRequest) *Iterator {
    return NewIteratorWithMemTable(ctx, shards, nil, nil, req, req.Fields)
}
```

- [ ] **Step 3: 修改 createDataIterator，接受 fields 参数并传递**

```go
// 修改前 (line 265):
func (e *Engine) createDataIterator(database, measurement string, startTime, endTime int64, _ int64) (*query.Iterator, error) {

// 修改后:
func (e *Engine) createDataIterator(database, measurement string, startTime, endTime int64, fields []string) (*query.Iterator, error) {
```

在最后一行返回处传递 fields：

```go
// 修改前 (line 330):
return query.NewIteratorWithMemTable(context.Background(), shards, writerMT, scoped, req, unorderedData...), nil

// 修改后:
return query.NewIteratorWithMemTable(context.Background(), shards, writerMT, scoped, req, fields, unorderedData...), nil
```

- [ ] **Step 4: 修改 Execute，从 ProjectSpec 提取 fields**

```go
// 修改前 (line 243):
dataIter, err := e.createDataIterator(plan.Database, plan.Measurement, plan.StartTime, plan.EndTime, 0)

// 修改后: 从 QueryPlan ops 中提取 ProjectSpec fields
var projFields []string
for _, op := range plan.Ops {
    if p := op.GetProject(); p != nil {
        projFields = p.Fields
        break
    }
}
dataIter, err := e.createDataIterator(plan.Database, plan.Measurement, plan.StartTime, plan.EndTime, projFields)
```

- [ ] **Step 5: 修改 Iterator 方法，传递 req.Fields**

```go
// 修改前 (line 117):
return query.NewIteratorWithMemTable(ctx, shards, writerMT, scoped, req, unorderedData...), nil

// 修改后:
return query.NewIteratorWithMemTable(ctx, shards, writerMT, scoped, req, req.Fields, unorderedData...), nil
```

- [ ] **Step 6: 修改 downsampleIterator，传递 nil fields（降采样不需投影）**

```go
// 修改前 (line 134):
return query.NewIteratorWithMemTable(ctx, nil, nil, scoped, req, downsampledData...), nil

// 修改后:
return query.NewIteratorWithMemTable(ctx, nil, nil, scoped, req, nil, downsampledData...), nil
```

- [ ] **Step 7: 修改 IteratorWithMemTable 包内辅助函数**

```go
// 修改前 (line 334):
func IteratorWithMemTable(ctx context.Context, shards []*shard.Shard, wmt *memtable.MemTable, extSeriesStore shard.SeriesStore, req *types.QueryRangeRequest) *Iterator {
    return query.NewIteratorWithMemTable(ctx, shards, wmt, extSeriesStore, req)
}

// 修改后:
func IteratorWithMemTable(ctx context.Context, shards []*shard.Shard, wmt *memtable.MemTable, extSeriesStore shard.SeriesStore, req *types.QueryRangeRequest) *Iterator {
    return query.NewIteratorWithMemTable(ctx, shards, wmt, extSeriesStore, req, req.Fields)
}
```

- [ ] **Step 8: 更新所有调用方**

搜索所有对 `IteratorWithMemTable`、`NewIteratorWithMemTable`、`NewShardIterator`、`NewShardIteratorWithMemTable` 的调用并更新参数。

```bash
cd /root/projects/mts && grep -rn "IteratorWithMemTable\|NewIteratorWithMemTable\|NewShardIterator\|NewShardIteratorWithMemTable" --include="*.go" | grep -v "_test.go" | grep -v ".pb.go"
```

- [ ] **Step 9: 运行测试验证**

```bash
cd /root/projects/mts && go build ./...
cd /root/projects/mts && go test ./internal/query/... ./internal/engine/... -v -count=1 2>&1 | tail -30
```

Expected: PASS

- [ ] **Step 10: Commit**

```bash
git add internal/query/iterator.go internal/engine/engine_query.go
git commit -m "feat: Engine 层从 ProjectSpec/req.Fields 提取字段列表并下传到 ShardIterator"
```

---

### Task 3: Tags 缓存 — 返回共享引用消除 map 分配

**目标:** 修改 `GetTags` 在缓存命中时直接返回共享引用而非 copyTags，消除每行 1 次 map 分配

**Files:**
- Modify: `internal/storage/metadata/series_impl.go:256-259` — 缓存命中返回共享引用
- Modify: `internal/storage/metadata/series_simple.go:153-161` — SimpleSeriesStore 同样优化
- Read: 验证调用方不修改返回的 map

- [ ] **Step 1: 验证调用方只读使用 Tags**

搜索所有对 `GetTags` 返回值的赋值和使用：

```bash
cd /root/projects/mts && grep -rn "GetTags\|\.Tags\s*=" --include="*.go" | grep -v "_test.go" | grep -v ".pb.go"
```

确认 `ShardIterator.pointToRow()` 和 `resolveTags()` 仅赋值 `row.Tags = tags`，无后续修改。

- [ ] **Step 2: 修改 series_impl.go GetTags，缓存命中返回共享引用**

```go
// 修改前 (line 256-289):
func (s *seriesStore) GetTags(database, measurement string, sid uint64) (map[string]string, bool) {
    key := s.cacheKey(database, measurement, sid)
    if cached, ok := s.cache.Load(key); ok {
        return copyTags(cached.(map[string]string)), true
    }
    // ... bbolt lookup ...
    if tags != nil {
        s.cache.Store(key, copyTags(tags))
        return tags, true
    }
    return nil, false
}

// 修改后:
func (s *seriesStore) GetTags(database, measurement string, sid uint64) (map[string]string, bool) {
    key := s.cacheKey(database, measurement, sid)
    if cached, ok := s.cache.Load(key); ok {
        // 返回共享引用：调用方 (ShardIterator) 只读使用 Tags，不得修改
        return cached.(map[string]string), true
    }
    var tags map[string]string
    _ = s.db.View(func(tx *bolt.Tx) error {
        // ... 不变 ...
    })
    if tags != nil {
        s.cache.Store(key, tags) // 直接存储 bbolt 反序列化的 map，不再 copy
        return tags, true
    }
    return nil, false
}
```

- [ ] **Step 3: 修改 SimpleSeriesStore.GetTags，同样返回共享引用**

```go
// 修改前 (line 153-161):
func (s *SimpleSeriesStore) GetTags(database, measurement string, sid uint64) (map[string]string, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    tags, ok := s.series[sid]
    if !ok {
        return nil, false
    }
    return copyTags(tags), true
}

// 修改后:
func (s *SimpleSeriesStore) GetTags(database, measurement string, sid uint64) (map[string]string, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    tags, ok := s.series[sid]
    if !ok {
        return nil, false
    }
    return tags, true // 返回共享引用
}
```

- [ ] **Step 4: 运行测试验证正确性**

```bash
cd /root/projects/mts && go test ./internal/storage/metadata/... -v -count=1 2>&1 | tail -20
```

Expected: PASS（所有 Tags 相关测试通过）

- [ ] **Step 5: 运行 benchmark 验证内存分配降低**

```bash
cd /root/projects/mts && go test ./internal/storage/metadata/... -bench=BenchmarkGetTags -benchmem -count=1 2>&1
```

Expected: 每次 GetTags 0 allocs（缓存命中时）

- [ ] **Step 6: Commit**

```bash
git add internal/storage/metadata/series_impl.go internal/storage/metadata/series_simple.go
git commit -m "perf: GetTags 缓存命中时返回共享引用消除 map 分配"
```

---

### Task 4: Zone Map — 类型定义与 FlagHasZoneMap

**目标:** 创建 ZoneMap 类型和文件格式支持

**Files:**
- Create: `internal/storage/shard/sstable/zone_map.go`
- Modify: `internal/storage/shard/sstable/format.go:28-33` — 新增 FlagHasZoneMap

- [ ] **Step 1: 创建 zone_map.go**

```go
package sstable

import (
	"encoding/binary"
	"fmt"
	"math"
)

// ZoneMapEntry 单个字段在一个 block 内的 min/max 统计。
type ZoneMapEntry struct {
	FieldName string
	Min       float64
	Max       float64
}

// BlockZoneMap 单个 block 所有字段的 ZoneMap。
type BlockZoneMap struct {
	FieldZMaps []ZoneMapEntry
}

// ZoneMapIndex 所有 block 的 ZoneMap 汇总。
type ZoneMapIndex struct {
	Blocks []BlockZoneMap
}

// Marshal 序列化 ZoneMapIndex。
// 格式: [block_count:4B][for each block: field_count:2B][for each field: name_len:2B, name:var, min:8B, max:8B]
func (zm *ZoneMapIndex) Marshal() []byte {
	size := 4 // block_count
	for _, b := range zm.Blocks {
		size += 2 // field_count
		for _, e := range b.FieldZMaps {
			size += 2 + len(e.FieldName) + 8 + 8 // name_len + name + min + max
		}
	}
	buf := make([]byte, size)
	pos := 0
	binary.BigEndian.PutUint32(buf[pos:], uint32(len(zm.Blocks)))
	pos += 4
	for _, b := range zm.Blocks {
		binary.BigEndian.PutUint16(buf[pos:], uint16(len(b.FieldZMaps)))
		pos += 2
		for _, e := range b.FieldZMaps {
			binary.BigEndian.PutUint16(buf[pos:], uint16(len(e.FieldName)))
			pos += 2
			copy(buf[pos:], e.FieldName)
			pos += len(e.FieldName)
			binary.BigEndian.PutUint64(buf[pos:], math.Float64bits(e.Min))
			pos += 8
			binary.BigEndian.PutUint64(buf[pos:], math.Float64bits(e.Max))
			pos += 8
		}
	}
	return buf
}

// UnmarshalZoneMapIndex 反序列化 ZoneMapIndex。
func UnmarshalZoneMapIndex(data []byte) (*ZoneMapIndex, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("zone map data too short: %d bytes", len(data))
	}
	blockCount := binary.BigEndian.Uint32(data[0:4])
	zm := &ZoneMapIndex{Blocks: make([]BlockZoneMap, 0, blockCount)}
	pos := 4
	for bi := uint32(0); bi < blockCount; bi++ {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("zone map truncated at block %d", bi)
		}
		fieldCount := binary.BigEndian.Uint16(data[pos : pos+2])
		pos += 2
		bzm := BlockZoneMap{FieldZMaps: make([]ZoneMapEntry, 0, fieldCount)}
		for fi := uint16(0); fi < fieldCount; fi++ {
			if pos+2 > len(data) {
				return nil, fmt.Errorf("zone map field %d truncated", fi)
			}
			nameLen := binary.BigEndian.Uint16(data[pos : pos+2])
			pos += 2
			if pos+int(nameLen)+16 > len(data) {
				return nil, fmt.Errorf("zone map field %d data truncated", fi)
			}
			name := string(data[pos : pos+int(nameLen)])
			pos += int(nameLen)
			min := math.Float64frombits(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			max := math.Float64frombits(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			bzm.FieldZMaps = append(bzm.FieldZMaps, ZoneMapEntry{FieldName: name, Min: min, Max: max})
		}
		zm.Blocks = append(zm.Blocks, bzm)
	}
	return zm, nil
}

// Lookup 按 block 索引和字段名查找 ZoneMap 条目。
func (zm *ZoneMapIndex) Lookup(blockIdx int, fieldName string) (ZoneMapEntry, bool) {
	if blockIdx < 0 || blockIdx >= len(zm.Blocks) {
		return ZoneMapEntry{}, false
	}
	for _, e := range zm.Blocks[blockIdx].FieldZMaps {
		if e.FieldName == fieldName {
			return e, true
		}
	}
	return ZoneMapEntry{}, false
}
```

- [ ] **Step 2: 在 format.go 新增 FlagHasZoneMap**

```go
// 修改 format.go，在 FlagUnordered 之后新增:
const (
    FlagSorted    uint16 = 0x0000
    FlagUnordered uint16 = 0x0001
    FlagHasZoneMap uint16 = 0x0002  // 新增：文件包含 Zone Map
)
```

- [ ] **Step 3: 编写 ZoneMap 序列化往返测试**

创建 `internal/storage/shard/sstable/zone_map_test.go`：

```go
package sstable

import (
	"math"
	"testing"
)

func TestZoneMapIndex_RoundTrip(t *testing.T) {
	original := &ZoneMapIndex{
		Blocks: []BlockZoneMap{
			{FieldZMaps: []ZoneMapEntry{
				{FieldName: "cpu", Min: 10.5, Max: 99.0},
				{FieldName: "mem", Min: 100.0, Max: 200.0},
			}},
			{FieldZMaps: []ZoneMapEntry{
				{FieldName: "cpu", Min: 0.0, Max: 50.0},
			}},
		},
	}

	data := original.Marshal()
	restored, err := UnmarshalZoneMapIndex(data)
	if err != nil {
		t.Fatal("unmarshal error:", err)
	}
	if len(restored.Blocks) != 2 {
		t.Fatalf("expected 2 blocks, got %d", len(restored.Blocks))
	}

	entry, ok := restored.Lookup(0, "cpu")
	if !ok {
		t.Fatal("expected cpu in block 0")
	}
	if entry.Min != 10.5 || entry.Max != 99.0 {
		t.Errorf("block 0 cpu: expected [10.5, 99.0], got [%v, %v]", entry.Min, entry.Max)
	}
}

func TestZoneMapIndex_Lookup_Missing(t *testing.T) {
	zm := &ZoneMapIndex{Blocks: []BlockZoneMap{{}}}
	_, ok := zm.Lookup(0, "nonexistent")
	if ok {
		t.Error("expected false for nonexistent field")
	}
	_, ok = zm.Lookup(1, "cpu")
	if ok {
		t.Error("expected false for out-of-range block")
	}
}
```

- [ ] **Step 4: 运行测试**

```bash
cd /root/projects/mts && go test ./internal/storage/shard/sstable/... -run TestZoneMap -v -count=1
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/shard/sstable/zone_map.go internal/storage/shard/sstable/zone_map_test.go internal/storage/shard/sstable/format.go
git commit -m "feat: 新增 ZoneMap 类型定义与 FlagHasZoneMap"
```

---

### Task 5: Zone Map — Writer 计算并写入 ZoneMap

**目标:** Writer 在写入 block 时计算每字段 min/max，Close 时写入 `_zone_map` section

**Files:**
- Modify: `internal/storage/shard/sstable/writer.go:30-62` — Writer 新增 zoneMap 累积字段
- Modify: `internal/storage/shard/sstable/writer_close.go:17-57` — flushBlock 计算 zone map
- Modify: `internal/storage/shard/sstable/writer_close.go:185-199` — Close 写入 _zone_map section

- [ ] **Step 1: Writer 新增 zoneMap 累积字段**

在 `writer.go` Writer struct 中新增：

```go
type Writer struct {
    // ... 现有字段 ...

    // Zone Map 累积缓冲区
    zoneMapCurr  map[string]*zoneAccumulator // 当前 block 各字段的 min/max 累积
    zoneMapIndex *ZoneMapIndex               // 已完成 block 的 zone map
}

type zoneAccumulator struct {
    min, max    float64
    initialized bool
}

func (za *zoneAccumulator) update(v float64) {
    if !za.initialized {
        za.min = v
        za.max = v
        za.initialized = true
        return
    }
    if v < za.min {
        za.min = v
    }
    if v > za.max {
        za.max = v
    }
}
```

在 `NewWriter` 中初始化：

```go
w := &Writer{
    // ... 现有初始化 ...
    zoneMapCurr:  make(map[string]*zoneAccumulator),
    zoneMapIndex: &ZoneMapIndex{Blocks: make([]BlockZoneMap, 0)},
}
```

- [ ] **Step 2: 在 flushBlock 中计算 ZoneMap**

在 `flushBlock()` 的 `w.blockIndex.Add(...)` 之前，将当前 zoneMap 累积值记录到 zoneMapIndex：

```go
func (w *Writer) flushBlock() error {
    // ... 现有写入逻辑 (保持不变) ...

    // 记录当前 block 的 ZoneMap
    bzm := BlockZoneMap{FieldZMaps: make([]ZoneMapEntry, 0, len(w.zoneMapCurr))}
    for name, acc := range w.zoneMapCurr {
        if acc.initialized {
            bzm.FieldZMaps = append(bzm.FieldZMaps, ZoneMapEntry{
                FieldName: name, Min: acc.min, Max: acc.max,
            })
        }
    }
    w.zoneMapIndex.Blocks = append(w.zoneMapIndex.Blocks, bzm)

    // 重置当前 block 的累积器
    w.zoneMapCurr = make(map[string]*zoneAccumulator)

    w.blockIndex.Add(w.firstTs, lastTs, uint32(w.totalRows), uint32(w.rowCount))
    // ... 其余不变 ...
}
```

- [ ] **Step 3: 在 writeMemPoint 中累积 ZoneMap 值**

在 `writer_field.go` 的 `writeMemPoint` 方法中（line 175），添加对数值字段的 min/max 累积：

在 `appendFieldValueIdx` 调用之后添加：

```go
// 累积 ZoneMap（仅数值字段）
if fv, ok := val.(*types.FieldValue); ok {
    switch fv.Value.(type) {
    case *types.FieldValue_FloatValue:
        v := fv.GetFloatValue()
        za, ok := w.zoneMapCurr[name]
        if !ok {
            za = &zoneAccumulator{}
            w.zoneMapCurr[name] = za
        }
        za.update(v)
    case *types.FieldValue_IntValue:
        v := float64(fv.GetIntValue())
        za, ok := w.zoneMapCurr[name]
        if !ok {
            za = &zoneAccumulator{}
            w.zoneMapCurr[name] = za
        }
        za.update(v)
    }
}
```

- [ ] **Step 4: 在 Close 中写入 ZoneMap section、设置 FlagHasZoneMap**

在 `writer_close.go` 的 `Close()` 方法中，在 "6. 构建 Section Table" 之前添加 ZoneMap 写入：

```go
// 4.5 写入 zone map section (在 block index 之后)
zoneMapOffset := currentOffset
zoneMapData := w.zoneMapIndex.Marshal()
if _, err := outFile.Write(zoneMapData); err != nil {
    return cleanupErr(err)
}
currentOffset += uint64(len(zoneMapData))
```

在 Section Table entries 末尾添加：

```go
// 在 section table entries 末尾添加 zone map entry（在 _block_map 之后）
sectionTable.Entries = append(sectionTable.Entries,
    SectionEntry{Type: SectionIndex, Name: "_zone_map", Offset: zoneMapOffset,
        Size: uint64(len(zoneMapData)), Encoding: EncodingRaw, Compression: CompressionNone},
)
```

修改 header.Flags 设置：

```go
header := FileHeader{
    // ... 现有字段 ...
    Flags: w.flags | FlagHasZoneMap, // 新增：标记文件含 ZoneMap
    // ... 其余不变 ...
}
```

- [ ] **Step 5: 运行现有测试验证写入不受影响**

```bash
cd /root/projects/mts && go test ./internal/storage/shard/sstable/... -v -count=1 2>&1 | tail -30
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/storage/shard/sstable/writer.go internal/storage/shard/sstable/writer_close.go internal/storage/shard/sstable/writer_field.go
git commit -m "feat: Writer 计算 ZoneMap 并写入 _zone_map section"
```

---

### Task 6: Zone Map — Iterator 块跳过逻辑

**目标:** Iterator.loadBlock 前检查 ZoneMap，跳过不满足过滤条件的块

**Files:**
- Modify: `internal/storage/shard/sstable/iterator.go:9-25` — Iterator 新增 zoneMapIndex 和 filterConds 字段
- Modify: `internal/storage/shard/sstable/iterator.go:27-44` — NewIterator 新增 filterConds 参数
- Modify: `internal/storage/shard/sstable/iterator_block.go:4-46` — loadBlock 新增 ZoneMap 跳过逻辑
- Modify: `internal/storage/shard/sstable/reader.go:13-22` — Reader 新增 zoneMapIndex 字段
- Modify: `internal/storage/shard/sstable/reader.go:23-96` — NewReader 读取 _zone_map section

- [ ] **Step 1: Reader 读取 ZoneMap section**

在 `reader.go` 的 Reader struct 新增字段：

```go
type Reader struct {
    // ... 现有字段 ...
    zoneMapIndex *ZoneMapIndex // 新增
}
```

在 `NewReader` 中读取 `_zone_map` section：

```go
// 在创建 blockSectionMap 之后添加:
var zmIndex *ZoneMapIndex
zmOffset, zmSize := sectionTable.Lookup("_zone_map")
if zmSize > 0 {
    zmData := make([]byte, zmSize)
    if _, err := f.ReadAt(zmData, int64(zmOffset)); err == nil {
        zmIndex, _ = UnmarshalZoneMapIndex(zmData)
    }
}

return &Reader{
    // ... 现有字段 ...
    zoneMapIndex: zmIndex, // 新增
}, nil
```

- [ ] **Step 2: Iterator 新增 zoneMapIndex 和 filterConds**

在 `iterator.go` 的 Iterator struct 新增：

```go
type Iterator struct {
    // ... 现有字段 ...
    zoneMapIndex *ZoneMapIndex             // 新增
    filterConds  []FilterCondition         // 新增：用于 ZoneMap 跳过的过滤条件
}
```

`FilterCondition` 是从 types.FilterCondition 简化来的（避免循环导入）：

```go
// 在 iterator.go 或 zone_map.go 中定义:
type FilterCondition struct {
    Field string
    Op    int32  // 对应 types.FilterOp 的值
    Value float64
}
```

- [ ] **Step 3: NewIterator 新增 filterConds 参数**

```go
// 修改前:
func (r *Reader) NewIterator(fields []string) (*Iterator, error) {

// 修改后:
func (r *Reader) NewIterator(fields []string, filterConds []FilterCondition) (*Iterator, error) {
    it := &Iterator{
        // ... 现有字段 ...
        zoneMapIndex: r.zoneMapIndex, // 新增
        filterConds:  filterConds,    // 新增
    }
    // ... 其余不变 ...
}
```

- [ ] **Step 4: loadBlock 新增 ZoneMap 跳过逻辑**

在 `loadBlock` 开头新增 ZoneMap 检查，在读取数据之前决定是否跳过整个 block：

```go
func (it *Iterator) loadBlock(blockIdx int) error {
    // ... 现有边界检查 ...

    // ZoneMap 跳过检查
    if it.zoneMapIndex != nil && len(it.filterConds) > 0 {
        if it.shouldSkipBlock(blockIdx) {
            it.blockRowCount = 0
            return nil
        }
    }

    // ... 现有加载逻辑 ...
}

// shouldSkipBlock 检查 zone map 是否表明整个 block 可被跳过。
func (it *Iterator) shouldSkipBlock(blockIdx int) bool {
    for _, cond := range it.filterConds {
        entry, ok := it.zoneMapIndex.Lookup(blockIdx, cond.Field)
        if !ok {
            continue // 该字段无 zone map，不能跳过
        }
        switch cond.Op {
        case 1: // GT
            if entry.Max <= cond.Value {
                return true
            }
        case 2: // GTE
            if entry.Max < cond.Value {
                return true
            }
        case 3: // LT
            if entry.Min >= cond.Value {
                return true
            }
        case 4: // LTE
            if entry.Min > cond.Value {
                return true
            }
        case 5: // EQ
            if entry.Min > cond.Value || entry.Max < cond.Value {
                return true
            }
        case 6: // NE
            // NE 无法用 min/max 跳过
        }
    }
    return false
}
```

- [ ] **Step 5: 更新 NewMergeIterator 透传 filterConds**

```go
// 修改前 (line 78):
func NewMergeIterator(filePaths []string, startTime, endTime int64, schema Schema, refMgr SSTableRefManager, fields []string) (*MergeIterator, error) {

// 修改后:
func NewMergeIterator(filePaths []string, startTime, endTime int64, schema Schema, refMgr SSTableRefManager, fields []string, filterConds []FilterCondition) (*MergeIterator, error) {
```

内部调用：

```go
// 修改前 (line 103):
iter, err := r.NewIterator(fields)

// 修改后:
iter, err := r.NewIterator(fields, filterConds)
```

- [ ] **Step 6: 更新所有 NewIterator/NewMergeIterator 调用方**

```bash
cd /root/projects/mts && grep -rn "NewIterator\|NewMergeIterator" --include="*.go" | grep -v "_test.go" | grep -v ".pb.go"
```

关键调用方更新：
- `ShardIterator`: `sstable.NewMergeIterator(sstFiles, startTime, endTime, schema, si.shard, fields, nil)` (暂不传 filterConds，后续 Task 7 补充)
- `ReadAll` (降采样等场景): `r.NewIterator(fields, nil)`
- 测试文件: 传 `nil`

- [ ] **Step 7: 运行测试**

```bash
cd /root/projects/mts && go build ./...
cd /root/projects/mts && go test ./internal/storage/shard/sstable/... -v -count=1 2>&1 | tail -30
```

Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add internal/storage/shard/sstable/iterator.go internal/storage/shard/sstable/iterator_block.go internal/storage/shard/sstable/reader.go internal/storage/shard/sstable/merge_iterator.go internal/storage/shard/sstable/
git commit -m "feat: Iterator.loadBlock 支持 ZoneMap 块跳过逻辑"
```

---

### Task 7: Zone Map — Engine 传递 Filter 条件到 Iterator

**目标:** Engine.Execute 从 FilterSpec 提取条件，传递到 SSTable Iterator 层用于 ZoneMap 跳过

**Files:**
- Modify: `internal/engine/engine_query.go:237-262` — Execute 提取 filter 条件
- Modify: `internal/engine/engine_query.go:265-331` — createDataIterator 传递 filterConds
- Modify: `internal/query/iterator.go:179-230` — NewIteratorWithMemTable 传递 filterConds 到 ShardIterator
- Modify: `internal/storage/shard/iterator.go:65-126` — ShardIterator 传递 filterConds 到 MergeIterator

- [ ] **Step 1: ShardIterator 新增 filterConds 参数透传**

```go
// 修改 NewShardIteratorWithMemTable 签名:
func NewShardIteratorWithMemTable(shard *Shard, externalMT *memtable.MemTable, extSeriesStore SeriesStore, startTime, endTime int64, maxRows int, fields []string, filterConds []sstable.FilterCondition) *ShardIterator {
```

在创建 MergeIterator 处：

```go
sstIter, err := sstable.NewMergeIterator(sstFiles, startTime, endTime, schema, si.shard, fields, filterConds)
```

更新 `NewShardIterator` 透传 nil：

```go
func NewShardIterator(shard *Shard, startTime, endTime int64, maxRows int) *ShardIterator {
    return NewShardIteratorWithMemTable(shard, nil, nil, startTime, endTime, maxRows, nil, nil)
}
```

- [ ] **Step 2: NewIteratorWithMemTable 新增 filterConds 参数**

```go
func NewIteratorWithMemTable(ctx context.Context, shards []*shard.Shard, writerMT *memtable.MemTable, extSeriesStore shard.SeriesStore, req *types.QueryRangeRequest, fields []string, filterConds []sstable.FilterCondition, unorderedData ...[]*types.PointRow) *Iterator {
```

内部传递给 ShardIterator：

```go
si = shard.NewShardIteratorWithMemTable(s, writerMT, extSeriesStore, startTimeNs, endTimeNs, maxRows, fields, filterConds)
```

- [ ] **Step 3: createDataIterator 新增 filterConds 参数**

```go
func (e *Engine) createDataIterator(database, measurement string, startTime, endTime int64, fields []string, filterConds []sstable.FilterCondition) (*query.Iterator, error) {
```

传递到 NewIteratorWithMemTable：

```go
return query.NewIteratorWithMemTable(context.Background(), shards, writerMT, scoped, req, fields, filterConds, unorderedData...), nil
```

- [ ] **Step 4: Execute 从 FilterSpec 提取条件并转换类型**

在 `Execute` 方法中：

```go
var filterConds []sstable.FilterCondition
for _, op := range plan.Ops {
    if f := op.GetFilter(); f != nil {
        for _, c := range f.Conditions {
            if c.Tag != "" {
                continue // tag 过滤不用于 ZoneMap
            }
            var val float64
            if c.Value != nil {
                val = c.Value.GetFloatValue()
            }
            filterConds = append(filterConds, sstable.FilterCondition{
                Field: c.Field,
                Op:    int32(c.Op),
                Value: val,
            })
        }
        break // 只有一个 Filter operator
    }
}

dataIter, err := e.createDataIterator(plan.Database, plan.Measurement, plan.StartTime, plan.EndTime, projFields, filterConds)
```

- [ ] **Step 5: 更新所有受影响的调用方**

```bash
cd /root/projects/mts && grep -rn "NewIteratorWithMemTable\|createDataIterator\|NewShardIteratorWithMemTable" --include="*.go" | grep -v "_test.go"
```

更新：
- `Iterator()` 方法: `createDataIterator` 调用传 nil filterConds
- `IteratorWithMemTable` 辅助函数: 传 nil filterConds
- 所有测试文件

- [ ] **Step 6: 运行测试**

```bash
cd /root/projects/mts && go build ./...
cd /root/projects/mts && go test ./internal/query/... ./internal/engine/... ./internal/storage/shard/... -v -count=1 2>&1 | tail -30
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/engine/engine_query.go internal/query/iterator.go internal/storage/shard/iterator.go
git commit -m "feat: Engine 传递 Filter 条件到 SSTable Iterator 用于 ZoneMap 跳过"
```

---

### Task 8: 并行 Shard 扫描

**目标:** 多 Shard 时各 Shard 独立 goroutine 扫描，结果通过 channel 汇聚到 heap merge

**Files:**
- Modify: `internal/query/iterator.go:179-230` — NewIteratorWithMemTable 改为并行模式

- [ ] **Step 1: 实现 channelIterator 适配器**

在 `iterator.go` 中添加：

```go
// channelIterator 将 channel 适配为 heapItem 接口，支持并行 Shard 扫描。
type channelIterator struct {
    ch   <-chan *types.PointRow
    cur  *types.PointRow
    done bool
}

func (c *channelIterator) Current() *types.PointRow {
    return c.cur
}

func (c *channelIterator) Next() *types.PointRow {
    if c.done {
        return nil
    }
    row, ok := <-c.ch
    if !ok {
        c.done = true
        return nil
    }
    c.cur = row
    return row
}

func (c *channelIterator) Close() {
    // 排空 channel 以释放 goroutine
    for range c.ch {
    }
}
```

- [ ] **Step 2: 实现并行 Shard 扫描逻辑**

修改 `NewIteratorWithMemTable`，替换 Shard 部分的串行创建为并行模式：

```go
// 并行 Shard 扫描
shardCount := len(shards)
if len(shards) == 0 && writerMT != nil {
    shardCount = 1
}

// 使用 goroutine 并行扫描各 Shard
ctx, cancel := context.WithCancel(ctx)
chans := make([]chan *types.PointRow, 0, shardCount+len(unorderedData))

for i, s := range shards {
    ch := make(chan *types.PointRow, 256)
    chans = append(chans, ch)
    go func(idx int, sh *shard.Shard) {
        defer close(ch)
        var si *shard.ShardIterator
        if idx == 0 && writerMT != nil {
            si = shard.NewShardIteratorWithMemTable(sh, writerMT, extSeriesStore, startTimeNs, endTimeNs, maxRows, fields, filterConds)
        } else {
            si = shard.NewShardIteratorWithMemTable(sh, nil, nil, startTimeNs, endTimeNs, maxRows, fields, filterConds)
        }
        defer si.Close()
        for {
            select {
            case <-ctx.Done():
                return
            default:
            }
            row := si.Current()
            if row == nil {
                return
            }
            select {
            case ch <- row:
            case <-ctx.Done():
                return
            }
            si.Next()
        }
    }(i, s)
}

// 没有 shard 但有 writerMT
if len(shards) == 0 && writerMT != nil {
    ch := make(chan *types.PointRow, 256)
    chans = append(chans, ch)
    go func() {
        defer close(ch)
        si := shard.NewShardIteratorWithMemTable(nil, writerMT, extSeriesStore, startTimeNs, endTimeNs, maxRows, fields, filterConds)
        defer si.Close()
        for {
            select {
            case <-ctx.Done():
                return
            default:
            }
            row := si.Current()
            if row == nil {
                return
            }
            select {
            case ch <- row:
            case <-ctx.Done():
                return
            }
            si.Next()
        }
    }()
}

// 构建 heap
q.heap = make(mergeHeap, 0, len(chans)+len(unorderedData))
for _, ch := range chans {
    ci := &channelIterator{ch: ch}
    if ci.Next() != nil {
        q.heap = append(q.heap, ci)
    }
}

// 添加 unordered 数据源
for _, rows := range unorderedData {
    if len(rows) == 0 {
        continue
    }
    q.heap = append(q.heap, &sliceIterator{rows: rows})
}

// 当所有 goroutine 结束后，取消 context
go func() {
    for _, ch := range chans {
        for range ch {
        }
    }
    cancel()
}()

heap.Init(&q.heap)
q.fetchNextValid()
return q
```

注意：需要存储 `cancel` 函数以便 Close 时取消。

需在 `Iterator` struct 中新增字段：
```go
type Iterator struct {
    // ... 现有字段 ...
    cancel context.CancelFunc // 新增：用于并行扫描的取消
}
```

在 `Close()` 中调用 cancel：
```go
func (q *Iterator) Close() error {
    q.closed = true
    if q.cancel != nil {
        q.cancel()
    }
    for _, si := range q.heap {
        si.Close()
    }
    return nil
}
```

- [ ] **Step 3: 添加单 Shard 场景的串行回退**

```go
// 并行度控制：Shard 数 ≤ 2 时直接串行（goroutine 开销 > 收益）
if shardCount <= 2 {
    // 走原有串行逻辑（保留原实现）
}
```

- [ ] **Step 4: 运行测试**

```bash
cd /root/projects/mts && go build ./...
cd /root/projects/mts && go test ./internal/query/... -v -count=1 -race 2>&1 | tail -30
```

Expected: PASS（无 race condition）

- [ ] **Step 5: Commit**

```bash
git add internal/query/iterator.go
git commit -m "feat: 多 Shard 并行扫描支持 goroutine 扇出"
```

---

## Phase 依赖与执行顺序

```
Phase 1 (并行): Task 1 → Task 2 + Task 3
                Column Projection 传递链完成后才能验证列裁剪效果
                但 Task 1 和 Task 3 的实现互不依赖

Phase 2 (串行): Task 4 → Task 5 → Task 6 → Task 7
                ZoneMap 类型定义 → Writer 写入 → Iterator 跳过 → Engine 传递

Phase 3 (独立): Task 8
                依赖 Phase 1+2 的 Shard 级稳定性（fields + filterConds 传递链就绪）
```

## 实施顺序

1. **Task 1 + Task 3** (Phase 1，可并行) — 列裁剪传递链 + Tags 缓存
2. **Task 2** (Phase 1) — 列裁剪 Engine 层集成（依赖 Task 1）
3. **Task 4 → 5 → 6 → 7** (Phase 2) — Zone Map 完整链路
4. **Task 8** (Phase 3) — 并行 Shard 扫描

## 验证

全部实现完成后运行：

```bash
cd /root/projects/mts && go build ./...
cd /root/projects/mts && go test ./... -count=1 2>&1 | tail -40
cd /root/projects/mts && golangci-lint run ./... 2>&1 | tail -20
cd /root/projects/mts/tests/e2e/query_op_benchmark && go build -o query_op_benchmark . && ./query_op_benchmark
```

清理临时构建产物：
```bash
rm -f /root/projects/mts/tests/e2e/query_op_benchmark/query_op_benchmark
```
