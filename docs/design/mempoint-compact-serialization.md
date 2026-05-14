# 方案 B 详细设计：用序列化 []byte 替代 []InternalField

> 日期：2025-05-14
> 状态：设计阶段
> 关联：[内存分配优化分析](../review/memory-allocation-analysis-2025-05-14.md)

---

## 一、目标与动机

### 1.1 当前问题

每条数据点写入路径上，`[]InternalField` 有 **2 次独立分配**：

```
Point → PointToInternal (分配 #1: []InternalField)
  → serializeInternalPoint (读取 Fields, 产出 []byte)
  → MemTable.Write (分配 #2: 再次 make+copy []InternalField)
```

5 字段场景：每次 Write 浪费 2×120B = 240B 的堆分配。

### 1.2 设计目标

1. **合并分配**：WAL 序列化产物直接存入 MemTable，写入路径 `[]InternalField` 分配降为零
2. **紧凑内存**：序列化字节比 Go struct 更紧凑（连续内存，对 GC 友好）
3. **惰性解码**：仅在被查询时反序列化字段，flush/compaction 时批量解码
4. **向后兼容**：`InternalPoint` 保留用于 compaction 和短暂需要解码字段的场景

---

## 二、核心设计

### 2.1 新类型：`MemPoint`

```go
// types/internal.go

// MemPoint 是 MemTable 中存储的紧凑数据点。
// FieldData 使用 WAL v2 格式的字段部分（不含 version/ts/sid 头），
// 避免 []InternalField 切片分配。
//
// 内存布局（5 字段，键平均 10 字符）：
//   MemPoint: 8B(ts) + 8B(sid) + 24B(slice header) + 107B(FieldData) = 147B
//   对比 InternalPoint: 8B + 24B + 8B + 120B(backing array) = 160B
//   节省约 8%，但 FieldData 是单块连续内存，GC 扫描成本更低。
type MemPoint struct {
    Timestamp int64
    Sid       uint64
    FieldData []byte // 格式见 §2.2
}

// InternalPoint 保留用于短暂解码场景（flush、compaction）。
type InternalPoint struct {
    Timestamp int64
    Fields    []InternalField
    Sid       uint64
}
```

### 2.2 FieldData 序列化格式（WAL v2 字段部分）

```
FieldCount(2B, BigEndian)
+ [KeyLen(2B, BigEndian) + Key(variable) + Type(1B) + Value(variable)]...
```

其中 Type + Value 与 WAL v2 完全一致：

| Type | 含义 | Value 编码 |
|------|------|-----------|
| 0 | float64 | 8B BigEndian (IEEE 754) |
| 1 | int64 | 8B BigEndian |
| 2 | string | Len(2B) + UTF-8 bytes |
| 3 | bool | 1B (0=false, 1=true) |

**与 WAL 的关系**：WAL v2 完整格式 = `Version(1B) + Timestamp(8B) + Sid(8B) + FieldData`。
`FieldData` 即为 WAL 记录去掉前 17 字节头后的部分。

### 2.3 数据流对比

#### 写入路径

```
【优化前】
Point → PointToInternal → InternalPoint{Fields: []InternalField}  ← 分配 #1
  ├→ serializeInternalPoint → []byte (WAL)                          ← 分配 #3
  └→ MemTable.Write → copy Fields → 新的 []InternalField           ← 分配 #2

【优化后】
Point → serializeFieldsFromMap → []byte (FieldData)                 ← 唯一分配
  ├→ serializePointForWAL(ts, sid, FieldData) → []byte (WAL)       ← 池化复用
  └→ MemTable.Write(MemPoint{ts, sid, FieldData})                  ← 接管所有权
```

#### 查询路径

```
【优化前】
MemTable.Iterator → InternalPoint → InternalFieldsToMap → PointRow  ← 分配 map

【优化后】
MemTable.Iterator → MemPoint → MemPointToInternal → InternalFieldsToMap → PointRow
                                  ↑ 惰性解码（首次访问时）
```

---

## 三、新增与修改的函数

### 3.1 `types/internal.go` — 新增序列化/反序列化

```go
// serializeFieldsFromMap 直接将 map[string]*FieldValue 序列化为 FieldData 格式。
// 跳过 []InternalField 中间态，零额外分配。
func serializeFieldsFromMap(fields map[string]*FieldValue) []byte {
    if len(fields) == 0 {
        return nil
    }
    // 1. 预计算大小
    size := 2 // fieldCount
    for k, v := range fields {
        size += 2 + len(k) + 1 // keyLen + key + type
        switch val := v.GetValue().(type) {
        case *FieldValue_FloatValue, *FieldValue_IntValue:
            size += 8
        case *FieldValue_StringValue:
            size += 2 + len(val.StringValue)
        case *FieldValue_BoolValue:
            size += 1
        }
    }
    // 2. 分配并编码
    buf := make([]byte, 0, size)
    buf = appendU16(buf, uint16(len(fields)))
    for k, v := range fields {
        buf = appendU16(buf, uint16(len(k)))
        buf = append(buf, k...)
        switch val := v.GetValue().(type) {
        case *FieldValue_FloatValue:
            buf = append(buf, 0)
            var vb [8]byte
            binary.BigEndian.PutUint64(vb[:], math.Float64bits(val.FloatValue))
            buf = append(buf, vb[:]...)
        case *FieldValue_IntValue:
            buf = append(buf, 1)
            var vb [8]byte
            binary.BigEndian.PutUint64(vb[:], uint64(val.IntValue))
            buf = append(buf, vb[:]...)
        case *FieldValue_StringValue:
            buf = append(buf, 2)
            buf = appendU16(buf, uint16(len(val.StringValue)))
            buf = append(buf, val.StringValue...)
        case *FieldValue_BoolValue:
            buf = append(buf, 3)
            if val.BoolValue {
                buf = append(buf, 1)
            } else {
                buf = append(buf, 0)
            }
        }
    }
    return buf
}

// deserializeFieldData 从 FieldData 解码出 []InternalField。
func deserializeFieldData(data []byte) ([]InternalField, error) {
    if len(data) < 2 {
        return nil, fmt.Errorf("field data too short")
    }
    fieldCount := int(binary.BigEndian.Uint16(data[:2]))
    pos := 2
    fields := make([]InternalField, 0, fieldCount)
    for range fieldCount {
        // 解析 key
        if pos+2 > len(data) { return nil, fmt.Errorf("truncated key len") }
        kLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
        pos += 2
        if pos+kLen > len(data) { return nil, fmt.Errorf("truncated key") }
        key := string(data[pos : pos+kLen])
        pos += kLen
        // 解析 value
        if pos+1 > len(data) { return nil, fmt.Errorf("truncated type") }
        typ := data[pos]; pos++
        var fv *FieldValue
        switch typ {
        case 0: // float64
            fv = NewFieldValue(math.Float64frombits(binary.BigEndian.Uint64(data[pos:pos+8])))
            pos += 8
        case 1: // int64
            fv = NewFieldValue(int64(binary.BigEndian.Uint64(data[pos:pos+8])))
            pos += 8
        case 2: // string
            vLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
            pos += 2
            fv = NewFieldValue(string(data[pos : pos+vLen]))
            pos += vLen
        case 3: // bool
            fv = NewFieldValue(data[pos] == 1)
            pos++
        default:
            return nil, fmt.Errorf("unknown field type: %d", typ)
        }
        fields = append(fields, InternalField{Key: key, Value: fv})
    }
    return fields, nil
}

// MemPointToInternal 将 MemPoint 解码为 InternalPoint（惰性解码）。
func MemPointToInternal(mp MemPoint) (InternalPoint, error) {
    fields, err := deserializeFieldData(mp.FieldData)
    if err != nil {
        return InternalPoint{}, err
    }
    return InternalPoint{
        Timestamp: mp.Timestamp,
        Sid:       mp.Sid,
        Fields:    fields,
    }, nil
}

// PointToMemPoint 将外部 Point 直接序列化为 MemPoint（写入路径入口）。
func PointToMemPoint(p *Point, sid uint64) MemPoint {
    return MemPoint{
        Timestamp: p.Timestamp,
        Sid:       sid,
        FieldData: serializeFieldsFromMap(p.Fields),
    }
}
```

### 3.2 `shard/wal_serialize.go` — WAL 序列化适配

```go
// serializePointForWAL 将 ts + sid + FieldData 组装为完整 WAL 格式。
// 格式: Version(1B) + Timestamp(8B) + Sid(8B) + FieldData
func serializePointForWAL(ts int64, sid uint64, fieldData []byte) []byte {
    size := 1 + 8 + 8 + len(fieldData)
    buf := make([]byte, 0, size)
    buf = append(buf, pointVersion)
    var tmp [8]byte
    binary.BigEndian.PutUint64(tmp[:], uint64(ts))
    buf = append(buf, tmp[:]...)
    binary.BigEndian.PutUint64(tmp[:], sid)
    buf = append(buf, tmp[:]...)
    buf = append(buf, fieldData...)
    return buf
}

// deserializeFromWAL 从 WAL 完整格式解析出 MemPoint。
func deserializeFromWAL(data []byte) (types.MemPoint, error) {
    if len(data) < 19 {
        return types.MemPoint{}, fmt.Errorf("wal data too short: %d bytes", len(data))
    }
    if data[0] != pointVersion {
        return types.MemPoint{}, fmt.Errorf("unsupported point version: %d", data[0])
    }
    ts := int64(binary.BigEndian.Uint64(data[1:9]))
    sid := binary.BigEndian.Uint64(data[9:17])
    // FieldData 是 data[17:] 的副本（WAL data 缓冲区会被复用）
    fieldData := make([]byte, len(data)-17)
    copy(fieldData, data[17:])
    return types.MemPoint{
        Timestamp: ts,
        Sid:       sid,
        FieldData: fieldData,
    }, nil
}
```

注意：`deserializeFromWAL` 需要对 `FieldData` 做一次 copy，因为 WAL replay 的 data 缓冲区会被重用（WAL 分段读取）。这与优化前 `deserializeInternalPoint` 的行为一致（当时也需要对每个字段 key 做 `string(data[...])` 拷贝）。

### 3.3 `memtable/memtable.go` — 存储 MemPoint

```go
type MemTable struct {
    mu          sync.RWMutex
    active      []types.MemPoint      // 原: []types.InternalPoint
    passive     []types.MemPoint
    // ... 其余字段不变
}

func (m *MemTable) Write(mp types.MemPoint) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    // 直接追加，无需字段拷贝（MemPoint.FieldData 所有权转移给 MemTable）
    m.active = append(m.active, mp)
    m.activeCount++
    m.lastWrite = time.Now()
    if !m.sorted || (m.activeCount > 1 && m.active[m.activeCount-1].Timestamp < m.active[m.activeCount-2].Timestamp) {
        m.sortActive()
    }
    m.sorted = true
    return nil
}

// sortActive 排序基于 Timestamp，无需解码 FieldData。
func (m *MemTable) sortActive() {
    sort.Slice(m.active, func(i, j int) bool {
        return m.active[i].Timestamp < m.active[j].Timestamp
    })
    m.sorted = true
}
```

### 3.4 `memtable/memtable.go` — Iterator 惰性解码

```go
type MemTableIterator struct {
    active     []types.MemPoint
    passive    []types.MemPoint
    idxA       int
    idxP       int
    // 当前行缓存
    current    types.InternalPoint   // 惰性解码后的 InternalPoint
    hasCurrent bool
}

func (it *MemTableIterator) Next() bool {
    // ... 二路归并逻辑不变，选择下一个 MemPoint ...
    // 选中后解码为 InternalPoint
    ip, err := types.MemPointToInternal(mp)
    if err != nil {
        it.hasCurrent = false
        return false
    }
    it.current = ip
    it.hasCurrent = true
    return true
}

func (it *MemTableIterator) Point() types.InternalPoint {
    if !it.hasCurrent {
        return types.InternalPoint{}
    }
    return it.current
}
```

**关键设计**：Iterator 在 `Next()` 中解码，缓存到 `current`。`Point()` 返回 `InternalPoint`（不变），上游 `ShardIterator.pointToRow` 无需修改。

### 3.5 `shard/shard_io.go` — 写入路径

```go
// Write（单点）
func (s *Shard) Write(point *types.Point) error {
    // ... 背压检查不变 ...
    s.mu.Lock()

    sid, _ := s.seriesStore.AllocateSID(point.Tags)
    s.ValidateFieldTypes(point)

    // 一步到位：Point → MemPoint
    mp := types.PointToMemPoint(point, sid)

    // WAL：从 MemPoint 构造完整格式
    if s.wal != nil {
        walData := serializePointForWAL(mp.Timestamp, mp.Sid, mp.FieldData)
        s.wal.Write(walData)
    }

    // MemTable：直接存入（接管 FieldData 所有权）
    s.memTable.Write(mp)

    // ... 后续逻辑不变 ...
}

// WriteBatch（批量）
func (s *Shard) WriteBatch(points []*types.Point) (int, error) {
    // ...
    mps := make([]types.MemPoint, 0, len(points))
    walData := make([][]byte, 0, len(points))

    for i, point := range points {
        sid, _ := s.seriesStore.AllocateSID(point.Tags)
        s.ValidateFieldTypes(point)
        mp := types.PointToMemPoint(point, sid)
        mps = append(mps, mp)
        if s.wal != nil {
            walData = append(walData, serializePointForWAL(mp.Timestamp, mp.Sid, mp.FieldData))
        }
    }

    if s.wal != nil {
        s.wal.WriteBatch(walData)
    }
    for _, mp := range mps {
        s.memTable.Write(mp)
    }
    // ...
}
```

### 3.6 `shard/shard_flush.go` — Flush 时批量解码

```go
func (s *Shard) writeSSTableSync(points []types.MemPoint) error {
    // 批量解码为 InternalPoint（仅存活于 flush 期间）
    ips := make([]types.InternalPoint, len(points))
    for i, mp := range points {
        ip, err := types.MemPointToInternal(mp)
        if err != nil {
            return fmt.Errorf("decode mempoint %d: %w", i, err)
        }
        ips[i] = ip
    }
    // 写入 SSTable（接口不变）
    w, _ := sstable.NewWriter(...)
    w.WritePoints(ips)
    // ips 在函数返回后被 GC
    return w.Close()
}
```

### 3.7 `shard/shard.go` — WAL Replay

```go
// replayWAL 重放 WAL，直接产出 MemPoint
func (s *Shard) replayWAL() error {
    return s.wal.Replay(func(data []byte) error {
        mp, err := deserializeFromWAL(data)
        if err != nil {
            return err
        }
        return s.memTable.Write(mp)
    })
}
```

### 3.8 Compaction 路径 — 不变

Compaction 路径从 SSTable 读取 `*PointRow`（Fields 是 map），通过 `MapToInternalFields` 转为 `[]InternalField`，再通过 `sstable.WritePoints` 写入。这条路径不需要 MemPoint，因为数据来自 SSTable（已经是解码后的格式）。

---

## 四、各路径分配数对比

| 路径 | 优化前 | 优化后 | 降幅 |
|------|--------|--------|------|
| **Write (单点)** | PointToInternal(1) + MemTable copy(1) + WAL(1) = **3 次** | serializeFieldsFromMap(1) + WAL前缀(1, 池化) = **1 次新分配** | 67% |
| **WriteBatch (N点)** | N×PointToInternal(1) + N×MemTable copy(1) + N×WAL(1) = **3N 次** | N×serializeFieldsFromMap(1) + WAL前缀(N, 池化) = **N 次新分配** | 67% |
| **Query (每行)** | InternalFieldsToMap(1 map) = **1 次** | MemPointToInternal(1 slice) + InternalFieldsToMap(1 map) = **2 次** | -1 次 |
| **Flush (N点)** | 0（已有 InternalPoint） | MemPointToInternal(N slices) = **N 次** | +N 次 |
| **WAL Replay** | deserializeInternalPoint(1) + MemTable copy(1) = **2 次** | deserializeFromWAL(1 copy) = **1 次** | 50% |

**核心权衡**：写入路径大幅减少分配（热路径），代价是查询/刷盘时多一次解码（冷路径）。

---

## 五、文件变更清单

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `types/internal.go` | **重写** | 新增 `MemPoint`、`PointToMemPoint`、`MemPointToInternal`、`serializeFieldsFromMap`、`deserializeFieldData`；保留 `InternalPoint` |
| `memtable/memtable.go` | **修改** | `active/passive` 类型改为 `[]MemPoint`；`Write` 消除 copy；Iterator 惰性解码 |
| `memtable/memtable_test.go` | **修改** | `PointToInternal` → `PointToMemPoint`；调整断言 |
| `memtable/memtable_bench_test.go` | **修改** | 适配新类型 |
| `shard/wal_serialize.go` | **重写** | 拆分为 `serializePointForWAL` + `deserializeFromWAL`；移除旧函数 |
| `shard/shard_io.go` | **修改** | `Write`/`WriteBatch` 使用 `PointToMemPoint` |
| `shard/shard_flush.go` | **修改** | 参数类型 `[]InternalPoint` → `[]MemPoint`；flush 内批量解码 |
| `shard/shard.go` | **修改** | WAL replay 使用 `deserializeFromWAL` |
| `shard/iterator.go` | **不变** | `pointToRow` 接收 `InternalPoint`（Iterator 已解码） |
| `shard/iterator_test.go` | **修改** | 适配新类型 |
| `shard/shard_extra_test.go` | **修改** | 测试适配 |
| `shard/sstable/writer.go` | **不变** | 仍接收 `[]InternalPoint` |
| `shard/sstable/writer_field.go` | **不变** | 仍操作 `InternalPoint.Fields` |
| `compaction/merge.go` | **不变** | 从 `PointRow` 转为 `InternalPoint`（逻辑不变） |
| `compaction/level.go` | **不变** | 同上 |
| `query/iterator.go` | **不变** | 操作 `*PointRow`，与下层无关 |
| `api/grpc.go` | **不变** | 操作 `*PointRow` |

---

## 六、风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| **Flush CPU 增加** | 每点额外解码一次 | Flush 本身是 I/O bound，解码开销 < 5% |
| **Query 惰性解码** | Iterator.Next 多一次解码 | 结果缓存在 `it.current`，`Point()` 返回零开销 |
| **FieldData 生命周期** | MemPoint 持有 []byte，需确保不被意外释放 | MemTable 接管所有权；WAL 在 `Write` 返回后不保留原始 data |
| **内存碎片** | 大量小 []byte 分配 | 后续可引入 FieldData 池化 |

---

## 七、不在此方案中的优化（后续）

1. **FieldData 池化**：在 MemTable flush 后将 FieldData 归还池中
2. **字段 ID 化**：用 uint16 ID 替代字符串 Key（需 schema 体系支撑）
3. **Compaction 跳过解码**：SSTable MergeIterator 直接输出 MemPoint 格式

---

## 八、验收标准

1. 所有现有测试通过（适配新类型后）
2. 新增 `serializeFieldsFromMap` ↔ `deserializeFieldData` 往返测试
3. 新增 `PointToMemPoint` ↔ `MemPointToInternal` 往返测试
4. WAL write + replay 端到端测试通过
5. 写入基准测试：内存分配数降低 ≥ 50%
6. golangci-lint 零告警
7. 全量 e2e 测试通过

---

## 九、任务分解

| # | 任务 | 涉及文件 | 预估工作量 |
|---|------|---------|-----------|
| T1 | 实现 `MemPoint` + 序列化函数 | `types/internal.go` | 中 |
| T2 | 修改 `wal_serialize.go` | `shard/wal_serialize.go` | 小 |
| T3 | 修改 `MemTable` 存储 `MemPoint` | `memtable/memtable.go` | 中 |
| T4 | 修改 `Iterator` 惰性解码 | `memtable/memtable.go` | 小 |
| T5 | 修改 `Write`/`WriteBatch` | `shard/shard_io.go` | 中 |
| T6 | 修改 Flush 批量解码 | `shard/shard_flush.go` | 小 |
| T7 | 修改 WAL Replay | `shard/shard.go` | 小 |
| T8 | 适配所有测试 | 6+ 测试文件 | 中 |
| T9 | 新增往返序列化测试 | `types/internal_test.go` | 小 |
| T10 | 运行全量测试 + lint + e2e | — | 小 |
