# 方案四：PointRow Fields 改为 Slice 设计方案

> 日期：2025-05-15
> 状态：设计阶段
> 关联：[InternalField 优化分析](../review/internalfield-optimization-analysis-2025-05-14.md)、[MemPoint 紧凑序列化](./mempoint-compact-serialization.md)

---

## 一、问题分析

### 1.1 当前状态

外部 protobuf 类型全部使用 `map<string, FieldValue>`：

| 类型 | 用途 | Fields 类型 |
|------|------|-------------|
| `Point` | 写入请求 | `map[string]*FieldValue` |
| `PointRow` | 查询结果行 | `map[string]*FieldValue` |
| `Row` | gRPC 响应行 | `map[string]*FieldValue` |
| `WriteRequest` | gRPC 写入 | `map[string]*FieldValue` |

内部类型已经全部改为 slice/compact 格式：

| 类型 | Fields 类型 |
|------|-------------|
| `InternalPoint` | `[]InternalField` |
| `MemPoint` | `[]byte` (FieldData) |

### 1.2 核心矛盾

存在 **5 处不必要的 map↔slice 双向转换**：

```
写入路径: map (Point) → slice (InternalPoint) → bytes (MemPoint)
查询路径: bytes (MemPoint) → slice (InternalField) → map (PointRow)  ← 每行分配!
合并路径: map (PointRow) → slice (InternalField)                       ← 每行转换!
gRPC 输出: map (PointRow) → map (Row)                                  ← 每行拷贝!
字段投影: map (PointRow) → map (PointRow)                              ← 每行分配!
```

### 1.3 map 的开销

每个 `map[string]*FieldValue` 的内存成本（5 字段为例）：

| 组件 | 大小 |
|------|------|
| map header (hmap) | 48B |
| 8 bucket slots | 144B |
| 5 key strings | 40B (string headers) |
| 5 value pointers | 40B |
| **总计** | **~272B** |

对比 `[]*FieldEntry` (slice)：

| 组件 | 大小 |
|------|------|
| slice header | 24B |
| 5 FieldEntry structs | 5 × 32B = 160B |
| **总计** | **~184B** |

**节省约 32%，且 GC 扫描代价更低（单块连续内存 vs 哈希桶链表）。**

### 1.4 字段数通常很少

时序数据库每条记录的字段数通常在 1-10 之间。在此范围内：
- **线性搜索 slice 比哈希查找更快**（缓存友好、无哈希计算开销）
- **确定性遍历顺序**（slice 保持插入顺序，map 无序）

---

## 二、方案设计

### 2.1 核心思路

在 proto 定义中新增 `FieldEntry` 消息，将 `PointRow` 和 `Row` 的 `map<string, FieldValue>` 改为 `repeated FieldEntry`，消除查询路径的每行 map 分配。

```proto
// 字段条目（替代 map<string, FieldValue>）
message FieldEntry {
  string     key   = 1;
  FieldValue value = 2;
}

message PointRow {
  uint64              sid       = 1;
  int64               timestamp = 2;
  map<string, string> tags      = 3;
  repeated FieldEntry fields    = 4;  // 原: map<string, FieldValue>
}

message Row {
  int64               timestamp = 1;
  map<string, string> tags      = 2;
  repeated FieldEntry fields    = 3;  // 原: map<string, FieldValue>
}
```

### 2.2 Point / WriteRequest 保持不变（短期内）

写入侧（`Point`、`WriteRequest`）暂时保留 `map<string, FieldValue>`，理由：
- 写入请求字段通常由客户端构造，map 语义更方便
- 写入路径已有 `serializeFieldsFromMap` 直接序列化，不经过 `[]InternalField`
- 改动客户端影响面太大

**如果后续要统一**，可同样改为 `repeated FieldEntry`，届时 `PointToInternal` 可零分配。

### 2.3 生成的 Go 代码变化

```go
// 当前 (protobuf 生成)
type PointRow struct {
    Sid       uint64
    Timestamp int64
    Tags      map[string]string
    Fields    map[string]*FieldValue  // ← map
}

// 方案四后 (protobuf 生成)
type PointRow struct {
    Sid       uint64
    Timestamp int64
    Tags      map[string]string
    Fields    []*FieldEntry           // ← slice
}

type FieldEntry struct {
    Key   string
    Value *FieldValue
}

type Row struct {
    Timestamp int64
    Tags      map[string]string
    Fields    []*FieldEntry           // ← slice
}
```

### 2.4 辅助函数

提供字段查找和转换的辅助函数，避免散落线性搜索代码：

```go
// GetField 从 PointRow.Fields 按名称查找字段值（线性搜索）。
// 适用于字段数 < 20 的场景，比 map 查找更快（缓存友好）。
func (r *PointRow) GetField(name string) *FieldValue {
    for _, f := range r.Fields {
        if f.Key == name {
            return f.Value
        }
    }
    return nil
}

// SetField 设置字段值（存在则替换，不存在则追加）。
func (r *PointRow) SetField(name string, v *FieldValue) {
    for i, f := range r.Fields {
        if f.Key == name {
            r.Fields[i] = &FieldEntry{Key: name, Value: v}
            return
        }
    }
    r.Fields = append(r.Fields, &FieldEntry{Key: name, Value: v})
}

// FieldsToMap 将 Fields slice 转换为 map（需要 map 语义时使用）。
func (r *PointRow) FieldsToMap() map[string]*FieldValue {
    if len(r.Fields) == 0 {
        return nil
    }
    m := make(map[string]*FieldValue, len(r.Fields))
    for _, f := range r.Fields {
        m[f.Key] = f.Value
    }
    return m
}

// FieldEntryToInternal 将 []*FieldEntry 转换为 []InternalField。
func FieldEntryToInternal(fields []*FieldEntry) []InternalField {
    if len(fields) == 0 {
        return nil
    }
    out := make([]InternalField, len(fields))
    for i, f := range fields {
        out[i] = InternalField{Key: f.Key, Value: f.Value}
    }
    return out
}

// InternalFieldsToFieldEntry 将 []InternalField 转换为 []*FieldEntry。
func InternalFieldsToFieldEntry(fields []InternalField) []*FieldEntry {
    if len(fields) == 0 {
        return nil
    }
    out := make([]*FieldEntry, len(fields))
    for i, f := range fields {
        out[i] = &FieldEntry{Key: f.Key, Value: f.Value}
    }
    return out
}
```

### 2.5 数据流对比

#### 查询路径

```
【优化前】
MemTable Iterator → MemPointToInternal → InternalFieldsToMap → PointRow  ← map 分配 (~272B)
SSTable Iterator  → make(map[string]*FieldValue)              → PointRow  ← map 分配 (~272B)
Query project     → make(map[string]*FieldValue)              → PointRow  ← map 分配 (~272B)

【优化后】
MemTable Iterator → MemPointToInternal → InternalFieldsToFieldEntry → PointRow  ← slice 分配 (~184B)
SSTable Iterator  → make([]*FieldEntry)                             → PointRow  ← slice 分配 (~184B)
Query project     → make([]*FieldEntry)                             → PointRow  ← slice 分配 (~184B)
```

#### 字段投影

```go
// 优化前：分配新 map
func (q *Iterator) projectFields(row *types.PointRow) *types.PointRow {
    filtered := make(map[string]*types.FieldValue)
    for _, name := range q.req.Fields {
        if v, ok := row.Fields[name]; ok {
            filtered[name] = v
        }
    }
    return &types.PointRow{Fields: filtered, ...}
}

// 优化后：分配新 slice，保持顺序
func (q *Iterator) projectFields(row *types.PointRow) *types.PointRow {
    if len(q.req.Fields) == 0 {
        return row
    }
    out := make([]*types.FieldEntry, 0, len(q.req.Fields))
    for _, f := range row.Fields {
        for _, name := range q.req.Fields {
            if f.Key == name {
                out = append(out, f)
                break
            }
        }
    }
    return &types.PointRow{Fields: out, ...}
}
```

#### Compaction 路径

```go
// 优化前
Fields: types.MapToInternalFields(row.Fields),  // map → []InternalField

// 优化后
Fields: types.FieldEntryToInternal(row.Fields),  // []*FieldEntry → []InternalField (直接拷贝)
```

---

## 三、兼容性分析

### 3.1 破坏性变更

| 影响面 | 说明 |
|--------|------|
| **gRPC 协议** | `map<string, FieldValue>` → `repeated FieldEntry`，二进制编码完全不同 |
| **所有客户端** | 需要重新生成 proto stub 并适配新 API |
| **流式查询** | `QueryRange` 返回 `stream Row`，Row.Fields 类型变更 |

### 3.2 非破坏性变更

| 范围 | 说明 |
|------|------|
| 磁盘格式 | SSTable、WAL、MemTable 均不依赖 PointRow 的 map 类型 |
| 内部类型 | InternalPoint、MemPoint、InternalField 不受影响 |
| 配置/元数据 | 不涉及 |

### 3.3 迁移路径

1. **同时发布新旧 API**：保留旧 gRPC 方法 + 新增 `QueryRangeV2` 等方法
2. **内部转换层**：旧 API 路由到新内部实现，做一层 map↔slice 转换
3. **逐步迁移**：客户端逐步切换到新 API
4. **废弃旧 API**：下个大版本移除

---

## 四、各路径分配数对比

| 路径 | 优化前（每行） | 优化后（每行） | 降幅 |
|------|---------------|---------------|------|
| **SSTable Iterator.Point()** | 1 map (~272B) | 1 slice (~184B) | 32% |
| **ShardIterator.pointToRow()** | InternalFieldsToMap (~272B) | InternalFieldsToFieldEntry (~184B) | 32% |
| **query.projectFields()** | 1 map (~272B) | 1 slice (~184B) | 32% |
| **pointRowToProto (gRPC)** | 1 map copy | slice 直接赋值 | 100% (仅 slice header) |
| **Compaction merge** | MapToInternalFields (~120B slice) | FieldEntryToInternal (~120B slice) | 持平 |
| **Schema 验证** | map iteration | slice iteration | 略快 |

**查询路径每行节省约 88B × 3 = 264B（map overhead）。**
100K 行查询可节省约 25MB 堆内存。

---

## 五、文件变更清单

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `proto/microts.proto` | **修改** | 新增 `FieldEntry` 消息；`PointRow.fields`、`Row.fields` 改为 `repeated FieldEntry` |
| `types/microts.pb.go` | **重新生成** | protobuf 代码生成 |
| `types/internal.go` | **修改** | 用 `FieldEntryToInternal` 替代 `MapToInternalFields`；用 `InternalFieldsToFieldEntry` 替代 `InternalFieldsToMap`；新增辅助函数 |
| `types/internal_test.go` | **修改** | 适配新类型 |
| `internal/storage/shard/iterator.go` | **修改** | `pointToRow` 使用 `InternalFieldsToFieldEntry` |
| `internal/storage/shard/sstable/iterator_next.go` | **修改** | `Point()` 构建 `[]*FieldEntry` 而非 `map[string]*FieldValue` |
| `internal/storage/shard/sstable/reader.go` | **修改** | `ReadAll` 构建 `[]*FieldEntry` |
| `internal/storage/shard/sstable/reader_range.go` | **修改** | `readRangeBlocks` 构建 `[]*FieldEntry` |
| `internal/query/iterator.go` | **修改** | `projectFields` 使用 slice 操作 |
| `internal/api/grpc.go` | **修改** | `pointRowToProto` 直接赋值 slice |
| `internal/storage/shard/shard.go` | **修改** | `ValidateFieldTypes` 适配 slice 遍历 |
| `internal/storage/compaction/merge.go` | **修改** | `FieldEntryToInternal` 替代 `MapToInternalFields` |
| `internal/storage/compaction/level.go` | **修改** | 同上 |
| `internal/engine/engine_test.go` | **修改** | 适配 `GetField()` 或 slice 遍历 |
| `types/convert_test.go` | **修改** | 适配新类型 |
| `tests/e2e/*/` | **修改** | 多个 e2e 测试适配 |

---

## 六、可选扩展：统一 Point / WriteRequest

如果后续要将 `Point.Fields` 和 `WriteRequest.Fields` 也改为 `repeated FieldEntry`：

**额外收益**：
- 消除 `PointToInternal` 中的 map→slice 转换分配
- `serializeFieldsFromMap` 改为 `serializeFieldsFromSlice`，无需 map 迭代
- 写入路径完全消除 map 语义

**额外成本**：
- 所有写入客户端需适配
- `SetField` / `GetField` 用法替代直接的 map 访问

**建议**：先实施 PointRow/Row 的变更（查询侧），待验证收益且客户端适配后，再推进 Point/WriteRequest 的变更（写入侧）。

---

## 七、风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| **gRPC 协议不兼容** | 旧客户端无法使用 | 新增 API 方法，保留旧方法一个版本 |
| **O(n) 按名访问字段** | 字段多时 `GetField(name)` 变慢 | 时序场景字段数 < 10，线性扫描反而比 map 快 |
| **重复字段键** | proto3 `repeated` 允许重复 key | 写入时自动去重（最后写入选） |
| **Tags 仍为 map** | Tags 仍有 map overhead | Tags 数量通常 < 5，影响小；后续可单独优化 |

---

## 八、验收标准

1. proto 重新生成成功，Go 类型正确
2. 所有现有测试适配后通过
3. `FieldEntry` 与 `InternalField` 往返转换测试通过
4. `GetField` / `SetField` / `FieldsToMap` 单元测试通过
5. 全量 e2e 测试通过
6. golangci-lint 零告警
7. Benchmark 测试：查询路径内存分配下降 ≥ 25%

---

## 九、任务分解

| # | 任务 | 涉及文件 | 预估 |
|---|------|---------|------|
| T1 | 修改 proto + 重新生成 Go 代码 | `proto/microts.proto`、`types/microts.pb.go` | 小 |
| T2 | 实现辅助函数 + 更新 `types/internal.go` | `types/internal.go` | 中 |
| T3 | 适配 `types/internal_test.go` | `types/internal_test.go` | 小 |
| T4 | 适配 SSTable 迭代器/读取器 | `sstable/iterator_next.go`、`reader.go`、`reader_range.go` | 中 |
| T5 | 适配 ShardIterator | `shard/iterator.go` | 小 |
| T6 | 适配查询迭代器 (projectFields) | `query/iterator.go` | 小 |
| T7 | 适配 Compaction 路径 | `compaction/merge.go`、`level.go` | 小 |
| T8 | 适配 gRPC 层 | `api/grpc.go` | 小 |
| T9 | 适配 Schema 验证 | `shard/shard.go` | 小 |
| T10 | 适配所有测试（单元 + e2e） | 10+ 测试文件 | 中 |
| T11 | 全量测试 + lint + e2e 验证 | — | 小 |

---

## 十、本次实施范围建议

**本期仅实施 PointRow + Row 的 Fields 改为 slice**（T1-T11），原因是：
1. 查询侧 map 分配是高优先级问题（每行 3 次 map 分配）
2. 写入侧已有 `serializeFieldsFromMap` 绕过了 `[]InternalField`
3. 客户端影响面可控（仅查询响应变更）
4. Point/WriteRequest 的变更可独立在下期进行
