# []InternalField 内存优化分析

> 日期：2025-05-14
> 分析对象：MemTable 中 `[]InternalField` 切片的分配与拷贝开销

---

## 一、当前数据流与分配链路

### 1.1 写入路径（每条 Point 的分配）

```
gRPC Point (map[string]*FieldValue)
  │
  ├─► PointToInternal(p, sid)          make([]InternalField, 0, N)  ← 分配 #1 (N×24B)
  │     └─ InternalPoint{Fields: ...}
  │
  ├─► serializeInternalPoint(ip)       make([]byte, 0, size)        ← 读取 Fields
  │     └─ []byte (WAL 序列化)
  │
  ├─► wal.Write(data)                  CompressPayload → 池化       ← 不保留 data
  │
  └─► memTable.Write(ip)               make([]InternalField, N)     ← 分配 #2 (N×24B)
        └─ copy(fields, ip.Fields)                                  ← 深拷贝
```

**结论：每条 Point 在写入路径上有 2 次 `[]InternalField` 分配 + 1 次 copy。**

### 1.2 InternalField 结构

```go
type InternalField struct {
    Key   string          // 16B (ptr 8B + len 8B)
    Value *FieldValue     // 8B
}
// 总计: 24B / field
```

对于 5 个字段的数据点，每次 `[]InternalField` 分配 = 5 × 24B = 120B 的 backing array。

### 1.3 当前生命周期

| 阶段 | 操作 | 分配 | 存活时间 |
|------|------|------|---------|
| PointToInternal | 从 map 构造 []InternalField | 120B × N_fields | 写入完成即释放 |
| MemTable.Write | copy 一份新的 []InternalField | 120B × N_fields | 直到 MemTable flush |
| InternalFieldsToMap | 查询时还原为 map | N_fields 个 map entry | 查询响应完成 |
| MapToInternalFields | Compaction 合并时 | 120B × N_fields | 合并写入完成 |

---

## 二、优化方案对比

### 方案 A：消除 MemTable 字段拷贝（推荐立即执行）

```diff
func (m *MemTable) Write(ip types.InternalPoint) error {
-   fields := make([]types.InternalField, len(ip.Fields))
-   copy(fields, ip.Fields)
    m.active = append(m.active, types.InternalPoint{
        Timestamp: ip.Timestamp,
-       Fields:    fields,
+       Fields:    ip.Fields,  // 直接接管所有权
        Sid:       ip.Sid,
    })
```

**为什么安全？** 所有调用方在 `MemTable.Write` 返回后都不再使用 `ip.Fields`：
- `shard_io.go Write()`：ip 是局部变量，Write 返回后回收
- `shard_io.go WriteBatch()`：ips 切片在 memTable.Write 循环后不再使用
- `shard.go WAL replay`：deserialized ip 是局部变量

**收益**：每次写入消除 1 次 120B 切片分配 + copy（约占写路径 InternalField 相关分配的 50%）。

**风险**：极低——纯粹的代码删除，不改变任何所有权语义（调用方本身就不复用 Fields）。

---

### 方案 B：用紧凑 []byte 替代 []InternalField（推荐中期演进）

将 MemTable 中的 `InternalPoint` 替换为紧凑的序列化格式：

```go
// 当前 (68B + N×24B)
type InternalPoint struct {
    Timestamp int64          // 8B
    Fields    []InternalField // 24B (slice header)
    Sid       uint64          // 8B
    // 堆: N × 24B (InternalField array)
}

// 方案 B: 序列化字节 (32B + N×20B 紧凑编码)
type CompactPoint struct {
    Timestamp int64  // 8B
    Data      []byte  // 24B (slice header) — WAL 序列化格式
    Sid       uint64  // 8B
    // 堆: 变长 varint 编码字段
}
```

**内存对比**（5 字段，键平均 10 字符，float64 值）：

| 格式 | 计算 | 总大小 |
|------|------|--------|
| InternalField | 5 × 24B | **120B** |
| 紧凑序列化 | 2B(count) + 5×(2B+10B+1B+8B) | **107B** |

紧凑格式节省约 11%，但更大的优势在于：

1. **与 WAL 序列化合并**：`serializeInternalPoint` 的输出直接存入 MemTable，不再需要 `PointToInternal`
2. **零对象分散**：单一 `[]byte` 分配，对 GC 更友好（不是 N+1 个小对象）
3. **池化友好**：字节缓冲区可池化复用
4. **缓存友好**：连续内存 vs 指针跳转

**代价**：
- 查询时需要反序列化字段（当前已经需要 `InternalFieldsToMap` 转换）
- 排序需要比较 Timestamp（已在结构体字段中，无需反序列化）
- 架构改动较大，涉及 MemTable、Iterator、Flush、Compaction 等多个模块

---

### 方案 C：简单的 []InternalField 池化（不推荐）

```go
var fieldSlicePool = sync.Pool{
    New: func() any {
        s := make([]InternalField, 0, 8)
        return &s
    },
}
```

**不推荐原因**：
1. `[]InternalField` 存活在 MemTable 中直到 flush，生命周期长
2. Pool 的回收益需要在 flush 时触发，需要跨模块传递 pool 引用
3. 收益有限（只节省 backing array 分配，不节省空间）
4. 复杂度/收益比不划算

---

### 方案 D：不要 InternalField，直接存 FieldValue 键值对字节（参考方案）

如果字段键是有限集合（如 schemal 定义），可以使用字段 ID 替代字符串键：

```go
// 当前
InternalField{Key: "temperature", Value: &FieldValue{...}}
InternalField{Key: "humidity",    Value: &FieldValue{...}}

// 优化后
CompactField{ID: 1, Value: &FieldValue{...}}  // "temperature" → ID 1
CompactField{ID: 3, Value: &FieldValue{...}}  // "humidity" → ID 3
```

字段 ID 在 schema 注册时分配，字符串键只需存储一次。

**收益**：每个字段节省 ~16B（string header），同时减少散落字符串引用。
**代价**：增加 schema 查找层，查询时需要 ID→Key 映射。

---

## 三、推荐执行路径

| 阶段 | 方案 | 复杂度 | 收益 | 说明 |
|------|------|--------|------|------|
| **立即执行** | 方案 A：消除拷贝 | ★☆☆ | 50% InternalField 分配减少 | 纯删除代码，风险极低 |
| **本期执行** | Pool []byte + 紧凑格式 | ★★☆ | 写入路径 -70% 对象分配 | 需改 MemTable 存储格式 |
| **下期规划** | 方案 D：字段 ID 化 | ★★★ | 额外 16B/字段 + 字符串池化 | 依赖 schema 体系成熟 |

---

## 四、方案 B 详细设计摘要

如果采用方案 B（紧凑序列化字节），核心变更：

### 4.1 新 InternalPoint 定义
```go
type InternalPoint struct {
    Timestamp int64
    Data      []byte  // 直接用 serializeInternalPoint 的输出格式
    Sid       uint64
}
```

### 4.2 写入路径变为
```
Point → serializeDirectly → []byte
  ├─ wal.Write(data)              // WAL 压缩后不保留 data
  └─ memTable.Write(data)         // 接管 data 所有权，零额外分配
```

### 4.3 读取路径
- `MemTableIterator.Point()` → 返回 `[]byte`，调用方懒反序列化
- `ShardIterator.pointToRow()` → `deserializeFields(data)` → `map[string]*FieldValue`

### 4.4 排序
- Timestamp 已在结构体顶层，排序不依赖 Data 字段

---

## 五、建议

**本期先执行方案 A**（消除拷贝），改动 1 行代码，立即见效。

**方案 B** 核心优势是将 WAL 序列化 + MemTable 存储合并为一次分配，"一次序列化，两处使用"。可与方案 A 叠加，最终写入路径上 InternalField 相关分配降为零。是否需要我按方案 B 做详细设计？
