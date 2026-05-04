# SSTable 流式读取设计

## 1. 概述

**问题**：当前 SSTable Iterator 在创建时将整个文件加载到内存，导致查询时内存占用过高。

**目标**：实现真正的流式读取，按需加载数据块，支持时间范围过滤。

---

## 2. 行业方案对比

### 2.1 InfluxDB TSM

```
┌─────────────────────────────────────────────────────────┐
│  Header (16 bytes): Magic + Version                    │
├─────────────────────────────────────────────────────────┤
│  Index Section (按 key+time 排序)                      │
│  IndexEntry[]: [key][min_time][max_time][offset][size] │
├─────────────────────────────────────────────────────────┤
│  Data Blocks (4KB chunks)                              │
│  - delta-of-delta 压缩时间戳                           │
│  - Gorilla XOR 压缩浮点数                              │
├─────────────────────────────────────────────────────────┤
│  Footer: Index offset pointer                          │
└─────────────────────────────────────────────────────────┘
```

**特点**：
- 4KB 小块，内存效率高
- 索引按 `(key, time)` 排序，支持多维查询
- 每个 block 独立压缩
- 查找：二分查找索引定位 block

### 2.2 VictoriaMetrics

```
┌─────────────────────────────────────────────────────────┐
│  .dat 文件（数据）                                      │
│  - 64KB block 压缩（snappy）                           │
│  - 按列存储                                             │
├─────────────────────────────────────────────────────────┤
│  .idx 文件（索引）                                      │
│  Header: min_time, max_time, row_count                 │
│  BlockIndex[]: [first_value][last_value][offset]       │
│  每列独立索引                                           │
└─────────────────────────────────────────────────────────┘
```

**特点**：
- 64KB 大块，压缩率高
- 索引按时间排序，二分查找
- 索引与数据分离
- mmap 友好

### 2.3 当前实现问题

```go
// iterator.go NewIterator - 问题代码
func (r *Reader) NewIterator() (*Iterator, error) {
    // 1. 把整个 timestamps 文件加载到内存
    tsData, err := os.ReadFile(...)

    // 2. 把每个字段的整个 .bin 文件加载到内存！
    for _, name := range fields {
        data, err := os.ReadFile(...)
        fieldData[name] = data  // 全部加载
    }
}
```

**对比**：

| 方案 | 块大小 | 索引位置 | 索引排序 | 时间查找 | 压缩 |
|------|--------|----------|----------|----------|------|
| InfluxDB TSM | 4KB | 内嵌 | key+time | 二分 | per-block |
| VictoriaMetrics | 64KB | 分离 | time | 二分 | per-block snappy |
| 当前实现 | 无块 | 无 | **线性扫描** | **线性扫描** | 仅 timestamp |

---

## 3. 设计决策

### 3.1 Block 大小

**决策**：64KB（与 VictoriaMetrics 一致）

**理由**：
- 时序数据写入模式稳定，大块压缩效率更好
- 当前 MemTable flush 触发就是 64MB，block 大小对齐自然
- 相比 4KB 小块，索引更小，压缩率更高

### 3.2 索引加载策略

**决策**：全量加载索引到内存

**理由**：
- 索引文件很小（每个 block 20 bytes，100万条数据约 1000 个 block，约 20KB）
- 二分查找直接在内存进行，无额外 IO
- 实现简单，不需要处理 mmap 的复杂边界情况
- 与 InfluxDB 成熟方案一致

### 3.3 索引文件结构

**决策**：统一索引 `_index.bin`

**结构**：

```
_index.bin:
┌──────────────────────────────────────────────────────────┐
│ Header (16 bytes)                                        │
│   - Magic: [8]byte = "TSIDX001"                        │
│   - Version: uint32 = 1                                 │
│   - BlockCount: uint32                                  │
├──────────────────────────────────────────────────────────┤
│ Entries (BlockCount × 20 bytes):                        │
│   - FirstTimestamp: int64 (8 bytes)                     │
│   - LastTimestamp:  int64 (8 bytes)                     │
│   - Offset:         uint32 (4 bytes) - 文件内偏移       │
└──────────────────────────────────────────────────────────┘
```

**理由**：
- timestamps 和字段数据的 block 边界一致（同时写入）
- 一个索引覆盖所有列，简化实现

### 3.4 Block 内数据布局

**决策**：行式布局

```
Block 数据结构：
┌─────────────────────────────────────────────────────────────┐
│ BlockHeader (16 bytes)                                     │
│   - Magic: [4]byte = "BLKH"                               │
│   - Version: uint32 = 1                                    │
│   - RowCount: uint32                                       │
│   - FirstTimestamp: int64                                  │
├─────────────────────────────────────────────────────────────┤
│ Timestamps[]: delta-of-delta 编码                         │
├─────────────────────────────────────────────────────────────┤
│ Field1[]: 类型特定编码（float64/int64/string/bool）        │
├─────────────────────────────────────────────────────────────┤
│ Field2[]: ...                                              │
└─────────────────────────────────────────────────────────────┘
```

**理由**：
- 行式布局解码后直接是 row by row，ShardIterator 消费方便
- 列式布局需要分别追踪每个列的当前位置，复杂
- 变长字段（string）通过偏移表解决

### 3.5 压缩策略

**决策**：保持现状（仅 timestamp delta 编码）

**当前状态**：
| 字段类型 | 编码方式 |
|----------|----------|
| timestamp | Delta 编码 |
| float64 | 无压缩（8字节 raw big-endian） |
| int64 | 无压缩（8字节 raw big-endian） |
| string | 无压缩（4字节长度 + 原始 bytes） |
| bool | 无压缩（1字节） |

**理由**：
- 保持实现简单，先实现流式读取
- 后续可单独添加 float64 Gorilla 编码优化

### 3.6 MemTable Flush

**决策**：MemTable 直接 Flush 到 SSTable，不经过 block 缓冲

**理由**：
- 实现简单，不需要额外的 block 缓冲逻辑
- MemTable flush 是后台行为，不在关键路径上
- 小 block 不会造成功能问题，只是压缩效率略低
- 后续可以在 Writer 层面做 block 合并优化

### 3.7 向后兼容性

**决策**：Reader 降级 + 后台重建索引

**处理流程**：
```
Reader 初始化时：
1. 检查 _index.bin 是否存在
2. 存在 → 加载索引，二分查找
3. 不存在 → 降级：读取全部 timestamps 到内存，线性扫描
```

### 3.8 迭代器边界

**决策**：方案A（改进后）- 分层过滤

**职责划分**：

| 组件 | 职责 |
|------|------|
| SSTable Iterator | 块级读取 + block 过滤 |
| ShardIterator | 行级时间过滤 + MemTable/SSTable 归并 |

**SSTable Iterator 提供的接口**：
- `SeekToBlock(targetTime)` - 二分查找定位起始 block
- `BlockFirstTimestamp()` - 获取当前 block 的起始时间
- `Next()` - 移动到下一个点
- `Point()` - 获取当前点数据

**过滤流程**：

```
ShardIterator.nextSstRow():
1. 首次调用时：
   - SSTable Iterator 加载 block index
   - 二分查找第一个 block（LastTimestamp >= startTime）
   - 加载该 block 的数据

2. 每次 Next():
   - 如果当前 block 耗尽：
     - 加载下一个 block
     - 检查 block.FirstTimestamp >= endTime？若是则停止
   - 解码当前 row
```

---

## 4. SSTable 文件结构（改进后）

```
sstable/
├── _schema.json         # 字段类型定义
├── _index.bin          # 块索引（新增）
├── _timestamps.bin     # 时间戳数据
└── fields/
    ├── field1.bin      # 字段1数据
    ├── field2.bin      # 字段2数据
    └── ...
```

---

## 5. 实现计划

### 5.1 阶段一：Writer 修改

- [ ] 添加 BlockIndexEntry 结构
- [ ] 添加 block 缓冲逻辑
- [ ] 写入索引文件

### 5.2 阶段二：Reader 修改

- [ ] 添加索引读取
- [ ] 添加向后兼容降级逻辑

### 5.3 阶段三：SSTable Iterator

- [ ] 重写为流式迭代器
- [ ] 实现 SeekToBlock 二分查找
- [ ] 实现按需加载单个 block
- [ ] 实现 Next/Point 接口

### 5.4 阶段四：ShardIterator

- [ ] 修改使用新的 SSTable Iterator
- [ ] 确保时间过滤正确

### 5.5 阶段五：测试

- [ ] 单元测试
- [ ] e2e integrity 测试
- [ ] 内存占用验证

---

## 6. 内存占用对比

| 场景 | 改进前 | 改进后 |
|------|--------|--------|
| 100万条，10字段 | ~800MB 全加载 | ~64KB × 块数 |
| 单次查询覆盖 1% 数据 | 加载全部 800MB | 只加载 ~8MB |

---

## 7. 验收标准

- [ ] Block index 正确写入和读取
- [ ] 二分查找正确跳转到目标时间
- [ ] 只加载查询范围内的 block
- [ ] 内存占用显著降低
- [ ] e2e integrity 测试通过
- [ ] 向后兼容旧文件
