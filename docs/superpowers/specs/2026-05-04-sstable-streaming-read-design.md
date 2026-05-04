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
| 当前实现 | 无块 | 无 | 无 | **线性扫描** | 无 |

---

## 3. 设计方案

### 3.1 SSTable 文件结构（改进后）

```
sstable/
├── _schema.json         # 字段类型定义
├── _timestamps.bin      # 时间戳数据（按时间排序）
├── _ts_index.bin        # 时间戳块索引（新增）
└── fields/
    ├── field1.bin       # 字段1数据
    ├── field1.idx.bin   # 字段1块索引（新增）
    ├── field2.bin
    └── field2.idx.bin
```

### 3.2 块索引格式

```
BlockIndex Entry (20 bytes):
┌──────────────────────────────────────────────────────────┐
│ first_timestamp: int64 (8 bytes) - 块内第一个时间戳      │
│ last_timestamp:  int64 (8 bytes) - 块内最后一个时间戳    │
│ offset:          uint32 (4 bytes) - 块在数据文件中的偏移  │
└──────────────────────────────────────────────────────────┘

_ts_index.bin 结构:
┌──────────────────────────────────────────────────────────┐
│ magic: [8]byte = "TSIDX001"                            │
│ version: uint32 = 1                                     │
│ block_count: uint32                                     │
│ entries: [block_count]BlockIndexEntry                   │
└──────────────────────────────────────────────────────────┘
```

### 3.3 Writer 修改

**写入流程**：
1. 收集数据到 buffer（64KB per block）
2. 达到 block size 时，写入 block 数据
3. 记录 block 的 first/last timestamp 和 offset
4. 所有数据写完后，写入 block index

```go
// WriteBlock 写入一个数据块
func (w *Writer) WriteBlock(timestamps []int64, fieldValues map[string][]byte) error {
    offset := w.calcCurrentOffset()

    // 写入 timestamps block
    if err := w.writeTimestampBlock(timestamps); err != nil {
        return err
    }

    // 写入各字段 block
    for name, values := range fieldValues {
        if err := w.writeFieldBlock(name, values); err != nil {
            return err
        }
    }

    // 记录 block index entry
    w.blockIndex = append(w.blockIndex, BlockIndexEntry{
        FirstTimestamp: timestamps[0],
        LastTimestamp:  timestamps[len(timestamps)-1],
        Offset:         offset,
    })
    return nil
}
```

### 3.4 Reader 修改

**读取流程**：
1. 加载 block index 到内存（很小，约几 KB）
2. 二分查找第一个 `last_timestamp >= startTime` 的 block
3. 从该 block 开始顺序读取
4. 遇到 `first_timestamp >= endTime` 时停止

```go
// Iterator SSTable 流式迭代器
type Iterator struct {
    reader       *Reader
    blockIndex   []BlockIndexEntry
    blockData    []byte        // 当前 block 的原始数据
    curBlock     int           // 当前 block 索引
    pos          int           // block 内位置
    startTime    int64
    endTime      int64

    // 解码后的当前行
    curTimestamp int64
    curFields    map[string]any
}

// SeekToTime 使用二分查找定位到起始时间
func (it *Iterator) SeekToTime(target int64) error {
    // 二分查找第一个 last_timestamp >= target 的 block
    blockIdx := sort.Search(len(it.blockIndex), func(i int) bool {
        return it.blockIndex[i].LastTimestamp >= target
    })

    if blockIdx >= len(it.blockIndex) {
        return nil // 没有更多数据
    }

    it.curBlock = blockIdx
    return it.loadBlock(blockIdx)
}

// loadBlock 只加载单个 block 到内存
func (it *Iterator) loadBlock(idx int) error {
    entry := it.blockIndex[idx]

    // 读取该 block 的数据
    data := make([]byte, it.blockSize)
    n, err := it.reader.readAt(entry.Offset, data)
    if err != nil {
        return err
    }

    it.blockData = data[:n]
    it.pos = 0
    return nil
}

// Next 移动到下一个点
func (it *Iterator) Next() bool {
    for {
        // 当前 block 已耗尽，加载下一个
        if it.pos >= it.blockEntry.RowCount {
            it.curBlock++
            if it.curBlock >= len(it.blockIndex) {
                return false
            }
            if err := it.loadBlock(it.curBlock); err != nil {
                return false
            }
        }

        // 检查是否超出查询范围
        if it.curTimestamp >= it.endTime {
            return false
        }

        it.pos++
        return true
    }
}
```

### 3.5 内存占用对比

| 场景 | 改进前 | 改进后 |
|------|--------|--------|
| 100万条，10字段 | ~800MB 全加载 | ~64KB × 块数 |
| 单次查询覆盖 1% 数据 | 加载全部 800MB | 只加载 ~8MB |

---

## 4. 实现计划

### 4.1 阶段一：块索引结构

- [ ] 定义 BlockIndexEntry 结构
- [ ] 实现 Writer 块收集和索引构建
- [ ] 实现 Reader 加载索引

### 4.2 阶段二：流式读取

- [ ] 实现 SeekToTime 二分查找
- [ ] 实现按需加载单个 block
- [ ] 修改 ShardIterator 使用流式迭代器

### 4.3 阶段三：压缩（可选）

- [ ] 添加 per-block snappy 压缩
- [ ] 读取引擎添加解压

---

## 5. 关键文件

| 文件 | 变更 |
|------|------|
| `internal/storage/shard/sstable/writer.go` | 添加块收集和索引写入 |
| `internal/storage/shard/sstable/reader.go` | 添加索引读取 |
| `internal/storage/shard/sstable/iterator.go` | 重写为流式迭代器 |
| `internal/storage/shard/iterator.go` | 使用新的流式迭代器 |

---

## 6. 验收标准

- [ ] Block index 正确写入和读取
- [ ] 二分查找正确跳转到目标时间
- [ ] 只加载查询范围内的 block
- [ ] 内存占用显著降低
- [ ] e2e integrity 测试通过
