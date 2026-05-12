# SSTable 字段惰性解码与查询截断设计

## 1. 概述

**问题**：当前 `loadBlock` 和 `readRangeBlocks` 在 block 内贪婪解码全部字段的全部行（8000行 × N字段），即使查询只需要 2 个字段且 LIMIT 10。

**目标**：
1. Iterator 支持字段投影，仅解码需要的字段
2. ReadRange 解码量受 maxRows 限制，实现真正的早期终止
3. 不保留 `NewIterator(reader)` 兼容接口，统一为 `NewIterator(reader, fields)`

---

## 2. 设计

### 2.1 Iterator 结构变更

```go
type Iterator struct {
    reader       *Reader
    blockIndex   []BlockIndexEntry
    currentBlock int

    blockTimestamps  []int64
    blockSids        []uint64
    blockFieldData   map[string][]byte            // NEW: 解压后原始字节，惰性解码源
    blockFieldValues map[string][]*types.FieldValue // 懒加载缓存
    blockRowCount    int
    pos              int

    projectedFields  []string  // NEW: nil=全部字段
}
```

### 2.2 loadBlock 拆分

```
Before: decode timestamps + SIDs + ALL fields (全量解码后存储)
After:
  1. decode timestamps (必须，用于 Point() 和过滤)
  2. decode SIDs (必须)
  3. decompress 字段 block 数据 → 存入 blockFieldData (不解码，仅解压)
  4. 清除 blockFieldValues 缓存
  5. 如果 projectedFields != nil，仅解压这些字段的 block 数据
```

### 2.3 Point 惰性解码

```
Point():
  for each field in (projectedFields or all fields):
    if blockFieldValues[field] == nil:       // 缓存未命中
      data := blockFieldData[field]          // 已解压的原始字节
      blockFieldValues[field] = decodeAll(data, blockRowCount)
    row.Fields[field] = blockFieldValues[field][pos]
```

### 2.4 NewIterator 签名变更

```go
// Before:
func (r *Reader) NewIterator() (*Iterator, error)

// After:
func (r *Reader) NewIterator(fields []string) (*Iterator, error)
// fields=nil → 全部字段 (backward compat for merge)
```

### 2.5 ReadRange 早期终止

当前 `readRangeBlocks` 流程：
```
decode all timestamps → decode all fields → iterate rows → check maxRows
```

改为：
```
decode all timestamps → find matching row indices → truncate to maxRows
  → for each field: decode up to max(matching_row_indices) rows
  → only extract matching row indices
```

关键：编码格式天然支持截断（所有 `Decode*(data, count)` 已接受 count 参数），只需传入 `maxNeededRow+1` 而非 `rowCount`。

### 2.6 调用方适配

| 位置 | 旧调用 | 新调用 |
|------|-------|-------|
| `merge.go` | `r.NewIterator()` | `r.NewIterator(nil)` |
| `level.go` | `r.NewIterator()` | `r.NewIterator(nil)` |
| 所有测试 | `r.NewIterator()` | `r.NewIterator(nil)` |

---

## 3. 收益

| 场景 | Before | After |
|------|--------|-------|
| merge k=10, 5 字段 | 10 × 8000 × (5×8+16)B ≈ 3.2MB | 10 × 8000 × 16B ≈ 1.3MB (字段惰性，按需+cache) |
| ReadRange LIMIT 10, 5 字段 | 解码全部匹配 block 的全部行 | 仅解码所需行（~10-100行） |
| 宽表字段投影 (3/10) | 全解码 10 字段 | 仅解码 3 字段，内存降 ~3x |
| 首行延迟 | 等待全部字段解码 | 仅等 timestamps+sids，首字段延迟性解码 |

---

## 4. 不改的范围

- **不实现逐行流式解码器**（方案 B）：编码格式为有状态顺序解码，逐行流式复杂度高，对 64KB/8000 行 block 收益有限
- **不改变 block 内 timestamps/SIDs 解码策略**：这两者必须全量解码以支持时间过滤和去重
- **不改变 MergeIterator**：compaction merge 需要全字段，`NewIterator(nil)` 保持等价行为

---

## 5. 测试计划

### 5.1 Iterator 字段投影
- 写入 3 字段 SSTable，`NewIterator(r, []string{"cpu"})` → Point() 仅返回 cpu
- 验证 blockFieldData 仅包含 cpu 原始字节
- `NewIterator(r, nil)` → 返回全部字段

### 5.2 惰性解码缓存
- loadBlock 后 blockFieldValues 为空
- 首次 Point() 触发解码，二次 Point() 命中缓存
- block 切换后缓存被清除，新 block 首次 Point() 重新触发

### 5.3 ReadRange 截断
- 1000 行数据，ReadRange(maxRows=10) → 解码 ≤10 行
- 验证截断后数据正确（不多不少）
- 跨 block 场景截断：第一个 block 产出足够行后不再解码后续 block

### 5.4 回归
- `NewIterator(nil)` 行为等价旧 `NewIterator()`
- 所有单元测试 + E2E 测试通过
