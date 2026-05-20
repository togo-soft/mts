# 查询性能优化 — 按 Block 粒度解码

## 问题本质

当前查询路径存在结构性缺陷：无论查询范围匹配多少行，始终解码**文件中全部行**的字段数据（O(total_rows)），而非仅解码匹配行（O(matching_rows)）。数据量从 100K 增长到 1M 时，SSTable 文件数从 ~33 增加到 ~333，每文件仍全量解码，总解码量放大 10 倍以上。

核心代码：`reader_range.go:74-75`
```go
rowCount := int(r.header.RowCount)  // ← 文件总行数
decodedFields, err := r.ReadAllDecodedFieldSections(fields, rowCount)
```

## 目标

- 查询耗时随数据集规模**线性增长**（而非当前的非线性增长）
- 对于返回 N 行的查询，解码工作量与 N 成正比（加上必要的 block 对齐开销）
- 保持现有压缩率不显著退化（<5%）

## 约束

- 不改动 gRPC API 和对外接口
- 不改动 Shard 层的 Read/Write 签名
- 兼容现有 E2E 测试用例
- 文件格式升级到 Version 2（不兼容 v1，系统尚在开发阶段）

## EARS 需求

### R1 — Block 级独立解码

**[R1.1]** WHEN Writer 最终化 SSTable 时，SHALL 对每个 block 的数据独立编码（编码器状态在 block 边界重置），使得任意 block 的数据可独立解码而不依赖前序 block。

**[R1.2]** WHEN Writer 最终化 SSTable 时，SHALL 在每个 section 内部记录各 block 的字节偏移量，使得 Reader 可以直接定位到特定 block 的字节范围。

### R2 — 查询时按需解码

**[R2.1]** WHEN Reader 执行 `ReadRange(start, end)` 时，SHALL 仅解码与 `[start, end)` 有时间重叠的 block 中的数据，SHALL NOT 解码无关 block 的数据。

**[R2.2]** WHEN 查询匹配 N 个 block 时，字段解码操作 SHALL 仅对 N 个 block 的累积行数执行，而非文件总行数。

### R3 — 文件级时间过滤

**[R3.1]** FileHeader SHALL 包含文件中全部数据的 min/max timestamp，使得 Shard 层可在打开文件前判断文件是否包含目标时间范围的数据。

**[R3.2]** WHEN `readFromSSTable` 遍历 SSTable 文件时，SHALL 跳过 header 时间范围与查询无重叠的文件。

### R4 — 压缩率保持

**[R4.1]** 独立 block 编码后的总存储大小 SHALL 不超过当前连续编码大小的 105%（允许 <5% 的压缩率退化，因 block 边界编码器重置导致）。

### R5 — 测试覆盖

**[R5.1]** 所有现有 E2E 测试 SHALL 全部通过。

**[R5.2]** 新增 block 级范围查询的单元测试，覆盖：单 block 查询、跨 block 查询、首/尾 block 查询、空结果查询。
