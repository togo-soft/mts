# 查询性能优化 — 任务清单

## 阶段 1：BlockSectionMap 数据结构

### T1.1 定义 BlockSectionMap 类型
- 文件：`internal/storage/shard/sstable/block_map.go`
- 内容：`BlockSectionMap`、`BlockSectionOffsets` 结构体
- 方法：`Marshal()`, `Unmarshal(data []byte) error`, `ReadRanges(blockIndices []int, fields []string)`
- 验收：单元测试覆盖 round-trip 序列化/反序列化

### T1.2 实现 BlockSectionMap 序列化
- `Marshal()` 输出二进制格式
- `Unmarshal()` 解析并验证
- 验收：`go test ./internal/storage/shard/sstable/ -run BlockSectionMap`

---

## 阶段 2：Writer 改造（按 block 独立编码）

### T2.1 改造 encodeTimestampsSection → V2
- 文件：`writer_close.go`
- 修改 `encodeTimestampsSection`：读取全部原始数据 → 按 block 行范围分割 → 每 block 独立调用 `compression.EncodeTimestamps` → 拼接 → 返回 `(data, offsets, encoding, error)`
- 验收：生成的 section 数据中各 block 可独立解码

### T2.2 改造 encodeSidsSection → V2
- 同上模式，每 block 独立 `compression.EncodeSids`
- 验收：同上

### T2.3 改造 encodeFieldSection → V2
- 同上模式，按字段类型分别调用对应编码函数
- 验收：同上

### T2.4 修改 Close() 生成 _block_map section
- 收集所有 section 的 per-block offsets
- 构建 BlockSectionMap
- 序列化并作为 `_block_map` section 写入文件
- 在 SectionTable 中添加 `_block_map` entry
- 升级 FileHeader.Version 为 2

---

## 阶段 3：Reader 改造（按字节范围解码）

### T3.1 新增按字节范围解码方法
- 文件：`reader_blocks.go`
- `readTimestampsBlock(offset, size uint64, rowCount int) ([]int64, error)` — 从 section 指定偏移读取并解码
- `readSidsBlock(offset, size uint64, rowCount int) ([]uint64, error)` — 同上
- `decodeFieldSectionBlock(name string, offset, size uint64, rowCount int) ([]*types.FieldValue, error)` — 同上
- 验收：单元测试使用手工构造的编码数据

### T3.2 加载 BlockSectionMap
- 文件：`reader.go`
- 在 `NewReader` 中，从 SectionTable 查找 `_block_map` entry
- 读取并 `BlockSectionMap.Unmarshal`
- 验收：读取 v2 文件后 blockSectionMap 非 nil

### T3.3 改造 readRangeOptimized
- 文件：`reader_range.go`
- 旧流程：收集匹配 block → 全量解码字段 → 过滤
- 新流程：收集匹配 block → 从 BlockSectionMap 获取各 section 字节范围 → 逐 block 局部解码 → 拼接结果
- 注意：每 block 内部仍需按 timestamp 过滤（block 可能部分匹配）
- 验收：`go test ./internal/storage/shard/sstable/ -run ReadRange`

### T3.4 保留 v1 兼容读取路径
- `reader_range.go` 中检测 Version，v1 文件走旧路径（全量解码）
- 或直接要求所有测试环境强制使用 v2

---

## 阶段 4：Shard 层文件级过滤（可选）

### T4.1 readSSTableFile 增加时间范围预检
- 文件：`shard_io.go`
- 解析 file header + blockIndex 后，计算文件级 min/max timestamp
- 若与查询 [start, end) 无交集，跳过后续解码
- 验收：查询不覆盖的 SSTable 文件不被打开

---

## 阶段 5：测试与验证

### T5.1 单元测试
- BlockSectionMap round-trip
- 单 block 数据独立解码正确性
- 跨 block 查询结果正确性
- 空结果查询（时间范围不匹配任何 block）
- 边界条件：首/尾 block 查询

### T5.2 回归测试
- 运行全部 `go test ./internal/...`
- 运行全部 E2E 测试（见 CLAUDE.md E2E 测试清单）
- 重点验证：数据完整性、查询正确性

### T5.3 性能验证
- 运行 `tests/e2e/query_1k`, `query_10k`, `query_100k`, `query_1m`
- 对比优化前后延迟和内存变化
- 验证压缩率变化 < 5%

---

## 依赖关系

```
T1.1 → T1.2 → [T2.1, T2.2, T2.3] → T2.4 → [T3.1, T3.2] → T3.3 → T3.4 → T4.1 → T5.*
```

T2.1/T2.2/T2.3 可并行；T3.1/T3.2 可并行。
