# Tasks: SID Delta 编码优化

## Task 列表

### T1: 编码函数实现

- [x] **T1.1**: 在 `compression/encode.go` 添加 `EncodeSidsDelta`
  - 验收标准：递增 SID 序列编码后大小减少 50% 以上

- [x] **T1.2**: 在 `compression/encode.go` 添加 `DecodeSidsDelta`
  - 验收标准：解码结果与编码前完全一致

### T2: 写入路径修改

- [x] **T2.1**: 修改 `sstable/writer_close.go` 的 `encodeSidsSection`
  - 验收标准：使用 `EncodeSidsDelta` 替代 `EncodeSids`

### T3: 读取路径修改

- [x] **T3.1**: 修改 `sstable/reader_blocks.go` 的 `readSids`
  - 验收标准：使用 `DecodeSidsDelta`

- [x] **T3.2**: 修改 `sstable/reader_blocks.go` 的 `readSidsBlock`
  - 验收标准：使用 `DecodeSidsDelta`

### T4: 测试验证

- [x] **T4.1**: 添加 `EncodeSidsDelta` / `DecodeSidsDelta` 单元测试
  - 验收标准：100% 路径覆盖

- [x] **T4.2**: 集成测试：SSTable 写入后读取验证
  - 验收标准：写入 10000 条数据，读取完全一致

### T5: Lint 与格式化

- [x] **T5.1**: 运行 golangci-lint
  - 验收标准：golangci-lint 无警告

- [x] **T5.2**: 运行 goimports-reviser
  - 验收标准：符合项目格式规范

## 依赖关系

```
T1.1 → T1.2 → T2.1
                ↓
              T3.1 → T3.2
                         ↓
                       T4.1 → T4.2
                                ↓
                              T5.1 → T5.2
```

## 验收标准

1. SID 递增序列：编码大小减少 ≥ 50%
2. 100% 数据正确性：写入读取完全一致
3. 代码覆盖率：新增函数 ≥ 90%
4. Lint 通过：golangci-lint 无警告

## 实现结果

### 压缩效果

```
1000 递增 SID: Delta=1002 bytes, Varint=3000 bytes, 节省=66.6%
```

### 修改文件

| 文件 | 修改内容 |
|-----|---------|
| `compression/encode.go` | 新增 `EncodeSidsDelta`、`DecodeSidsDelta` |
| `compression/encode_test.go` | 新增 3 个测试用例 |
| `sstable/writer_close.go` | `encodeSidsSection` 使用 Delta 编码 |
| `sstable/reader_blocks.go` | `readSids`、`readSidsBlock` 使用 Delta 解码 |
