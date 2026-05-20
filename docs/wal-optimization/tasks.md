# Tasks: WAL 编码与压缩优化

## Task 列表

### T1: 依赖与基础设施

- [ ] **T1.1**: 添加 github.com/pierrec/lz4/v4 依赖
  - 验收标准：go.mod 中新增依赖，无 CGO 引用

- [ ] **T1.2**: 在 wal 包中创建 compress.go 实现压缩/解压函数
  - 验收标准：CompressPayload / DecompressPayload 函数

---

### T2: 格式变更

- [ ] **T2.1**: 更新 Segment Header Flags 定义
  - 验收标准：新增 FlagCompressed 标志位

- [ ] **T2.2**: 修改 EncodeRecord 支持压缩格式
  - 验收标准：EncodeRecord 接受压缩参数

- [ ] **T2.3**: 修改 readRecords 支持解压读取
  - 验收标准：根据 Flags 自动判断是否解压

---

### T3: 写入路径

- [ ] **T3.1**: 修改 WAL.Write 使用压缩
  - 验收标准：写入数据自动压缩

- [ ] **T3.2**: 修改 WAL.WriteBatch 使用压缩
  - 验收标准：批量写入自动压缩

---

### T4: 测试验证

- [ ] **T4.1**: 添加 CompressPayload / DecompressPayload 单元测试
  - 验收标准：压缩后能正确解压，数据一致

- [ ] **T4.2**: 添加 WAL 写入读取集成测试
  - 验收标准：写入 10000 条数据，Replay 完全一致

---

### T5: 性能基准测试

- [ ] **T5.1**: 基准测试：压缩/解压吞吐量
  - 验收标准：压缩速度 > 200MB/s

- [ ] **T5.2**: 基准测试：压缩率
  - 验收标准：压缩率 > 30%

---

### T6: Lint 与格式化

- [ ] **T6.1**: 运行 golangci-lint
  - 验收标准：无警告

- [ ] **T6.2**: 运行 goimports-reviser
  - 验收标准：符合项目格式规范

---

## 依赖关系

```
T1.1 → T1.2 → T2.1 → T2.2 → T2.3
                                   ↓
                                 T3.1 → T3.2
                                         ↓
                                       T4.1 → T4.2
                                               ↓
                                             T5.1 → T5.2
                                               ↓
                                             T6.1 → T6.2
```

## 验收标准

1. 压缩率 > 30%（典型时序数据）
2. 压缩速度 > 200MB/s
3. 100% 数据正确性：写入读取完全一致
4. Lint 通过：golangci-lint 无警告
