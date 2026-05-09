# Field Schema 迁移到 BoltDB 方案

## 1. 目标

将 field schema 从 SSTable 的 `_schema.json` 文件迁移到 boltDB 元数据系统。

## 2. 当前架构

### 2.1 SSTable 结构

```
sst_0/
├── _timestamps.bin
├── _sids.bin
├── _index.bin
├── _schema.json        # Field schema (需移除)
└── fields/
    ├── field1.bin
    └── field2.bin
```

### 2.2 元数据系统结构

```
metadata.db/
├── db1/
│   ├── meas1/
│   │   ├── _schema      # Catalog schema (已有实现)
│   │   ├── series/
│   │   └── ...
```

## 3. 迁移方案

### 3.1 核心设计

- **Schema 按 measurement 存储**：同一 measurement 的所有 SSTable 共享 schema
- **Writer 只检测不存储**：Writer.Schema() 返回检测到的 schema
- **Shard.flushLocked 更新 boltDB**：刷盘时调用 SetSchema

### 3.2 接口设计

```go
// SchemaStore 接口
type SchemaStore interface {
    GetSchema(db, measurement string) (*metadata.Schema, error)
    SetSchema(db, measurement string, s *metadata.Schema) error
}
```

## 4. 详细修改

### Phase 1: Writer 修改

**文件**: `internal/storage/shard/sstable/writer.go`

- 添加 `Schema() Schema` 方法返回检测到的 schema

**文件**: `internal/storage/shard/sstable/writer_close.go`

- 移除 `writeSchema()` 调用

### Phase 2: Reader 修改

**文件**: `internal/storage/shard/sstable/reader.go`

- `NewReader(dataDir string, schema Schema)` 接收外部 schema
- 移除 `readSchema()` 和 `_schema.json` 读取

### Phase 3: Shard 修改

**文件**: `internal/storage/shard/shard.go`

- Shard 添加 schemaStore 字段（实现 SchemaStore 接口）

**文件**: `internal/storage/shard/shard_flush.go`

- flush 完成后调用 `schemaStore.SetSchema()`

**文件**: `internal/storage/shard/shard_io.go`

- `readFromSSTable` 从 `schemaStore.GetSchema()` 获取 schema

### Phase 4: Compaction 修改

**文件**: `internal/storage/compaction/merge.go`

- 移除 `_schema.json` 验证

## 5. 数据流

**写入**:
```
Write → WAL → MemTable → flushLocked
                           ↓
                      Writer.WritePoints (检测 schema)
                           ↓
                      BoltDB.SetSchema
                           ↓
                      Writer.Close (不写 _schema.json)
```

**读取**:
```
Read → BoltDB.GetSchema → Reader(schema)
```

## 6. 实施步骤

1. **Writer**: 添加 Schema() 方法，移除 writeSchema() 调用
2. **Reader**: NewReader 接收外部 schema，移除 readSchema()
3. **Shard**: 添加 schemaStore 字段
4. **Shard.flushLocked**: 调用 SetSchema 更新 boltDB
5. **Shard.readFromSSTable**: 从 boltDB 获取 schema
6. **Compaction**: 移除 _schema.json 验证
7. **测试验证**

## 7. 风险

| 风险 | 影响 | 缓解 |
|------|------|------|
| boltDB 写入失败 | schema 丢失 | 暂无 fallback |
| 读取时 schema 不存在 | 读取失败 | 写入时确保 SetSchema 成功 |