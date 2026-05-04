# SSTable 自描述架构设计

## 1. 概述

**问题**：SSTable Reader 无法正确解析字段类型，导致数据完整性验证失败。

**原因**：原来的实现假设所有字段都是 float64/int64 定长类型，无法处理 string（变长）和 bool 类型。Reader 始终返回 uint64 而不是实际类型。

**解决方案**：为 SSTable 添加自描述能力，通过 schema 文件记录字段类型，使 SSTable 独立于外部状态。

---

## 2. 架构设计

### 2.1 SSTable 文件结构

```
sstable/
├── _schema.json      # 字段类型定义（自描述）
├── _timestamps.bin   # 时间戳数据
└── fields/
    ├── field1.bin    # 字段1数据
    ├── field2.bin    # 字段2数据
    └── ...
```

### 2.2 Schema 文件格式

```json
{
  "fields": {
    "field_name": "float64" | "int64" | "string" | "bool"
  }
}
```

### 2.3 字段编码格式

| 类型 | 编码格式 | 大小 |
|------|----------|------|
| float64 | 8字节 big-endian IEEE 754 | 8 字节 |
| int64 | 8字节 big-endian 无符号 | 8 字节 |
| string | 4字节长度 + UTF-8数据 | 变长 |
| bool | 1字节 (0=false, 1=true) | 1 字节 |

---

## 3. 组件设计

### 3.1 Writer 变更

```go
type FieldType string

const (
    FieldTypeFloat64 FieldType = "float64"
    FieldTypeInt64   FieldType = "int64"
    FieldTypeString  FieldType = "string"
    FieldTypeBool    FieldType = "bool"
)

type Schema struct {
    Fields map[string]FieldType `json:"fields"`
}

type Writer struct {
    // ...
    schema Schema
}
```

**写入流程**：
1. 写入 points 时检测每个字段的类型
2. 更新 `w.schema.Fields[fieldName] = detectFieldType(val)`
3. Close 时调用 `writeSchema()` 写入 `_schema.json`

### 3.2 Reader 变更

```go
type Reader struct {
    dataDir string
    schema  Schema
}

func NewReader(dataDir string) (*Reader, error) {
    r := &Reader{dataDir: dataDir}
    if err := r.readSchema(); err != nil {
        // schema 不存在也继续，使用默认类型推断
        r.schema = Schema{Fields: make(map[string]FieldType)}
    }
    return r, nil
}
```

**读取流程**：
1. 读取 `_schema.json` 获取字段类型
2. 读取所有字段的原始 bytes
3. 预计算每个字段的偏移量表（用于变长字段）
4. 根据类型解码每个字段值

### 3.3 偏移量计算

变长字段（string）需要预计算偏移量表：

```go
func (r *Reader) computeFieldOffsets(name string, data []byte, rowCount int) []int {
    offsets := make([]int, rowCount)
    fieldType := r.schema.Fields[name]

    pos := 0
    for i := 0; i < rowCount; i++ {
        offsets[i] = pos
        pos += r.fieldSize(data[pos:], fieldType)
    }
    return offsets
}

func (r *Reader) fieldSize(data []byte, fieldType FieldType) int {
    switch fieldType {
    case FieldTypeFloat64, FieldTypeInt64:
        return 8
    case FieldTypeString:
        strLen := binary.BigEndian.Uint32(data)
        return 4 + int(strLen)
    case FieldTypeBool:
        return 1
    default:
        return 8
    }
}
```

---

## 4. 数据流

```
写入路径:
Point → Writer.WritePoints() → 检测字段类型 → 写入二进制数据 → Close() → 写入 _schema.json

读取路径:
Reader → 读取 _schema.json → 读取字段数据 → 计算偏移量表 → 按类型解码 → PointRow
```

---

## 5. 向后兼容性

- **有 schema 文件**：按照 schema 解析
- **无 schema 文件**：兼容旧数据，使用默认类型（float64），但可能解码不正确

**注意**：修复前写入的 SSTable 没有 schema 文件，这些数据的类型解析可能不正确。需要重新写入或迁移。

---

## 6. 验收标准

- [ ] Schema 文件正确写入和读取
- [ ] float64/int64/float64/string/bool 类型正确编解码
- [ ] e2e integrity 测试通过
- [ ] 变长字段（string）正确处理

---

## 7. 相关文件

| 文件 | 变更 |
|------|------|
| `internal/storage/shard/sstable/writer.go` | 添加 Schema、FieldType、writeSchema、detectFieldType |
| `internal/storage/shard/sstable/reader.go` | 重写支持 schema 解析和变长字段 |
| `docs/superpowers/specs/YYYY-MM-DD-sstable-schema-design.md` | 本文档 |
