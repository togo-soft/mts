# Design: WAL 编码与压缩优化

## 架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                     优化后 WAL Write 流程                     │
├─────────────────────────────────────────────────────────────┤
│  1. 编码 payload                                           │
│  2. LZ4 压缩                                              │
│  3. 写入 record: [CRC32][Type][CSize][CompressedPayload]   │
│  4. 刷盘 (原有逻辑)                                        │
└─────────────────────────────────────────────────────────────┘
```

## 当前格式 vs 优化后格式

### 当前格式

```
┌──────────┬───────┬────────┬─────────────┬──────────┐
│ CRC32    │ Type  │ Length │  Payload    │ Padding  │
│ (4B)     │ (1B)  │ (4B)   │  (N bytes) │ (0-7B)  │
└──────────┴───────┴────────┴─────────────┴──────────┘
```

- **Header**: 9 字节固定
- **Padding**: 0-7 字节对齐
- **无压缩**: 100% 原始大小

### 优化后格式

```
┌──────────┬───────┬────────┬─────────────────────┐
│ CRC32    │ Type  │ Length │  Payload             │
│ (4B)     │ (1B)  │ (4B)  │  (N bytes)         │
└──────────┴───────┴────────┴─────────────────────┘
```

- **Header**: 9 字节固定
- **Payload**: 包含压缩标志 + 原始大小 + 数据
- **自动优化**: 小数据不压缩，大数据使用 LZ4

## 压缩库选择

| 库 | 语言 | CGO | 推荐度 |
|----|------|-----|--------|
| github.com/pierrec/lz4/v4 | Pure Go | ❌ | ⭐⭐⭐⭐⭐ |

**确认**: pierrec/lz4 是纯 Go 实现，无 CGO 依赖。

## 压缩算法参数

```go
import "github.com/pierrec/lz4/v4"

compressionLevel := lz4.Fast   // 速度优先
```

| 级别 | 压缩速度 | 压缩率 | 推荐场景 |
|-----|---------|--------|---------|
| Fast | ~500MB/s | 40% | **生产推荐** |
| High | ~100MB/s | 50% | 存储优先 |

## 详细设计

### 1. 新增文件: wal/compress.go

```go
// CompressPayload 使用 LZ4 压缩 payload。
// 格式: [flag:1B][size:4B][data]
// flag=0 表示未压缩，flag=1 表示压缩
func CompressPayload(payload []byte) ([]byte, error) {
    if len(payload) == 0 {
        return nil, nil
    }
    // 预分配空间：1字节flag + 4字节原始大小 + 数据
    dst := make([]byte, 5+len(payload)*2)

    n, err := lz4.CompressBlock(payload, dst[5:], nil)
    if err != nil {
        return nil, err
    }

    if n > 0 && n < len(payload) {
        // 压缩有效
        dst[0] = 1
        binary.BigEndian.PutUint32(dst[1:5], uint32(len(payload)))
        return dst[:5+n], nil
    }

    // 压缩无效，存储原始数据
    result := make([]byte, 5+len(payload))
    result[0] = 0
    binary.BigEndian.PutUint32(dst[1:5], uint32(len(payload)))
    copy(result[5:], payload)
    return result, nil
}

// DecompressPayload 解压 payload。
func DecompressPayload(src []byte) ([]byte, error) {
    // 根据 flag 判断是否解压
    ...
}
```
```

### 2. 修改 wal/format.go

```go
// Segment Header Flags
const (
    FlagNone       uint16 = 0x0000  // 无压缩
    FlagCompressed uint16 = 0x0001  // LZ4 压缩
)

// EncodeRecord 修改为支持压缩
func EncodeRecord(dst []byte, typ byte, payload []byte, compressed bool) []byte {
    if compressed {
        // 压缩编码
        compPayload := CompressPayload(payload)
        return encodeCompressedRecord(dst, typ, compPayload)
    }
    // 原始编码
    return encodeRawRecord(dst, typ, payload)
}
```

### 3. 修改 wal/wal.go

```go
func (w *WAL) Write(data []byte) (int, error) {
    // 1. 压缩
    compressed, err := CompressPayload(data)
    if err != nil {
        return 0, err
    }

    // 2. 编码为 record
    record := EncodeRecord(recordBuf, TypePointData, compressed, true)

    // 3. 写入（原有逻辑）
    ...
}

func (w *WAL) Replay(fn func(payload []byte) error) error {
    entries, err := listSegments(w.dir)
    ...

    for _, e := range entries {
        file, err := os.Open(e.Path)
        ...

        // 读取 header 获取 flags
        header := make([]byte, segmentHeaderSize)
        file.Read(header)
        flags := decodeSegmentFlags(header)

        compressed := flags&FlagCompressed != 0

        _, err = readRecords(file, int64(segmentHeaderSize), fn, compressed)
        ...
    }
}
```

### 4. 修改 wal/reader.go

```go
func readRecords(file *os.File, offset int64, fn func([]byte) error, compressed bool) (int, error) {
    ...

    for {
        // 读取 record header
        header := make([]byte, recordHeaderSize)
        n, err := file.Read(header)
        ...

        typ := header[4]
        length := binary.BigEndian.Uint32(header[5:9])

        // 读取 payload
        payload := make([]byte, length)
        file.Read(payload)

        // 如果压缩则解压
        if compressed {
            payload, err = DecompressPayload(payload, originalSize)
            ...
        }

        // 验证 CRC
        crc := binary.BigEndian.Uint32(header[0:4])
        calculatedCRC := crc32Sum(append([]byte{typ}, payload...))
        if crc != calculatedCRC {
            return total, ErrInvalidCRC
        }

        ...
    }
}
```

## 性能分析

### 压缩性能

| 级别 | 压缩速度 | 解压速度 | 压缩率 |
|-----|---------|---------|--------|
| Fast | ~500MB/s | ~3GB/s | 40% |
| High | ~100MB/s | ~3GB/s | 50% |

### 空间收益估算

假设每条记录原始 payload = 100 bytes

| 方案 | 每记录大小 | 节省 |
|-----|-----------|------|
| 当前 | 109-116B | 0% |
| +LZ4 | 60-70B | 40% |

### 写入延迟影响

LZ4 Fast 模式压缩 100 字节数据约 0.2 微秒，对写入延迟影响 < 1%。

## 风险与注意事项

### 数据安全

- CRC32 校验确保数据完整性
- 解压失败时返回错误，WAL 不会丢失数据

### 性能风险

- 压缩是 CPU 操作，高并发时需监控 CPU 使用率
- 建议在 8 核以上机器上启用压缩

## 实现顺序

1. 添加 LZ4 依赖
2. 实现 compress.go
3. 修改 format.go 添加 Flags
4. 修改 reader.go 支持解压
5. 修改 wal.go Write 使用压缩
6. 添加测试验证
7. 性能基准测试
