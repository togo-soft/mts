# Requirements: 数据类型压缩编码

## 问题

当前 SSTable 对所有数据类型均使用定长原始编码：
- 时间戳 (int64): 固定 8 字节 BigEndian
- SID (uint64): 固定 8 字节 BigEndian
- int64 字段: 固定 8 字节 BigEndian
- float64 字段: 固定 8 字节 IEEE 754 BigEndian
- string 字段: 4 字节长度前缀 + 原始数据
- bool 字段: 1 字节 (0/1)

`internal/storage/shard/compression/` 下已有 Delta 编码和 Varint 编码工具，但未接入实际读写路径。不同类型数据具有不同的分布特征，使用统一原始编码浪费大量存储空间。

## 目标

The system SHALL apply type-appropriate compression encoding to each data column within SSTable files, reducing storage footprint.

## 功能需求 (EARS)

### FR1: 时间戳 Delta-of-Delta + Varint 编码
WHEN SSTable 的时间戳段被写入，the system SHALL 对 int64 时间戳序列使用 Delta-of-Delta 编码后再 Varint 编码。对于等间隔时间序列，每个时间戳仅需 1 字节。

### FR2: SID Varint 编码
WHEN SSTable 的 SID 段被写入，the system SHALL 对 uint64 SID 使用 Varint 编码。

### FR3: int64 字段 ZigZag + Varint 编码
WHEN int64 类型字段段被写入，the system SHALL 使用 ZigZag 编码将有符号整数映射为无符号整数后，再使用 Varint 编码。

### FR4: float64 字段 XOR 编码
WHEN float64 类型字段段被写入，the system SHALL 使用 Gorilla 风格的 XOR 编码：首个值存储原始 8 字节，后续值存储与前值的 XOR 差异（仅保留前后导零之间的有效位）。

### FR5: string 字段字典编码
WHEN string 类型字段段被写入，the system SHALL 使用字典编码。若字典编码结果大于原始数据，自动回退为原始编码。

### FR6: bool 字段位图编码
WHEN bool 类型字段段被写入，the system SHALL 使用位图编码，每 8 个 bool 值压缩为 1 字节。

### FR7: 编码元数据记录
The system SHALL 在 SectionEntry 中记录每个段的编码类型，确保 Reader 能正确选择解码器。

### FR8: 编码工具补全
The system SHALL 在 `internal/storage/shard/compression/` 包中补充 ZigZag、XOR Float、Bitmap、BitWriter/BitReader 等编码工具，并复用已有的 Delta 和 Varint 工具。

### FR9: 段级独立编码
WHEN 一个 SSTable 包含多个不同类型的字段，the system SHALL 对每个字段段独立选择编码方式，互不影响。
