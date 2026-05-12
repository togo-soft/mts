# Requirements: WAL 编码与压缩优化

## 问题

当前 WAL (Write-Ahead Log) 使用原始格式存储数据：
- 每条记录固定 9 字节 header (CRC32 + Type + Length)
- Payload 原始存储，无压缩
- 8 字节对齐 padding

这导致 WAL 占用空间是实际数据的 1.5-2 倍。

## 目标

优化 WAL 的编码与压缩，在保证写入性能和可靠性的前提下，减少存储空间占用。

## 功能需求 (EARS)

### FR1: LZ4 压缩支持
WHEN WAL 记录被写入，the system SHALL 使用 LZ4 算法压缩 payload，支持解压回放。

### FR2: 压缩标志位
The system SHALL 在 Segment Header 中使用 Flags 字段指示压缩状态。

### FR3: CRC32 校验
The system SHALL 在解压后验证 CRC32，确保数据完整性。

### FR4: 纯 Go 实现
压缩库必须为纯 Go 实现，不依赖 CGO。

### FR5: 写入性能
压缩开销必须小于 5% 的写入延迟。
