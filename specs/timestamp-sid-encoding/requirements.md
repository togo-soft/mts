# Requirements: SID Delta 编码优化

## 问题

当前 SID 使用直接 Varint 编码，当 SID 是递增序列时（如 1000000, 1000001, 1000002...），每个 SID 都需要 3-4 字节存储。

MTS 的 SID 基于 measurement 级别，按时间线顺序递增分配，不同 tag 对应不同 sid。这意味着在大多数查询场景下，SID 序列是递增的，直接 Varint 编码浪费空间。

## 目标

实现 SID First-Delta 编码，减少存储空间。

## 功能需求 (EARS)

### FR1: SID First-Delta 编码
WHEN SSTable 的 SID 段被写入，the system SHALL 使用 First-Delta 编码：第一个 SID 存储原始值（Varint），后续 SID 存储与前一个 SID 的差值（Varint）。

### FR2: 自动优化
The system SHALL 根据数据特性自动选择最优编码，无需手动配置。
