# WAL 模块重构设计

## 目标

将 WAL 从 `internal/storage/shard/` 中独立为 `internal/storage/wal/` 包，解决现有设计中的关键数据安全问题和架构缺陷，参考 Prometheus TSDB WAL 实现。

## 动机

### 审计发现的关键问题

| 问题 | 严重度 |
|------|--------|
| 缺少 CRC 校验 | Critical |
| 大记录非原子写入 | Critical |
| 序列号管理错误（重启后 seq=0 覆盖旧文件） | Critical |
| 序列化格式无版本号、用 \0 分隔符 | High |
| Replay 全量加载到内存 | High |
| 双重 goroutine | High |
| fileSize 手动追踪 | Medium |
| WAL 目录未 fsync | Medium |

详见审计报告。

## 架构

### 包结构

```
internal/storage/wal/
├── wal.go            # 核心类型与公开 API
├── segment.go        # Segment 文件管理
├── format.go         # 记录格式编解码、CRC32
├── reader.go         # 流式重放
├── checkpoint.go     # 检查点持久化
├── cleanup.go        # 旧 Segment 清理
├── wal_test.go
├── wal_bench_test.go
├── format_test.go
└── segment_test.go
```

### 依赖边界

- **不依赖** `shard`、`types.Point`、`measurement`
- **仅依赖** 标准库 + `internal/storage`（安全文件操作）+ `log/slog`
- WAL 只操作 `[]byte`，序列化仍由 shard 层负责

### 职责分层

```
DB.Open()
├─ 从元数据查询活跃 Shard 列表及目录
├─ 对每个 Shard:
│   ├─ wal.Open(dir)         ← 打开 WAL
│   ├─ wal.Replay(fn)        ← 流式回放未 flush 数据
│   ├─ 恢复 SID 映射
│   └─ 重建 MemTable
└─ 所有 Shard 就绪，接受读写
```

WAL 本身不决定何时回放 — 那是上层职责。

## 文件格式

### Segment 文件布局

```
┌─────────────────────────────────────────────────────────────┐
│ Header (14 bytes)                                           │
│  Magic:    4B  0xD0C0A1FE  (big-endian)                    │
│  Version:  2B  0x0001                                       │
│  Flags:    2B  reserved                                     │
│  Segment:  4B  segment number (big-endian)                  │
│  Reserved: 2B                                               │
├─────────────────────────────────────────────────────────────┤
│ Record                                                      │
│  CRC32:    4B  CRC32 of (type + len + payload)             │
│  Type:     1B  0x01=PointData, 0x02=Meta, 0xFF=Pad         │
│  Length:   4B  payload length (big-endian, uint32)          │
│  Payload:  N bytes                                          │
│  Padding:  0-7 bytes (8 字节对齐)                            │
├─────────────────────────────────────────────────────────────┤
│ Record ...                                                  │
└─────────────────────────────────────────────────────────────┘
```

### 文件命名

```
<generation>_<segment>.wal

generation:  16位 hex，wal.Open 时的 Unix 秒
segment:     8位 hex，该世代内的 segment 序号
```

示例：`695b8f00_00000001.wal`

- 世代隔离：重启 = 新 generation，旧文件不会被覆盖
- 自然排序：按文件名排序即为正确重放顺序

## API

### 类型

```go
type Config struct {
    Dir         string
    SegmentSize int64    // 默认 64MB
    MaxSegments int      // 0=无限制
    SyncMode    SyncMode
    Logger      *slog.Logger
}

type SyncMode int
const (
    SyncNone     SyncMode = iota  // 不主动 fsync
    SyncPeriodic                  // 定时 fsync（默认，1s）
    SyncEvery                     // 每次写入 fsync
)

type WAL struct { /* 私有字段 */ }
```

### 公开方法

| 方法 | 说明 |
|------|------|
| `Open(cfg Config) (*WAL, error)` | 打开/创建 WAL，自动发现最大 segment 号 |
| `Write(data []byte) (int, error)` | 写入一条记录（原子：CRC+type+len+data 一次 Write） |
| `WriteBatch(data [][]byte) (int, error)` | 批量写入，锁获取一次 |
| `Sync() error` | 强制 fsync |
| `Close() error` | 关闭 segment，停止后台任务 |
| `Replay(fn func([]byte) error) error` | 流式回放，回调模式，内存恒定 |
| `TruncateCurrent() error` | 截断当前 segment（flush 后），清理旧 segment |
| `Cleanup(beforeGen uint64) error` | 删除指定世代之前的 segment |

### Write 执行路径

```
Write(data)
  → encodeRecord(data) → CRC + type + len + data + padding
  → 检查轮转条件
  → flushBuffer
  → file.Write(record)  // 单次系统调用
```

## 流式重放

```go
func (w *WAL) Replay(fn func([]byte) error) error
```

- 按文件名排序遍历所有 segment
- 基于 checkpoint 增量重放（跳过已完成的 segment/偏移）
- CRC32 校验每条记录，损坏记录跳过并告警
- 定时保存 checkpoint
- 回调模式，不积累 points，内存占用常数

### 损坏恢复

1. 记录告警（path, offset, expected CRC, actual CRC）
2. 逐字节扫描寻找下一个合法记录头
3. 无法恢复则跳过剩余 segment
4. 整个 segment 不可读则跳过

## Checkpoint

```go
type Checkpoint struct {
    Generation uint64 `json:"gen"`
    Segment    uint64 `json:"seg"`
    Position   int64  `json:"pos"`
}
```

- 原子写入：先写 `.tmp`，再 `os.Rename`
- 保存间隔：每 1000 条记录或每 5 秒

## Shard 集成

### NewShard 变更

```go
wal, _ := wal.Open(wal.Config{
    Dir:         walDir,
    SegmentSize: 64 << 20,
    SyncMode:    wal.SyncPeriodic,
    Logger:      logger,
})
```

- 移除 `walDone` channel：WAL 内部管理 sync goroutine
- `StartPeriodicSync` 移除：WAL 根据 SyncMode 自动管理

### Write 变更

不变 — `s.wal.Write(data)` 签名一致

### Replay 变更

```go
wal.Replay(func(data []byte) error {
    point, _ := deserializePoint(data)
    sid, _ := shard.seriesStore.AllocateSID(point.Tags)
    shard.sidCache[sid] = point.Tags
    shard.tsSidMap[point.Timestamp] = sid
    return shard.memTable.Write(point)
})
```

### flushLocked 变更

`TruncateCurrent` 后自动清理该 segment 的 WAL 文件

## 序列化格式（shard 层）

```
Point Record:
  Version:   1B
  Flags:     1B
  Timestamp: 8B  int64 big-endian
  TagCount:  2B  uint16
  [Tag: KeyLen(2B) + Key + ValLen(2B) + Value]...
  FieldCount: 2B  uint16
  [Field: KeyLen(2B) + Key + Type(1B) + Value]...
    float64: 8B IEEE754
    int64:   8B
    string:  2B len + N bytes
    bool:    1B
```

- 移除 `\0` 分隔符，改用 length-prefixed
- 支持 key/value 中的 null 字节
- 新增 1 字节版本号

## GC 策略

1. **Segment 级**：MemTable flush 后，`TruncateCurrent` 截断 + 删除旧 segment 文件
2. **Generation 级**：重启后旧 generation 的 segment 在回放后由 Cleanup 清理
3. **大小限制**：`MaxSegments` 配置项，超出时拒绝写入并告警

## 测试策略

- 所有公开 API 单元测试（覆盖率 ≥ 90%）
- CRC 损坏恢复测试
- Segment 轮转边界测试
- 并发写入压力测试
- 崩溃恢复模拟测试
- E2E 测试：`tests/e2e/wal_test/` 用例全部通过

## 兼容性

- WAL 格式不向后兼容旧格式
- 新版本首次启动时旧 WAL 文件被忽略（旧数据在 SSTable 中）
- 升级建议：先正常关闭旧版本（触发 flush），再升级
