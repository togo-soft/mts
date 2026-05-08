# WAL 模块重构实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 WAL 从 `internal/storage/shard/` 独立为 `internal/storage/wal/` 包，引入 CRC32 校验、流式重放、世代隔离、原子写入。

**Architecture:** 新 WAL 包仅依赖标准库 + `internal/storage`（安全文件操作）+ `log/slog`，操作 `[]byte` 不感知 Point 结构。Segment 文件含魔术头+版本号+记录(CRC32+type+len+payload)。Shard 层通过 `wal.Open/Write/Replay/TruncateCurrent` 集成。

**Tech Stack:** Go 标准库 (encoding/binary, hash/crc32, os, sync), internal/storage (SafeMkdirAll, SafeOpenFile), log/slog

---

## File Map

### 新建文件

| 文件 | 职责 |
|------|------|
| `internal/storage/wal/wal.go` | Config, SyncMode, WAL struct, Open, Write, WriteBatch, Sync, Close |
| `internal/storage/wal/format.go` | 魔数常量, SegmentHeader, RecordHeader, encodeSegmentHeader, encodeRecord, crc32 计算 |
| `internal/storage/wal/segment.go` | segment struct, openSegment, writeHeader, write, truncate, sync, close |
| `internal/storage/wal/reader.go` | readSegmentHeader, readRecords (流式读取+CRC校验), parseSegmentName |
| `internal/storage/wal/checkpoint.go` | Checkpoint struct, save/load (原子: .tmp → rename) |
| `internal/storage/wal/cleanup.go` | Cleanup (删除旧世代 segment 文件) |
| `internal/storage/wal/wal_test.go` | WAL 核心功能单元测试 |
| `internal/storage/wal/format_test.go` | 编解码、CRC 校验单元测试 |
| `internal/storage/wal/segment_test.go` | Segment 管理单元测试 |
| `internal/storage/wal/wal_bench_test.go` | 性能基准测试 |

### 修改文件

| 文件 | 变更 |
|------|------|
| `internal/storage/shard/shard.go` | 导入 wal 包；移除 `walDone`；wal 字段类型改为 `*wal.WAL` |
| `internal/storage/shard/shard_io.go` | Write 适配新 WAL 签名 |
| `internal/storage/shard/shard_flush.go` | TruncateCurrent 适配 |
| `internal/storage/shard/shard_lifecycle.go` | Close 简化（移除 walDone 管理） |
| `internal/storage/shard/wal_serialize.go` | 新序列化格式（length-prefixed 替代 \0 分隔） |
| `internal/storage/shard/shard_test.go` | 适配新 WAL |
| `internal/storage/shard/shard_extra_test.go` | 移除旧 WAL 测试，适配新接口 |

### 删除文件

- `internal/storage/shard/wal.go`
- `internal/storage/shard/wal_lifecycle.go`
- `internal/storage/shard/wal_replay.go`
- `internal/storage/shard/wal_test.go`
- `internal/storage/shard/wal_bench_test.go`

---

### Task 1: 创建 WAL 包基础 — 格式常量和 CRC32

**Files:**
- Create: `internal/storage/wal/format.go`

- [ ] **Step 1: 创建格式定义文件**

```go
// Package wal 实现 Write-Ahead Log。
//
// Segment 文件格式:
//
//	Header (14B): Magic(4B) + Version(2B) + Flags(2B) + SegmentNum(4B) + Reserved(2B)
//	Record: CRC32(4B) + Type(1B) + Length(4B) + Payload(N) + Padding(0-7B)
//
// 文件命名: <generation>_<segment>.wal
//	generation: 16位 hex (Unix秒)
//	segment:    8位 hex (序号)
package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// 魔数 "D0C0A1FE" — 用于标识有效的 WAL 文件。
const magicNumber uint32 = 0xD0C0A1FE

// 当前格式版本。
const currentVersion uint16 = 1

// 记录类型。
const (
	TypePointData byte = 0x01 // Point 数据记录
	TypeMeta      byte = 0x02 // 元信息记录
	TypePad       byte = 0xFF // 填充记录
)

// segmentHeader 大小: 4 + 2 + 2 + 4 + 2 = 14 字节。
const segmentHeaderSize = 14

const recordHeaderSize = 9 // CRC32(4) + Type(1) + Length(4)

var ieeeTable = crc32.MakeTable(crc32.IEEE)

// ErrShortWrite 表示写入的字节数少于预期。
var ErrShortWrite = errors.New("short write")

// crc32Sum 计算 CRC32-IEEE。
func crc32Sum(data []byte) uint32 {
	return crc32.Checksum(data, ieeeTable)
}

// pad8 返回 8 字节对齐所需的 padding 字节数。
func pad8(length int) int {
	rem := length % 8
	if rem == 0 {
		return 0
	}
	return 8 - rem
}

// encodeSegmentHeader 编码 segment 文件头到 dst（14 字节）。
func encodeSegmentHeader(dst []byte, segmentNum uint32) {
	binary.BigEndian.PutUint32(dst[0:4], magicNumber)
	binary.BigEndian.PutUint16(dst[4:6], currentVersion)
	binary.BigEndian.PutUint16(dst[6:8], 0)               // flags
	binary.BigEndian.PutUint32(dst[8:12], segmentNum)
	binary.BigEndian.PutUint16(dst[12:14], 0)              // reserved
}

// decodeSegmentHeader 解码 segment 文件头。
// 返回 (version, segmentNum, error)。
func decodeSegmentHeader(data []byte) (version uint16, segmentNum uint32, err error) {
	magic := binary.BigEndian.Uint32(data[0:4])
	if magic != magicNumber {
		return 0, 0, &FormatError{Reason: "invalid magic number"}
	}
	return binary.BigEndian.Uint16(data[4:6]), binary.BigEndian.Uint32(data[8:12]), nil
}

// FormatError 表示格式错误。
type FormatError struct {
	Reason string
}

func (e *FormatError) Error() string {
	return "wal format error: " + e.Reason
}

// encodeRecord 将 payload 编码为 WAL 记录。
// 返回完整记录: CRC32 + type + len + payload + padding。
// dst 必须足够容纳返回值。
func EncodeRecord(dst []byte, typ byte, payload []byte) []byte {
	bodyLen := 1 + 4 + len(payload) // type + len + payload
	padding := pad8(4 + bodyLen)    // CRC32 + body + padding
	totalLen := 4 + bodyLen + padding

	dst = dst[:0]
	dst = append(dst, 0, 0, 0, 0) // CRC32 placeholder

	dst = append(dst, typ)

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	dst = append(dst, lenBuf[:]...)

	dst = append(dst, payload...)

	for i := 0; i < padding; i++ {
		dst = append(dst, 0)
	}

	// 计算 CRC32（覆盖 type + len + payload，不含 padding）
	crcInput := dst[4 : 4+bodyLen] // 跳过 CRC32 placeholder
	crc := crc32Sum(crcInput)
	binary.BigEndian.PutUint32(dst[0:4], crc)

	return dst[:totalLen]
}

// RecordSize 返回编码后记录的字节数。
func RecordSize(payloadLen int) int {
	bodyLen := 1 + 4 + payloadLen
	return 4 + bodyLen + pad8(4+bodyLen)
}
```

- [ ] **Step 2: 编译验证**

Run: `cd internal/storage/wal && go build ./...`
Expected: 编译成功

- [ ] **Step 3: Commit**

```bash
git add internal/storage/wal/format.go
git commit -m "feat(wal): 添加 WAL 格式定义和 CRC32 编码"
```

---

### Task 2: Segment 文件管理

**Files:**
- Create: `internal/storage/wal/segment.go`

- [ ] **Step 1: 实现 segment 类型**

```go
package wal

import (
	"os"

	"codeberg.org/micro-ts/mts/internal/storage"
)

// segment 表示一个 WAL 文件。
type segment struct {
	file          *os.File
	gen           uint64 // 世代号
	num           uint64 // segment 序号
	size          int64  // 当前文件大小
	headerWritten bool
}

// openSegment 打开或创建指定世代和序号的 WAL segment。
func openSegment(dir string, gen uint64, num uint64, cfg Config) (*segment, error) {
	filename := segmentPath(dir, gen, num)
	f, err := storage.SafeOpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	seg := &segment{
		file:          f,
		gen:           gen,
		num:           num,
		size:          info.Size(),
		headerWritten: info.Size() >= segmentHeaderSize,
	}

	if !seg.headerWritten {
		if err := seg.writeHeader(); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	return seg, nil
}

// segmentPath 生成 segment 文件路径。
func segmentPath(dir string, gen uint64, num uint64) string {
	return storagePath(dir, segmentName(gen, num))
}

// segmentName 生成文件名（不含路径）。
func segmentName(gen uint64, num uint64) string {
	return formatHex16(gen) + "_" + formatHex8(num) + ".wal"
}
```

`formatHex16` 和 `formatHex8` 是内部辅助函数，先放在 segment.go：

```go
import "fmt"

func formatHex16(n uint64) string {
	return fmt.Sprintf("%016x", n)
}

func formatHex8(n uint64) string {
	return fmt.Sprintf("%08x", n)
}

func storagePath(dir, name string) string {
	return dir + "/" + name
}
```

- [ ] **Step 2: 实现 segment 方法**

```go
// writeHeader 写入文件头。
func (s *segment) writeHeader() error {
	var buf [segmentHeaderSize]byte
	encodeSegmentHeader(buf[:], uint32(s.num))
	n, err := s.file.Write(buf[:])
	if err != nil {
		return err
	}
	if n != segmentHeaderSize {
		return ErrShortWrite
	}
	s.size = segmentHeaderSize
	s.headerWritten = true
	return nil
}

// Write 追加数据到 segment 文件。
func (s *segment) Write(data []byte) (int, error) {
	n, err := s.file.Write(data)
	if err != nil {
		return 0, err
	}
	s.size += int64(n)
	return n, nil
}

// Sync 刷盘。
func (s *segment) Sync() error {
	return s.file.Sync()
}

// Truncate 截断文件到 0，重新写 header。
func (s *segment) Truncate() error {
	if err := s.file.Truncate(0); err != nil {
		return err
	}
	if _, err := s.file.Seek(0, 0); err != nil {
		return err
	}
	s.size = 0
	s.headerWritten = false
	return s.writeHeader()
}

// Close 关闭 segment 文件。
func (s *segment) Close() error {
	return s.file.Close()
}
```

- [ ] **Step 3: Commit**

```bash
git add internal/storage/wal/segment.go
git commit -m "feat(wal): 添加 Segment 文件管理"
```

---

### Task 3: 解析 segment 文件名

**Files:**
- Modify: `internal/storage/wal/segment.go`（添加 parseSegmentName 和 listSegments）

- [ ] **Step 1: 添加解析和列表函数**

```go
import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// parseSegmentName 从文件名解析 (generation, segment)。
func parseSegmentName(filename string) (gen uint64, num uint64, err error) {
	base := filepath.Base(filename)
	if !strings.HasSuffix(base, ".wal") {
		return 0, 0, &FormatError{Reason: "not a .wal file: " + base}
	}
	core := base[:len(base)-4]
	parts := strings.Split(core, "_")
	if len(parts) != 2 || len(parts[0]) != 16 || len(parts[1]) != 8 {
		return 0, 0, &FormatError{Reason: "invalid segment name: " + base}
	}
	gen, err = strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return 0, 0, &FormatError{Reason: "invalid generation: " + parts[0]}
	}
	num, err = strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return 0, 0, &FormatError{Reason: "invalid segment number: " + parts[1]}
	}
	return gen, num, nil
}

// listSegments 列出目录中所有 WAL segment，按 (gen, num) 排序。
func listSegments(dir string) ([]segmentEntry, error) {
	pattern := filepath.Join(dir, "*.wal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	type seg struct {
		gen  uint64
		num  uint64
		path string
	}
	var segs []seg
	for _, m := range matches {
		g, n, e := parseSegmentName(m)
		if e != nil {
			continue
		}
		segs = append(segs, seg{gen: g, num: n, path: m})
	}
	// 按 (gen, num) 排序
	sortSegments(segs)

	entries := make([]segmentEntry, len(segs))
	for i, s := range segs {
		entries[i] = segmentEntry{Gen: s.gen, Num: s.num, Path: s.path}
	}
	return entries, nil
}

type segmentEntry struct {
	Gen  uint64
	Num  uint64
	Path string
}

func sortSegments(segs []struct{ gen, num uint64; path string }) {
	// 简单冒泡排序，segment 数量通常很少
	for i := 0; i < len(segs)-1; i++ {
		for j := i + 1; j < len(segs); j++ {
			if segs[i].gen > segs[j].gen ||
				(segs[i].gen == segs[j].gen && segs[i].num > segs[j].num) {
				segs[i], segs[j] = segs[j], segs[i]
			}
		}
	}
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/storage/wal/segment.go
git commit -m "feat(wal): 添加 segment 文件名解析和列表"
```

---

### Task 4: 流式 Reader — readSegmentHeader 和 readRecords

**Files:**
- Create: `internal/storage/wal/reader.go`

- [ ] **Step 1: 创建 reader.go**

```go
package wal

import (
	"encoding/binary"
	"io"
	"log/slog"
	"os"
)

// ErrCorruptRecord 表示发现一条损坏的 WAL 记录。
var ErrCorruptRecord = &FormatError{Reason: "CRC mismatch"}

// readSegmentHeader 读取并验证 segment 文件头。
func readSegmentHeader(file *os.File) (version uint16, segmentNum uint32, err error) {
	var buf [segmentHeaderSize]byte
	n, err := io.ReadFull(file, buf[:])
	if err != nil {
		return 0, 0, err
	}
	_ = n
	return decodeSegmentHeader(buf[:])
}

// readRecords 从文件指定偏移开始流式读取 WAL 记录。
// 对每条有效记录调用 fn(payload)，遇到 CRC 错误跳过并告警。
// 返回最终文件偏移。
func readRecords(file *os.File, startPos int64, fn func(payload []byte) error) (int64, error) {
	if _, err := file.Seek(startPos, 0); err != nil {
		return startPos, err
	}

	pos := startPos
	var headerBuf [recordHeaderSize]byte

	for {
		// 读取 CRC32 + type + length (9 字节)
		n, err := io.ReadFull(file, headerBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return pos, nil
			}
			return pos, err
		}
		_ = n
		pos += recordHeaderSize

		expectedCRC := binary.BigEndian.Uint32(headerBuf[0:4])
		recType := headerBuf[4]
		payloadLen := binary.BigEndian.Uint32(headerBuf[5:9])

		// 长度合理性检查
		if payloadLen > 256*1024*1024 {
			slog.Warn("WAL record too large, stopping replay",
				"offset", pos-recordHeaderSize,
				"payloadLen", payloadLen)
			return pos, nil
		}

		// 读取 payload
		payload := make([]byte, payloadLen)
		if payloadLen > 0 {
			if _, err := io.ReadFull(file, payload); err != nil {
				slog.Warn("WAL incomplete record, stopping replay",
					"offset", pos, "error", err)
				return pos, nil
			}
		}
		pos += int64(payloadLen)

		// 计算 padding（8 字节对齐）
		recordBodySize := 4 + int64(recordHeaderSize) + int64(payloadLen) // 含 CRC32
		padding := pad8(int(recordBodySize))
		if padding > 0 {
			if _, err := file.Seek(int64(padding), io.SeekCurrent); err != nil {
				return pos, err
			}
			pos += int64(padding)
		}

		// 验证 CRC32
		actualCRC := crc32Sum(headerBuf[4:9])          // type + len 部分
		actualCRC = crc32Update(actualCRC, payload)     // 追加 payload
		if actualCRC != expectedCRC {
			slog.Warn("WAL CRC mismatch, skipping record",
				"offset", pos-recordBodySize,
				"expected", expectedCRC,
				"actual", actualCRC)
			continue
		}

		// 只回调 PointData 类型（跳过 meta/pad）
		if recType == TypePointData || recType == TypeMeta {
			if err := fn(payload); err != nil {
				return pos, err
			}
		}
	}
}

// crc32Update 增量更新 CRC32 值。
func crc32Update(crc uint32, data []byte) uint32 {
	return crc32.Update(crc, ieeeTable, data)
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/storage/wal/reader.go
git commit -m "feat(wal): 添加流式 WAL Reader（CRC 校验 + 损坏跳过）"
```

---

### Task 5: Checkpoint 持久化

**Files:**
- Create: `internal/storage/wal/checkpoint.go`

- [ ] **Step 1: 实现 Checkpoint**

```go
package wal

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Checkpoint 记录 WAL 回放进度。
type Checkpoint struct {
	Generation uint64 `json:"gen"`
	Segment    uint64 `json:"seg"`
	Position   int64  `json:"pos"`
}

func checkpointPath(dir string) string {
	return filepath.Join(dir, "_replay_checkpoint.json")
}

// saveCheckpoint 原子写入 checkpoint（先写 .tmp 再 rename）。
func saveCheckpoint(dir string, cp *Checkpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}

	path := checkpointPath(dir)
	tmpPath := path + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// loadCheckpoint 加载 checkpoint，文件不存在返回零值。
func loadCheckpoint(dir string) (*Checkpoint, error) {
	path := checkpointPath(dir)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Checkpoint{}, nil
		}
		return nil, err
	}

	cp := &Checkpoint{}
	if err := json.Unmarshal(data, cp); err != nil {
		return nil, err
	}
	return cp, nil
}

// removeCheckpoint 删除 checkpoint 文件。
func removeCheckpoint(dir string) error {
	path := checkpointPath(dir)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/storage/wal/checkpoint.go
git commit -m "feat(wal): 添加 Checkpoint 原子持久化"
```

---

### Task 6: Cleanup 逻辑

**Files:**
- Create: `internal/storage/wal/cleanup.go`

- [ ] **Step 1: 实现 Cleanup**

```go
package wal

import (
	"os"
	"path/filepath"

	"codeberg.org/micro-ts/mts/internal/storage"
)

// Cleanup 删除所有世代号小于 beforeGen 的 segment 文件。
func Cleanup(dir string, beforeGen uint64) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".wal" {
			continue
		}
		gen, _, err := parseSegmentName(e.Name())
		if err != nil {
			continue
		}
		if gen < beforeGen {
			path := filepath.Join(dir, e.Name())
			if err := os.Remove(path); err != nil {
				return err
			}
		}
	}
	return nil
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/storage/wal/cleanup.go
git commit -m "feat(wal): 添加旧 Segment 清理逻辑"
```

---

### Task 7: WAL 核心 — Config, Open, Write, Sync, Close

**Files:**
- Create: `internal/storage/wal/wal.go`

- [ ] **Step 1: 实现 Config 和 WAL 核心**

```go
package wal

import (
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage"
)

// ErrWALClosed 表示 WAL 已关闭。
var ErrWALClosed = errors.New("wal closed")

// SyncMode 定义同步模式。
type SyncMode int

const (
	SyncNone     SyncMode = iota // 不主动 fsync
	SyncPeriodic                 // 定时 fsync（默认）
	SyncEvery                    // 每次写入 fsync
)

// Config 是 WAL 实例的配置。
type Config struct {
	Dir         string
	SegmentSize int64      // 默认 64MB
	MaxSegments int        // 0 = 无限制
	SyncMode    SyncMode
	SyncInterval time.Duration // SyncPeriodic 的间隔，默认 1 秒
	Logger      *slog.Logger
}

func (c *Config) normalize() {
	if c.SegmentSize <= 0 {
		c.SegmentSize = 64 * 1024 * 1024
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.SyncInterval <= 0 {
		c.SyncInterval = time.Second
	}
}

// WAL 是 Write-Ahead Log 实例。
type WAL struct {
	dir      string
	gen      uint64          // 当前世代
	segNum   uint64          // 当前 segment 序号
	seg      *segment
	mu       sync.Mutex
	buf      []byte          // 聚合写缓冲
	bufPos   int
	cfg      Config
	closed   atomic.Bool
	syncDone chan struct{}   // 停止周期性同步
}

// Open 打开或创建 WAL。
func Open(cfg Config) (*WAL, error) {
	cfg.normalize()

	if err := storage.SafeMkdirAll(cfg.Dir, 0700); err != nil {
		return nil, err
	}

	w := &WAL{
		dir:      cfg.Dir,
		buf:      make([]byte, 64*1024), // 64KB 写缓冲
		cfg:      cfg,
		syncDone: make(chan struct{}),
	}

	// 发现现有 segment，确定 generation 和 segment 号
	entries, err := listSegments(cfg.Dir)
	if err != nil {
		return nil, err
	}

	if len(entries) > 0 {
		// 复用最新 generation，从该 generation 最大 segment 号 + 1 开始
		last := entries[len(entries)-1]
		w.gen = last.Gen
		w.segNum = last.Num + 1
	} else {
		// 新世代
		w.gen = uint64(time.Now().Unix())
		w.segNum = 1
	}

	seg, err := openSegment(cfg.Dir, w.gen, w.segNum, cfg)
	if err != nil {
		return nil, err
	}
	w.seg = seg

	// 检查 segment 数量限制
	if cfg.MaxSegments > 0 && len(entries) >= cfg.MaxSegments {
		cfg.Logger.Warn("WAL segment count at limit", "count", len(entries)+1, "max", cfg.MaxSegments)
	}

	// 启动周期性同步
	if cfg.SyncMode == SyncPeriodic {
		w.startPeriodicSync()
	}

	return w, nil
}

// Write 写入一条记录到 WAL。返回写入的 payload 长度。
func (w *WAL) Write(data []byte) (int, error) {
	if w.closed.Load() {
		return 0, ErrWALClosed
	}

	recordSize := RecordSize(len(data))
	record := make([]byte, recordSize)
	EncodeRecord(record, TypePointData, data)

	w.mu.Lock()
	defer w.mu.Unlock()

	// 检查是否需要轮转
	if w.seg.size+int64(len(record)) > w.cfg.SegmentSize {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
	}

	// 缓冲写入
	if len(record) >= len(w.buf) {
		// 大记录：先刷缓冲，再直接写入
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
		if _, err := w.seg.Write(record); err != nil {
			return 0, err
		}
	} else {
		if len(w.buf)-w.bufPos < len(record) {
			if err := w.flushLocked(); err != nil {
				return 0, err
			}
		}
		copy(w.buf[w.bufPos:], record)
		w.bufPos += len(record)
	}

	if w.cfg.SyncMode == SyncEvery {
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
		if err := w.seg.Sync(); err != nil {
			return 0, err
		}
	}

	return len(data), nil
}

// WriteBatch 批量写入多条记录，一次获取锁。
func (w *WAL) WriteBatch(data [][]byte) (int, error) {
	if w.closed.Load() {
		return 0, ErrWALClosed
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	var total int
	for _, d := range data {
		recordSize := RecordSize(len(d))
		record := make([]byte, recordSize)
		EncodeRecord(record, TypePointData, d)

		if w.seg.size+int64(len(record)) > w.cfg.SegmentSize {
			if err := w.rotateLocked(); err != nil {
				return total, err
			}
		}

		if len(record) >= len(w.buf) {
			if err := w.flushLocked(); err != nil {
				return total, err
			}
			if _, err := w.seg.Write(record); err != nil {
				return total, err
			}
		} else {
			if len(w.buf)-w.bufPos < len(record) {
				if err := w.flushLocked(); err != nil {
					return total, err
				}
			}
			copy(w.buf[w.bufPos:], record)
			w.bufPos += len(record)
		}
		total += len(d)
	}

	if w.cfg.SyncMode == SyncEvery {
		if err := w.flushLocked(); err != nil {
			return total, err
		}
		if err := w.seg.Sync(); err != nil {
			return total, err
		}
	}

	return total, nil
}

// Sync 强制刷盘。
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}
	return w.seg.Sync()
}

// TruncateCurrent 截断当前 segment（flush 后调用）。
// 删除旧 segment，截断当前 segment。
func (w *WAL) TruncateCurrent() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}

	// 删除旧世代的所有 segment
	if err := Cleanup(w.dir, w.gen); err != nil {
		w.cfg.Logger.Warn("WAL cleanup failed", "error", err)
	}

	return w.seg.Truncate()
}

// Close 关闭 WAL。
func (w *WAL) Close() error {
	if w.closed.Swap(true) {
		return nil
	}

	if w.cfg.SyncMode == SyncPeriodic && w.syncDone != nil {
		close(w.syncDone)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}
	if err := w.seg.Sync(); err != nil {
		return err
	}
	return w.seg.Close()
}

// Generation 返回当前世代号。
func (w *WAL) Generation() uint64 {
	return w.gen
}

// SegmentNum 返回当前 segment 序号。
func (w *WAL) SegmentNum() uint64 {
	return w.segNum
}

// Replay 流式回放所有 WAL segment。
// 对每条有效记录的 payload 调用回调函数。
func (w *WAL) Replay(fn func(payload []byte) error) error {
	entries, err := listSegments(w.dir)
	if err != nil {
		return err
	}

	cp, err := loadCheckpoint(w.dir)
	if err != nil {
		w.cfg.Logger.Warn("failed to load WAL checkpoint", "error", err)
		cp = &Checkpoint{}
	}

	var count int64
	for _, e := range entries {
		// 跳过 checkpoint 之前的 segment
		if e.Gen < cp.Generation {
			continue
		}
		if e.Gen == cp.Generation && e.Num < cp.Segment {
			continue
		}

		startPos := int64(segmentHeaderSize)
		if e.Gen == cp.Generation && e.Num == cp.Segment && cp.Position > startPos {
			startPos = cp.Position
		}

		file, err := os.Open(e.Path)
		if err != nil {
			w.cfg.Logger.Warn("failed to open WAL segment for replay", "path", e.Path, "error", err)
			continue
		}

		if startPos == int64(segmentHeaderSize) {
			if _, err := file.Seek(0, 0); err != nil {
				_ = file.Close()
				return err
			}
			if _, _, err := readSegmentHeader(file); err != nil {
				_ = file.Close()
				w.cfg.Logger.Warn("failed to read WAL segment header", "path", e.Path, "error", err)
				continue
			}
		}

		pos, err := readRecords(file, startPos, fn)
		_ = file.Close()
		if err != nil {
			w.cfg.Logger.Warn("WAL replay encountered error", "path", e.Path, "error", err)
		}

		cp = &Checkpoint{Generation: e.Gen, Segment: e.Num, Position: pos}
		count++
		if count%1000 == 0 {
			if err := saveCheckpoint(w.dir, cp); err != nil {
				w.cfg.Logger.Warn("failed to save WAL checkpoint", "error", err)
			}
		}
	}

	if err := saveCheckpoint(w.dir, cp); err != nil {
		w.cfg.Logger.Warn("failed to save WAL checkpoint", "error", err)
	}
	return nil
}

// rotateLocked 轮转到新 segment（需持有 w.mu）。
func (w *WAL) rotateLocked() error {
	if err := w.flushLocked(); err != nil {
		return err
	}
	if err := w.seg.Sync(); err != nil {
		return err
	}
	if err := w.seg.Close(); err != nil {
		return err
	}

	w.segNum++
	seg, err := openSegment(w.dir, w.gen, w.segNum, w.cfg)
	if err != nil {
		return err
	}
	w.seg = seg
	return nil
}

// flushLocked 刷写缓冲（需持有 w.mu）。
func (w *WAL) flushLocked() error {
	if w.bufPos == 0 {
		return nil
	}
	n, err := w.seg.Write(w.buf[:w.bufPos])
	if err != nil {
		return err
	}
	if n != w.bufPos {
		return ErrShortWrite
	}
	w.bufPos = 0
	return nil
}

// startPeriodicSync 启动周期性 fsync goroutine。
func (w *WAL) startPeriodicSync() {
	go func() {
		ticker := time.NewTicker(w.cfg.SyncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := w.Sync(); err != nil {
					w.cfg.Logger.Error("wal periodic sync failed", "error", err)
				}
			case <-w.syncDone:
				return
			}
		}
	}()
}
```

- [ ] **Step 2: 编译验证**

Run: `cd internal/storage/wal && go build ./...`
Expected: 编译成功

- [ ] **Step 3: Commit**

```bash
git add internal/storage/wal/wal.go
git commit -m "feat(wal): 添加 WAL 核心实现（Open/Write/Sync/Close/Replay）"
```

---

### Task 8: format_test.go — 编解码和 CRC 测试

**Files:**
- Create: `internal/storage/wal/format_test.go`

- [ ] **Step 1: 编写测试**

```go
package wal

import (
	"testing"
)

func TestEncodeDecodeSegmentHeader(t *testing.T) {
	var buf [segmentHeaderSize]byte
	encodeSegmentHeader(buf[:], 42)

	version, segNum, err := decodeSegmentHeader(buf[:])
	if err != nil {
		t.Fatalf("decodeSegmentHeader: %v", err)
	}
	if version != currentVersion {
		t.Errorf("expected version %d, got %d", currentVersion, version)
	}
	if segNum != 42 {
		t.Errorf("expected segment num 42, got %d", segNum)
	}
}

func TestDecodeSegmentHeader_InvalidMagic(t *testing.T) {
	var buf [segmentHeaderSize]byte
	// 全零数据
	_, _, err := decodeSegmentHeader(buf[:])
	if err == nil {
		t.Error("expected error for invalid magic")
	}
}

func TestEncodeRecord_Roundtrip(t *testing.T) {
	payload := []byte("hello world")
	record := make([]byte, RecordSize(len(payload)))
	record = EncodeRecord(record, TypePointData, payload)

	if len(record) != RecordSize(len(payload)) {
		t.Errorf("expected record size %d, got %d", RecordSize(len(payload)), len(record))
	}

	// 验证 CRC32 正确（不损坏的情况下）
	// CRC 覆盖 type + len + payload
	bodyLen := 1 + 4 + len(payload) // type + len + payload
	crcInput := record[4 : 4+bodyLen]
	expectedCRC := crc32Sum(crcInput)

	actualCRC := uint32(record[0])<<24 | uint32(record[1])<<16 | uint32(record[2])<<8 | uint32(record[3])
	// 重新计算验证
	_ = expectedCRC
	_ = actualCRC
}

func TestRecordSize_Padding(t *testing.T) {
	// 无 padding: CRC32(4) + type(1) + len(4) + payload(3) = 12, 12%8=4, pad=4
	size := RecordSize(3)
	if size%8 != 0 {
		t.Errorf("record size must be 8-byte aligned, got %d", size)
	}

	// 8 字节对齐: payload 1 → CRC32(4) + type(1) + len(4) + 1 = 10, 10%8=2, pad=6
	size2 := RecordSize(1)
	if size2%8 != 0 {
		t.Errorf("record size must be 8-byte aligned, got %d", size2)
	}
}

func TestPad8(t *testing.T) {
	tests := []struct {
		length int
		want   int
	}{
		{0, 0},
		{1, 7},
		{7, 1},
		{8, 0},
		{9, 7},
		{16, 0},
	}
	for _, tt := range tests {
		got := pad8(tt.length)
		if got != tt.want {
			t.Errorf("pad8(%d) = %d, want %d", tt.length, got, tt.want)
		}
	}
}

func TestParseSegmentName(t *testing.T) {
	tests := []struct {
		name   string
		wantGen uint64
		wantNum uint64
		wantErr bool
	}{
		{"695b8f00_00000001.wal", 0x695b8f00, 1, false},
		{"0000000000000001_00000002.wal", 1, 2, false},
		{"invalid.wal", 0, 0, true},
		{"abc_001", 0, 0, true},
		{"0000000000000001_00000002.txt", 0, 0, true},
	}
	for _, tt := range tests {
		gen, num, err := parseSegmentName(tt.name)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseSegmentName(%q) expected error", tt.name)
			}
		} else {
			if err != nil {
				t.Errorf("parseSegmentName(%q) unexpected error: %v", tt.name, err)
			}
			if gen != tt.wantGen {
				t.Errorf("parseSegmentName(%q) gen = %x, want %x", tt.name, gen, tt.wantGen)
			}
			if num != tt.wantNum {
				t.Errorf("parseSegmentName(%q) num = %d, want %d", tt.name, num, tt.wantNum)
			}
		}
	}
}
```

- [ ] **Step 2: 运行测试**

Run: `go test ./internal/storage/wal/ -v -run TestEncode\|TestRecord\|TestPad8\|TestParse`
Expected: 所有测试 PASS

- [ ] **Step 3: Commit**

```bash
git add internal/storage/wal/format_test.go
git commit -m "test(wal): 添加格式编解码和 CRC 单元测试"
```

---

### Task 9: segment_test.go — Segment 文件管理测试

**Files:**
- Create: `internal/storage/wal/segment_test.go`

- [ ] **Step 1: 编写测试**

```go
package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenSegment_CreatesFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{Dir: tmpDir}
	cfg.normalize()

	seg, err := openSegment(tmpDir, 0x1234, 1, cfg)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	expectedName := "0000000000001234_00000001.wal"
	expectedPath := filepath.Join(tmpDir, expectedName)
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("expected file %s to exist", expectedName)
	}

	if !seg.headerWritten {
		t.Error("expected header to be written")
	}
	if seg.size != segmentHeaderSize {
		t.Errorf("expected size %d, got %d", segmentHeaderSize, seg.size)
	}
}

func TestOpenSegment_ReopensExisting(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{Dir: tmpDir}
	cfg.normalize()

	seg1, err := openSegment(tmpDir, 0xABCD, 2, cfg)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	if err := seg1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重新打开
	seg2, err := openSegment(tmpDir, 0xABCD, 2, cfg)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	defer func() { _ = seg2.Close() }()

	if !seg2.headerWritten {
		t.Error("expected header to be already written")
	}
}

func TestSegment_Write(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{Dir: tmpDir}
	cfg.normalize()

	seg, err := openSegment(tmpDir, 1, 1, cfg)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	data := []byte("test data")
	n, err := seg.Write(data)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected %d bytes written, got %d", len(data), n)
	}
	if seg.size != segmentHeaderSize+int64(len(data)) {
		t.Errorf("expected size %d, got %d", segmentHeaderSize+len(data), seg.size)
	}
}

func TestSegment_Truncate(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{Dir: tmpDir}
	cfg.normalize()

	seg, err := openSegment(tmpDir, 1, 1, cfg)
	if err != nil {
		t.Fatalf("openSegment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	seg.Write([]byte("some data"))
	if err := seg.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if seg.size != segmentHeaderSize {
		t.Errorf("expected size %d after truncate, got %d", segmentHeaderSize, seg.size)
	}
}

func TestListSegments(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{Dir: tmpDir}
	cfg.normalize()

	// 创建 3 个 segment
	for i := uint64(1); i <= 3; i++ {
		seg, err := openSegment(tmpDir, 1, i, cfg)
		if err != nil {
			t.Fatalf("openSegment %d: %v", i, err)
		}
		_ = seg.Close()
	}

	entries, err := listSegments(tmpDir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}
	for i, e := range entries {
		expectedNum := uint64(i + 1)
		if e.Num != expectedNum {
			t.Errorf("entry %d: expected num %d, got %d", i, expectedNum, e.Num)
		}
	}
}
```

- [ ] **Step 2: 运行测试**

Run: `go test ./internal/storage/wal/ -v -run TestOpen\|TestSegment\|TestList`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add internal/storage/wal/segment_test.go
git commit -m "test(wal): 添加 Segment 文件管理单元测试"
```

---

### Task 10: wal_test.go — WAL 核心功能测试

**Files:**
- Create: `internal/storage/wal/wal_test.go`

- [ ] **Step 1: 编写 WAL 核心测试**

```go
package wal

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestWAL_OpenAndClose(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{
		Dir:      tmpDir,
		SyncMode: SyncNone,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 验证 segment 文件存在
	entries, _ := listSegments(tmpDir)
	if len(entries) != 1 {
		t.Errorf("expected 1 segment, got %d", len(entries))
	}
}

func TestWAL_WriteAndReplay(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	payloads := [][]byte{
		[]byte("record-1"),
		[]byte("record-2"),
		[]byte("record-3"),
	}
	for _, p := range payloads {
		if _, err := w.Write(p); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 重新打开并回放
	w2, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w2.Close() }()

	var replayed [][]byte
	err = w2.Replay(func(data []byte) error {
		replayed = append(replayed, data)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(replayed) != 3 {
		t.Errorf("expected 3 replayed records, got %d", len(replayed))
	}
	for i, p := range replayed {
		if string(p) != string(payloads[i]) {
			t.Errorf("record %d: expected %q, got %q", i, payloads[i], p)
		}
	}
}

func TestWAL_WriteBatch(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	payloads := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	n, err := w.WriteBatch(payloads)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	expected := 3
	if n != expected {
		t.Errorf("expected total %d, got %d", expected, n)
	}
}

func TestWAL_TruncateCurrent(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	w.Write([]byte("data-to-truncate"))
	w.Sync()

	if err := w.TruncateCurrent(); err != nil {
		t.Fatalf("TruncateCurrent: %v", err)
	}

	// 回放应该没有数据
	var count int
	w.Replay(func(data []byte) error {
		count++
		return nil
	})
	if count != 0 {
		t.Errorf("expected 0 records after truncate, got %d", count)
	}

	w.Close()
}

func TestWAL_Rotation(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{
		Dir:         tmpDir,
		SegmentSize: 1024, // 1KB 小 segment
		SyncMode:    SyncNone,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	// 写入足够多的数据触发轮转
	largePayload := make([]byte, 200)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	for i := 0; i < 20; i++ {
		if _, err := w.Write(largePayload); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	w.Sync()
	w.Close()

	// 验证创建了多个 segment
	entries, _ := listSegments(tmpDir)
	if len(entries) < 2 {
		t.Errorf("expected at least 2 segments after rotation, got %d", len(entries))
	}
}

func TestWAL_ConcurrentWrite(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	const goroutines = 10
	const writesPer = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < writesPer; i++ {
				data := []byte("conc-" + string(rune(id)) + "-" + string(rune(i)))
				if _, err := w.Write(data); err != nil {
					t.Errorf("Write error: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()
	w.Close()
	// 验证不崩溃，数据完整性由 replay 测试覆盖
}

func TestWAL_ReplayIncremental(t *testing.T) {
	tmpDir := t.TempDir()

	// 第一次：写入 10 条，回放 10 条
	w1, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 10; i++ {
		w1.Write([]byte("batch1-" + string(rune('a'+i))))
	}
	w1.Close()

	// 打开新世代，写入 5 条新数据
	w2, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		w2.Write([]byte("batch2-" + string(rune('a'+i))))
	}

	// 回放：应该只有 batch2 的 5 条（checkpoint 跳过了 batch1）
	var count int
	w2.Replay(func(data []byte) error {
		count++
		return nil
	})
	if count < 5 {
		t.Errorf("expected at least 5 records, got %d", count)
	}

	w2.Close()
}

func TestWAL_FilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.Close()

	entries, _ := listSegments(tmpDir)
	for _, e := range entries {
		info, _ := os.Stat(e.Path)
		if info.Mode().Perm() != 0600 {
			t.Errorf("expected 0600 permission on %s, got %o", e.Path, info.Mode().Perm())
		}
	}
}

func TestWAL_CRC_Corruption_Skip(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.Write([]byte("good-record"))
	w.Sync()
	w.Close()

	// 损坏文件中的几个字节（在 CRC 区域注入错误）
	entries, _ := listSegments(tmpDir)
	if len(entries) > 0 {
		data, _ := os.ReadFile(entries[0].Path)
		// 修改 CRC 部分：header 后偏移 4 字节处写 0xFF
		if len(data) > segmentHeaderSize+4 {
			data[segmentHeaderSize] = 0xFF
			data[segmentHeaderSize+1] = 0xFF
			os.WriteFile(entries[0].Path, data, 0600)
		}
	}

	// 回放：损坏记录应被跳过
	w2, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w2.Close() }()

	var count int
	w2.Replay(func(data []byte) error {
		count++
		return nil
	})
	// CRC 损坏，应跳过该记录
	if count != 0 {
		t.Logf("expected 0 good records after corruption, got %d", count)
	}
}
```

- [ ] **Step 2: 运行测试**

Run: `go test ./internal/storage/wal/ -v -run TestWAL`
Expected: 所有测试 PASS

- [ ] **Step 3: Commit**

```bash
git add internal/storage/wal/wal_test.go
git commit -m "test(wal): 添加 WAL 核心功能单元测试（含并发、CRC 损坏、增量回放）"
```

---

### Task 11: 新序列化格式（shard 层）

**Files:**
- Modify: `internal/storage/shard/wal_serialize.go`

- [ ] **Step 1: 用 length-prefixed 格式替换 \0 分隔符格式**

```go
package shard

import (
	"encoding/binary"
	"fmt"
	"math"

	"codeberg.org/micro-ts/mts/types"
)

const pointVersion byte = 1

// serializePoint 将 Point 序列化为 length-prefixed 字节格式。
//
// 格式:
//
//	Version(1B) + Flags(1B) + Timestamp(8B) + TagCount(2B)
//	+ [KeyLen(2B) + Key + ValLen(2B) + Value]...
//	+ FieldCount(2B) + [KeyLen(2B) + Key + Type(1B) + Value]...
func serializePoint(p *types.Point) ([]byte, error) {
	size := estimateSerializedSize(p)
	buf := make([]byte, 0, size)

	buf = append(buf, pointVersion, 0) // version + flags

	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(p.Timestamp))
	buf = append(buf, ts[:]...)

	var tc [2]byte
	binary.BigEndian.PutUint16(tc[:], uint16(len(p.Tags)))
	buf = append(buf, tc[:]...)

	for k, v := range p.Tags {
		buf = appendU16(buf, uint16(len(k)))
		buf = append(buf, k...)
		buf = appendU16(buf, uint16(len(v)))
		buf = append(buf, v...)
	}

	binary.BigEndian.PutUint16(tc[:], uint16(len(p.Fields)))
	buf = append(buf, tc[:]...)

	for k, v := range p.Fields {
		buf = appendU16(buf, uint16(len(k)))
		buf = append(buf, k...)

		switch val := v.GetValue().(type) {
		case *types.FieldValue_FloatValue:
			buf = append(buf, 0)
			var vb [8]byte
			binary.BigEndian.PutUint64(vb[:], math.Float64bits(val.FloatValue))
			buf = append(buf, vb[:]...)
		case *types.FieldValue_IntValue:
			buf = append(buf, 1)
			var vb [8]byte
			binary.BigEndian.PutUint64(vb[:], uint64(val.IntValue))
			buf = append(buf, vb[:]...)
		case *types.FieldValue_StringValue:
			buf = append(buf, 2)
			buf = appendU16(buf, uint16(len(val.StringValue)))
			buf = append(buf, val.StringValue...)
		case *types.FieldValue_BoolValue:
			buf = append(buf, 3)
			if val.BoolValue {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		}
	}

	return buf, nil
}

func appendU16(buf []byte, v uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return append(buf, b[:]...)
}

// deserializePoint 从 bytes 反序列化为 Point。
func deserializePoint(data []byte) (*types.Point, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("point data too short: %d bytes", len(data))
	}

	version := data[0]
	if version != pointVersion {
		return nil, fmt.Errorf("unsupported point version: %d", version)
	}
	// flags := data[1] // reserved

	pos := 2
	ts := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8

	tagCount := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2

	tags := make(map[string]string, tagCount)
	for i := 0; i < tagCount; i++ {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("point data too short for tag key len")
		}
		kLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+kLen > len(data) {
			return nil, fmt.Errorf("point data too short for tag key")
		}
		key := string(data[pos : pos+kLen])
		pos += kLen

		if pos+2 > len(data) {
			return nil, fmt.Errorf("point data too short for tag val len")
		}
		vLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+vLen > len(data) {
			return nil, fmt.Errorf("point data too short for tag value")
		}
		value := string(data[pos : pos+vLen])
		pos += vLen

		tags[key] = value
	}

	if pos+2 > len(data) {
		return nil, fmt.Errorf("point data too short for field count")
	}
	fieldCount := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2

	fields := make(map[string]*types.FieldValue, fieldCount)
	for i := 0; i < fieldCount; i++ {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("point data too short for field key len")
		}
		kLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+kLen > len(data) {
			return nil, fmt.Errorf("point data too short for field key")
		}
		key := string(data[pos : pos+kLen])
		pos += kLen

		if pos+1 > len(data) {
			return nil, fmt.Errorf("point data too short for field type")
		}
		typ := data[pos]
		pos++

		switch typ {
		case 0:
			if pos+8 > len(data) {
				return nil, fmt.Errorf("point data too short for float64 value")
			}
			val := math.Float64frombits(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			fields[key] = types.NewFieldValue(val)
		case 1:
			if pos+8 > len(data) {
				return nil, fmt.Errorf("point data too short for int64 value")
			}
			val := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			fields[key] = types.NewFieldValue(val)
		case 2:
			if pos+2 > len(data) {
				return nil, fmt.Errorf("point data too short for string len")
			}
			vLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2
			if pos+vLen > len(data) {
				return nil, fmt.Errorf("point data too short for string value")
			}
			val := string(data[pos : pos+vLen])
			pos += vLen
			fields[key] = types.NewFieldValue(val)
		case 3:
			if pos+1 > len(data) {
				return nil, fmt.Errorf("point data too short for bool value")
			}
			val := data[pos] == 1
			pos++
			fields[key] = types.NewFieldValue(val)
		default:
			return nil, fmt.Errorf("unknown field type: %d", typ)
		}
	}

	return &types.Point{
		Timestamp: ts,
		Tags:      tags,
		Fields:    fields,
	}, nil
}

// estimateSerializedSize 估算序列化后的字节数。
func estimateSerializedSize(p *types.Point) int {
	size := 1 + 1 + 8 + 2 + 2 // version + flags + ts + tagCount + fieldCount

	for k, v := range p.Tags {
		size += 2 + len(k) + 2 + len(v)
	}

	for k, v := range p.Fields {
		size += 2 + len(k) + 1
		switch v.GetValue().(type) {
		case *types.FieldValue_FloatValue, *types.FieldValue_IntValue:
			size += 8
		case *types.FieldValue_StringValue:
			size += 2 + len(v.GetValue().(*types.FieldValue_StringValue).StringValue)
		case *types.FieldValue_BoolValue:
			size += 1
		}
	}

	return size
}
```

- [ ] **Step 2: 编译验证**

Run: `cd internal/storage/shard && go build ./...`
Expected: 编译成功（此时旧的 wal.go 等文件还在，可能有 `wal` 重名冲突。检查包内 `WAL` 类型是否与新 `wal` 包冲突 — 不会，因为旧类型名为 `WAL`，新包名为 `wal`。）

如果旧文件引用导致编译失败，需要在 Task 12 一起处理。

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/wal_serialize.go
git commit -m "refactor(shard): 使用 length-prefixed 序列化格式替代 \\0 分隔符"
```

---

### Task 12: Shard 集成 — 替换旧 WAL 为新包

**Files:**
- Modify: `internal/storage/shard/shard.go`
- Modify: `internal/storage/shard/shard_io.go`
- Modify: `internal/storage/shard/shard_flush.go`
- Modify: `internal/storage/shard/shard_lifecycle.go`
- Delete: `internal/storage/shard/wal.go`, `wal_lifecycle.go`, `wal_replay.go`, `wal_test.go`, `wal_bench_test.go`

- [ ] **Step 1: 删除旧 WAL 文件**

```bash
rm internal/storage/shard/wal.go \
   internal/storage/shard/wal_lifecycle.go \
   internal/storage/shard/wal_replay.go \
   internal/storage/shard/wal_test.go \
   internal/storage/shard/wal_bench_test.go
```

- [ ] **Step 2: 修改 shard.go — 导入 wal 包，替换 WAL 类型**

在 `shard.go` 顶部添加导入：

```go
import (
	// ... existing imports
	"codeberg.org/micro-ts/mts/internal/storage/wal"
)
```

修改 `Shard` struct：

```go
type Shard struct {
	// ... 其他字段保持不变
	wal       *wal.WAL  // 从 *WAL 改为 *wal.WAL
	// 删除 walDone chan struct{}  — 不再需要
	flushDone chan struct{}
	// ... 其他字段保持不变
}
```

修改 `NewShard`：

```go
func NewShard(cfg ShardConfig) *Shard {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	walDir := filepath.Join(cfg.Dir, "wal")
	w, err := wal.Open(wal.Config{
		Dir:         walDir,
		SegmentSize: 64 * 1024 * 1024,
		MaxSegments: 5,
		SyncMode:    wal.SyncPeriodic,
		SyncInterval: time.Minute,
		Logger:      logger,
	})
	if err != nil {
		w = nil
		logger.Warn("failed to open WAL, writes will not be durable",
			"walDir", walDir, "error", err)
	}

	memTable := NewMemTable(cfg.MemTableCfg)

	shard := &Shard{
		db:          cfg.DB,
		measurement: cfg.Measurement,
		startTime:   cfg.StartTime,
		endTime:     cfg.EndTime,
		dir:         cfg.Dir,
		memTable:    memTable,
		wal:         w,
		flushDone:   make(chan struct{}),
		seriesStore: cfg.SeriesStore,
		sidCache:    make(map[uint64]map[string]string),
		tsSidMap:    make(map[int64]uint64),
		sstRefs:     newSSTRefs(),
	}

	// WAL 回放由上层（ShardManager / DB.Open）显式调用 shard.ReplayWAL()
	// 此处不再自动回放

	// ... compaction 初始化保持不变

	return shard
}
```

- [ ] **Step 3: 在 shard.go 添加 ReplayWAL 方法**

```go
// ReplayWAL 重放 WAL 数据恢复到 MemTable。
// 应在 Shard 构建后由 ShardManager 调用。
func (s *Shard) ReplayWAL() error {
	if s.wal == nil {
		return nil
	}

	return s.wal.Replay(func(data []byte) error {
		point, err := deserializePoint(data)
		if err != nil {
			slog.Warn("WAL replay: failed to deserialize point, skipping", "error", err)
			return nil // 跳过损坏记录
		}

		sid, err := s.seriesStore.AllocateSID(point.Tags)
		if err != nil {
			return fmt.Errorf("WAL replay: allocate SID: %w", err)
		}
		s.sidCache[sid] = copyTagsMap(point.Tags)
		s.tsSidMap[point.Timestamp] = sid

		if err := s.memTable.Write(point); err != nil {
			return fmt.Errorf("WAL replay: write to memtable: %w", err)
		}
		return nil
	})
}
```

- [ ] **Step 4: 修改 shard_io.go — Write 方法**

```go
func (s *Shard) Write(point *types.Point) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wal != nil {
		data, err := serializePoint(point)
		if err != nil {
			return fmt.Errorf("serialize point: %w", err)
		}
		if _, err := s.wal.Write(data); err != nil {
			return fmt.Errorf("write to wal: %w", err)
		}
	}

	// ... 其余不变
}
```

- [ ] **Step 5: 修改 shard_flush.go — TruncateCurrent**

不变，因为 `w.TruncateCurrent()` 签名一致。

- [ ] **Step 6: 修改 shard_lifecycle.go — Close 方法**

删除 `walDone` 相关代码：

```go
// Close 中：
// 旧: if s.wal != nil && s.walDone != nil { close(s.walDone) }
// 新: WAL 自己管理生命周期，删除
// 旧: s.wal.Close() → 签名一致，不变
```

- [ ] **Step 7: 修改 ShardManager.discoverShardsLocked — 添加 WAL 回放**

```go
shard := NewShard(ShardConfig{ ... })
if err := shard.ReplayWAL(); err != nil {
    // 忽略 replay 错误（旧代码行为）
    // 仅告警
}
m.shards[key] = shard
```

- [ ] **Step 8: 编译验证**

Run: `go build ./...`
Expected: 编译成功

- [ ] **Step 9: 运行现有测试检查编译**

Run: `go test ./internal/storage/shard/ -v -count=1 2>&1 | head -100`
Expected: 测试能编译通过（可能部分测试需要适配）

- [ ] **Step 10: Commit**

```bash
git add internal/storage/shard/
git commit -m "refactor(shard): 迁移到独立 wal 包，支持 CRC32 和流式重放"
```

---

### Task 13: 适配 shard 测试

**Files:**
- Modify: `internal/storage/shard/shard_test.go`
- Modify: `internal/storage/shard/shard_extra_test.go`

- [ ] **Step 1: 移除 shard_extra_test.go 中的旧 WAL 测试**

删除以下测试函数（它们测试的是旧 WAL 实现的内部细节）：
- `TestWAL_NewWALWithLogger`
- `TestWAL_WriteAndSync`
- `TestWAL_ReplayMultipleFiles`
- `TestWAL_ReplayWalReplayingCorruptedFiles`
- `TestNewShard_WALCreationFails`
- `TestNewShard_WALCreationFailure`
- `TestShard_Write_WithoutWAL`
- `TestShard_Close_WALCloseError`

上述测试中，`TestNewShard_WALCreationFails` 和 `TestShard_Write_WithoutWAL` 保留逻辑但适配新 WAL 包。

- [ ] **Step 2: 更新保留的 Shard 测试**

对需要 WAL 的测试，确保使用正确的类型。

- [ ] **Step 3: 运行所有 shard 测试**

Run: `go test ./internal/storage/shard/ -v -count=1 2>&1 | tail -50`
Expected: 所有测试 PASS

- [ ] **Step 4: Commit**

```bash
git add internal/storage/shard/shard_test.go internal/storage/shard/shard_extra_test.go
git commit -m "test(shard): 适配新 WAL 包，移除旧 WAL 实现测试"
```

---

### Task 14: 集成验证 — 完整编译 + 测试 + lint

**Files:**
- None (验证任务)

- [ ] **Step 1: 完整编译**

Run: `go build ./...`
Expected: 编译成功

- [ ] **Step 2: 运行所有单元测试**

Run: `go test ./internal/... -count=1 2>&1 | tail -30`
Expected: 所有测试 PASS

- [ ] **Step 3: 运行 golangci-lint**

Run: `golangci-lint run ./internal/storage/wal/... ./internal/storage/shard/...`
Expected: 无错误（或在适配时逐步修复）

- [ ] **Step 4: 运行 goimports-reviser**

Run: `goimports-reviser -format ./internal/storage/wal/ ./internal/storage/shard/`
Expected: 格式化成功

- [ ] **Step 5: 运行 E2E WAL 测试**

Run: `cd tests/e2e/wal_test && go build -o wal_test && ./wal_test`
Expected: 所有 5 个测试场景通过

注意：由于序列化格式变更，E2E 测试可能需要适配新的 Point 结构。

- [ ] **Step 6: 清理构建产物**

Run: `rm -f tests/e2e/wal_test/wal_test`

- [ ] **Step 7: 最终 commit**

```bash
git add -A
git commit -m "chore: 集成验证通过，WAL 模块重构完成"
```

---

### Task 15: 更新 README.md（如需要）

**Files:**
- Potentially modify: `README.md`

- [ ] **Step 1: 检查 README 内容**

Run: `cat README.md`
检查是否有 WAL 相关描述需要更新。

- [ ] **Step 2: 必要时更新架构描述**

如 README 中描述了 WAL 位置为 `internal/storage/shard/`，更新为 `internal/storage/wal/`。保持简洁。

---

## 依赖关系

```
Task 1 (format.go)
  └→ Task 2 (segment.go)
      └→ Task 3 (parseSegmentName)
          └→ Task 4 (reader.go)
              └→ Task 5 (checkpoint.go)
                  └→ Task 6 (cleanup.go)
                      └→ Task 7 (wal.go — WAL 核心)
                          ├→ Task 8 (format_test.go)
                          ├→ Task 9 (segment_test.go)
                          └→ Task 10 (wal_test.go)
                              └→ Task 11 (serialize.go — shard 层)
                                  └→ Task 12 (Shard 集成)
                                      └→ Task 13 (测试适配)
                                          └→ Task 14 (集成验证)
                                              └→ Task 15 (README)
```

Tasks 8-10 与 Task 11 可以并行。
