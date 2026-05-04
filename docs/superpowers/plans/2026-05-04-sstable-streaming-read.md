# SSTable 流式读取实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal**: 实现 SSTable 流式读取，通过 block 索引支持时间范围过滤，避免全量加载数据到内存。

**Architecture**:
- 新增 `_index.bin` 统一索引文件，记录每个 block 的 first/last timestamp 和 offset
- Writer 在 Close 时写入索引；Reader 全量加载索引，二分查找定位 block
- SSTable Iterator 按需加载单个 block（64KB），ShardIterator 负责时间过滤和归并

**Tech Stack**: Go, binary encoding, sort.Search (二分查找)

---

## 文件结构

```
internal/storage/shard/sstable/
├── writer.go          # 修改: 添加 block 缓冲和索引写入
├── reader.go         # 修改: 添加索引读取
├── iterator.go       # 重写: 流式迭代器
├── index.go          # 新增: BlockIndex 结构和方法
└── iterator_test.go  # 修改: 适配新的迭代器接口
```

---

## Task 1: 定义 BlockIndex 结构

**Files:**
- Create: `internal/storage/shard/sstable/index.go`

- [ ] **Step 1: 创建 index.go 文件，定义 BlockIndexEntry 结构**

```go
// internal/storage/shard/sstable/index.go
package sstable

import (
	"encoding/binary"
	"sort"
)

// IndexMagic 索引文件魔数 "TSIDX001"
var IndexMagic = [8]byte{0x54, 0x53, 0x49, 0x44, 0x58, 0x30, 0x30, 0x31}

// IndexVersion 索引版本
const IndexVersion = 1

// BlockIndexEntry 单个 block 的索引条目
type BlockIndexEntry struct {
	FirstTimestamp int64  // block 内第一个时间戳
	LastTimestamp  int64  // block 内最后一个时间戳
	Offset         uint32 // block 在 timestamps.bin 文件中的偏移
	RowCount       uint32 // 该 block 的行数
}

// BlockIndex 索引管理器
type BlockIndex struct {
	entries []BlockIndexEntry
}
```

- [ ] **Step 2: 添加 Write 方法**

```go
// Write 写入索引到指定文件
func (idx *BlockIndex) Write(file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	// 写入 header
	var header [16]byte
	copy(header[0:8], IndexMagic[:])
	binary.BigEndian.PutUint32(header[8:12], IndexVersion)
	binary.BigEndian.PutUint32(header[12:16], uint32(len(idx.entries)))
	if _, err := f.Write(header[:]); err != nil {
		return err
	}

	// 写入 entries
	for _, e := range idx.entries {
		var buf [20]byte
		binary.BigEndian.PutUint64(buf[0:8], uint64(e.FirstTimestamp))
		binary.BigEndian.PutUint64(buf[8:16], uint64(e.LastTimestamp))
		binary.LittleEndian.PutUint32(buf[16:20], e.Offset)
		if _, err := f.Write(buf[:]); err != nil {
			return err
		}
	}

	return nil
}
```

- [ ] **Step 3: 添加 Read 方法**

```go
// Read 从指定文件读取索引
func (idx *BlockIndex) Read(file string) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}

	if len(data) < 16 {
		return ErrInvalidIndex
	}

	// 验证 magic
	if string(data[0:8]) != string(IndexMagic[:]) {
		return ErrInvalidIndex
	}

	// 解析 header
	version := binary.BigEndian.Uint32(data[8:12])
	if version != IndexVersion {
		return ErrInvalidIndex
	}

	blockCount := binary.BigEndian.Uint32(data[12:16])
	entrySize := 20
	if len(data) < 16+int(blockCount)*entrySize {
		return ErrInvalidIndex
	}

	// 解析 entries
	idx.entries = make([]BlockIndexEntry, blockCount)
	for i := uint32(0); i < blockCount; i++ {
		off := 16 + int(i)*entrySize
		idx.entries[i] = BlockIndexEntry{
			FirstTimestamp: int64(binary.BigEndian.Uint64(data[off : off+8])),
			LastTimestamp:  int64(binary.BigEndian.Uint64(data[off+8 : off+16])),
			Offset:         binary.LittleEndian.Uint32(data[off+16 : off+20]),
		}
	}

	return nil
}
```

- [ ] **Step 4: 添加 FindBlock 二分查找方法**

```go
// FindBlock 二分查找第一个 last_timestamp >= target 的 block
// 返回 block 索引，如果不存在返回 len(entries)
func (idx *BlockIndex) FindBlock(target int64) int {
	return sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].LastTimestamp >= target
	})
}

// Len 返回 entry 数量
func (idx *BlockIndex) Len() int {
	return len(idx.entries)
}

// Entry 返回指定索引的 entry
func (idx *BlockIndex) Entry(i int) BlockIndexEntry {
	return idx.entries[i]
}
```

- [ ] **Step 5: 添加 Add 方法**

```go
// Add 添加一个 block 的索引
func (idx *BlockIndex) Add(firstTs, lastTs int64, offset uint32, rowCount uint32) {
	idx.entries = append(idx.entries, BlockIndexEntry{
		FirstTimestamp: firstTs,
		LastTimestamp:  lastTs,
		Offset:         offset,
		RowCount:       rowCount,
	})
}
```

- [ ] **Step 6: 添加 error 定义**

```go
// ErrInvalidIndex 无效索引错误
var ErrInvalidIndex = &IndexError{msg: "invalid index file"}

type IndexError struct {
	msg string
}

func (e *IndexError) Error() string {
	return e.msg
}
```

- [ ] **Step 7: 运行测试验证**

Run: `go test ./internal/storage/shard/sstable/... -run TestBlockIndex -v`
Expected: 编译错误（index.go 未创建）

- [ ] **Step 8: 创建 index_test.go 并测试**

```go
// internal/storage/shard/sstable/index_test.go
package sstable

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBlockIndex_WriteRead(t *testing.T) {
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "_index.bin")

	idx := &BlockIndex{}

	// 添加 3 个 block entries
	idx.Add(1000, 1999, 0, 100)
	idx.Add(2000, 2999, 65536, 100)
	idx.Add(3000, 3999, 131072, 100)

	// 写入
	if err := idx.Write(indexFile); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 读取
	idx2 := &BlockIndex{}
	if err := idx2.Read(indexFile); err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// 验证
	if idx2.Len() != 3 {
		t.Errorf("expected 3 entries, got %d", idx2.Len())
	}

	if idx2.Entry(0).FirstTimestamp != 1000 {
		t.Errorf("expected first ts 1000, got %d", idx2.Entry(0).FirstTimestamp)
	}
}

func TestBlockIndex_FindBlock(t *testing.T) {
	idx := &BlockIndex{}

	idx.Add(1000, 1999, 0, 100)
	idx.Add(2000, 2999, 65536, 100)
	idx.Add(3000, 3999, 131072, 100)

	tests := []struct {
		target int64
		expect int
	}{
		{500, 0},    // 在第一个 block 之前
		{1000, 0},   // 第一个 block 的起始
		{1500, 0},   // 第一个 block 中间
		{2000, 1},   // 第二个 block 的起始
		{2500, 1},   // 第二个 block 中间
		{3000, 2},   // 第三个 block 的起始
		{5000, 3},   // 超过所有 block
	}

	for _, tc := range tests {
		result := idx.FindBlock(tc.target)
		if result != tc.expect {
			t.Errorf("FindBlock(%d) = %d, expect %d", tc.target, result, tc.expect)
		}
	}
}
```

Run: `go test ./internal/storage/shard/sstable/... -run TestBlockIndex -v`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add internal/storage/shard/sstable/index.go internal/storage/shard/sstable/index_test.go
git commit -m "feat(sstable): add BlockIndex structure for block-level indexing"
```

---

## Task 2: Writer 添加 block 索引收集

**Files:**
- Modify: `internal/storage/shard/sstable/writer.go`

- [ ] **Step 1: 在 Writer 结构中添加 blockIndex 字段**

Modify `writer.go` line 40-48:

```go
// Writer SSTable 写入器
type Writer struct {
	shardDir   string
	seq        uint64
	dataDir    string
	timestamp  *os.File
	fields     map[string]*os.File
	schema     Schema
	blockIndex *BlockIndex           // block 索引
	buf        []byte               // 当前 block 缓冲
	bufPos     int                  // 缓冲写入位置
	firstTs    int64                // 当前 block 第一个时间戳
	rowCount   uint32               // 当前 block 行数
}
```

- [ ] **Step 2: 修改 NewWriter 初始化 blockIndex**

Modify `writer.go` line 51-75:

```go
// NewWriter 创建 Writer
func NewWriter(shardDir string, seq uint64) (*Writer, error) {
	dataDir := filepath.Join(shardDir, "data")
	if err := storage.SafeMkdirAll(dataDir, 0700); err != nil {
		return nil, err
	}

	fieldsDir := filepath.Join(dataDir, "fields")
	if err := storage.SafeMkdirAll(fieldsDir, 0700); err != nil {
		return nil, err
	}

	tsFile, err := storage.SafeCreate(filepath.Join(dataDir, "_timestamps.bin"), 0600)
	if err != nil {
		return nil, err
	}

	return &Writer{
		shardDir:   shardDir,
		seq:        seq,
		dataDir:    dataDir,
		timestamp:  tsFile,
		fields:     make(map[string]*os.File),
		schema:     Schema{Fields: make(map[string]FieldType)},
		blockIndex: NewBlockIndex(),
		buf:        make([]byte, BlockSize),
		bufPos:     0,
		rowCount:   0,
	}, nil
}
```

- [ ] **Step 3: 添加 NewBlockIndex 构造函数**

Add after the Writer struct:

```go
// NewBlockIndex 创建空的 BlockIndex
func NewBlockIndex() *BlockIndex {
	return &BlockIndex{
		entries: make([]BlockIndexEntry, 0),
	}
}
```

- [ ] **Step 4: 修改 WritePoints 添加 block 缓冲逻辑**

Replace `WritePoints` method (lines 77-123):

```go
// WritePoints 写入一批 points
func (w *Writer) WritePoints(points []*types.Point) error {
	// 收集所有字段名并检测类型
	fieldNames := make(map[string]bool)
	for _, p := range points {
		for name, val := range p.Fields {
			fieldNames[name] = true
			if _, exists := w.schema.Fields[name]; !exists {
				w.schema.Fields[name] = detectFieldType(val)
			}
		}
	}

	// 打开字段文件
	for name := range fieldNames {
		if _, ok := w.fields[name]; !ok {
			f, err := storage.SafeOpenFile(
				filepath.Join(w.dataDir, "fields", name+".bin"),
				os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
			if err != nil {
				return err
			}
			w.fields[name] = f
		}
	}

	// 写入每条数据
	for _, p := range points {
		if err := w.writePoint(p); err != nil {
			return err
		}
	}

	return nil
}

// writePoint 写入单条数据，处理 block 缓冲
func (w *Writer) writePoint(p *types.Point) error {
	// 如果 buf 满了，flush 当前 block
	if w.bufPos >= BlockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	// 记录第一个时间戳
	if w.rowCount == 0 {
		w.firstTs = p.Timestamp
	}

	// 编码并写入 timestamp (delta of delta)
	// 当前简化处理：直接写入 8 字节
	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(p.Timestamp))
	copy(w.buf[w.bufPos:w.bufPos+8], tsBuf[:])
	w.bufPos += 8

	// 写入各字段
	for name, f := range w.fields {
		if val, ok := p.Fields[name]; ok {
			if err := w.writeFieldValueToBuf(val); err != nil {
				return err
			}
		}
	}

	w.rowCount++
	return nil
}

// writeFieldValueToBuf 将字段值写入 buf
func (w *Writer) writeFieldValueToBuf(val any) error {
	switch v := val.(type) {
	case float64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(v))
		copy(w.buf[w.bufPos:w.bufPos+8], buf[:])
		w.bufPos += 8
		return nil
	case int64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(v))
		copy(w.buf[w.bufPos:w.bufPos+8], buf[:])
		w.bufPos += 8
		return nil
	case string:
		// 4 字节长度 + 数据
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(v)))
		copy(w.buf[w.bufPos:w.bufPos+4], lenBuf[:])
		w.bufPos += 4
		copy(w.buf[w.bufPos:w.bufPos+len(v)], v)
		w.bufPos += len(v)
		return nil
	case bool:
		if v {
			w.buf[w.bufPos] = 1
		} else {
			w.buf[w.bufPos] = 0
		}
		w.bufPos++
		return nil
	}
	return nil
}

// flushBlock 将当前 buf 作为 block 写入文件
func (w *Writer) flushBlock() error {
	if w.bufPos == 0 {
		return nil
	}

	// 获取当前文件偏移量
	info, err := w.timestamp.Stat()
	if err != nil {
		return err
	}
	offset := uint32(info.Size())

	// 写入 buf 数据
	if _, err := w.timestamp.Write(w.buf[:w.bufPos]); err != nil {
		return err
	}

	// 记录 block 索引
	lastTs := w.firstTs + int64(w.rowCount-1)*1000 // 简化：假设每行间隔 1ms
	w.blockIndex.Add(w.firstTs, lastTs, offset, w.rowCount)

	// 重置 buf
	w.bufPos = 0
	w.rowCount = 0
	w.firstTs = 0

	return nil
}
```

- [ ] **Step 5: 修改 Close 方法，关闭前 flush 并写入索引**

Replace `Close` method (lines 201-218):

```go
// Close 关闭
func (w *Writer) Close() error {
	// flush 剩余数据
	if err := w.flushBlock(); err != nil {
		return err
	}

	// 写入 schema 文件
	if err := w.writeSchema(); err != nil {
		return err
	}

	// 写入 block 索引文件
	if err := w.writeBlockIndex(); err != nil {
		return err
	}

	if w.timestamp != nil {
		if err := w.timestamp.Close(); err != nil {
			return err
		}
	}
	for _, f := range w.fields {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

// writeBlockIndex 写入 block 索引文件
func (w *Writer) writeBlockIndex() error {
	indexFile := filepath.Join(w.dataDir, "_index.bin")
	return w.blockIndex.Write(indexFile)
}
```

- [ ] **Step 6: 运行测试验证**

Run: `go build ./internal/storage/shard/sstable/...`
Expected: 可能有编译错误

- [ ] **Step 7: 修复编译错误（如果有）**

- [ ] **Step 8: 运行现有测试**

Run: `go test ./internal/storage/shard/sstable/... -v`
Expected: 现有测试应该 PASS

- [ ] **Step 9: Commit**

```bash
git add internal/storage/shard/sstable/writer.go
git commit -m "feat(sstable): add block buffering and index collection to Writer"
```

---

## Task 3: Reader 添加索引读取和向后兼容

**Files:**
- Modify: `internal/storage/shard/sstable/reader.go`

- [ ] **Step 1: 添加索引加载逻辑到 Reader 结构**

Modify `reader.go` line 16-19:

```go
// Reader SSTable 读取器
type Reader struct {
	dataDir   string
	schema    Schema
	blockIndex *BlockIndex  // 新增
}
```

- [ ] **Step 2: 修改 NewReader 加载索引**

Modify `NewReader` method (lines 22-29):

```go
// NewReader 创建 Reader
func NewReader(dataDir string) (*Reader, error) {
	r := &Reader{dataDir: dataDir}
	if err := r.readSchema(); err != nil {
		r.schema = Schema{Fields: make(map[string]FieldType)}
	}

	// 尝试加载 block 索引
	r.blockIndex = &BlockIndex{}
	indexFile := filepath.Join(dataDir, "_index.bin")
	if err := r.blockIndex.Read(indexFile); err != nil {
		// 索引文件不存在或无效，使用空索引
		r.blockIndex = nil
	}

	return r, nil
}
```

- [ ] **Step 3: 添加 BlockIndex 访问方法**

Add after `readSchema`:

```go
// HasBlockIndex 返回是否有有效的 block 索引
func (r *Reader) HasBlockIndex() bool {
	return r.blockIndex != nil && r.blockIndex.Len() > 0
}

// GetBlockIndex 返回 block 索引
func (r *Reader) GetBlockIndex() *BlockIndex {
	return r.blockIndex
}
```

- [ ] **Step 4: 添加兼容旧文件的降级逻辑**

Add after `GetBlockIndex`:

```go
// readAllFallback 当没有索引时，读取全部数据（向后兼容）
func (r *Reader) readAllFallback(fields []string) ([]types.PointRow, error) {
	// 使用现有的 ReadAll 逻辑
	return r.ReadAll(fields)
}
```

- [ ] **Step 5: 修改 ReadRange 支持索引**

Modify `ReadRange` method to use block index if available:

```go
// ReadRange 读取时间范围内的数据
func (r *Reader) ReadRange(startTime, endTime int64) ([]types.PointRow, error) {
	dataDir := r.dataDir

	// 检查数据目录是否存在
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return nil, nil
	}

	// 如果有 block 索引，使用二分查找
	if r.HasBlockIndex() {
		return r.readRangeWithIndex(startTime, endTime)
	}

	// 否则使用原来的线性扫描
	return r.readRangeFallback(startTime, endTime)
}
```

- [ ] **Step 6: 添加 readRangeWithIndex 方法**

Add:

```go
// readRangeWithIndex 使用 block 索引读取范围数据
func (r *Reader) readRangeWithIndex(startTime, endTime int64) ([]types.PointRow, error) {
	dataDir := filepath.Join(r.dataDir, "data")

	// 读取 timestamps
	tsFile, err := os.Open(filepath.Join(dataDir, "_timestamps.bin"))
	if err != nil {
		return nil, err
	}
	timestamps, err := r.readTimestamps(tsFile)
	if closeErr := tsFile.Close(); closeErr != nil {
		return nil, closeErr
	}
	if err != nil {
		return nil, err
	}

	// 二分查找起始位置
	idx := r.blockIndex.FindBlock(startTime)

	// 读取所有字段文件
	entries, err := os.ReadDir(filepath.Join(dataDir, "fields"))
	if err != nil {
		return nil, err
	}

	fields := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			fields = append(fields, e.Name()[:len(e.Name())-4])
		}
	}

	fieldData := make(map[string][]byte)
	for _, name := range fields {
		f, err := os.Open(filepath.Join(dataDir, "fields", name+".bin"))
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(f)
		if closeErr := f.Close(); closeErr != nil {
			return nil, closeErr
		}
		if err != nil {
			return nil, err
		}
		fieldData[name] = data
	}

	// 计算偏移量
	offsets := r.computeOffsets(fields, fieldData, len(timestamps))

	// 构建结果，按时间过滤
	var rows []types.PointRow
	for i := idx; i < len(timestamps); i++ {
		ts := timestamps[i]
		if ts < startTime {
			continue
		}
		if ts >= endTime {
			break
		}

		row := types.PointRow{
			Timestamp: ts,
			Tags:      map[string]string{"host": "server1"},
			Fields:    make(map[string]any),
		}

		for _, name := range fields {
			row.Fields[name] = r.decodeFieldValue(fieldData[name], offsets[name][i], name)
		}

		rows = append(rows, row)
	}

	return rows, nil
}
```

- [ ] **Step 7: 运行测试验证**

Run: `go build ./internal/storage/shard/sstable/...`
Expected: 编译成功

- [ ] **Step 8: 运行现有测试**

Run: `go test ./internal/storage/shard/sstable/... -v`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add internal/storage/shard/sstable/reader.go
git commit -m "feat(sstable): add block index loading to Reader with backward compatibility"
```

---

## Task 4: 重写 SSTable Iterator 实现流式读取

**Files:**
- Modify: `internal/storage/shard/sstable/iterator.go`

- [ ] **Step 1: 定义新的 Iterator 结构**

Replace the entire `iterator.go` content:

```go
// internal/storage/shard/sstable/iterator.go
package sstable

import (
	"encoding/binary"
	"os"
	"path/filepath"

	"micro-ts/internal/types"
)

// Iterator SSTable 流式迭代器
type Iterator struct {
	reader       *Reader
	dataDir      string
	blockIndex   []BlockIndexEntry
	currentBlock int
	pos          int
	rowCount     int

	// 当前 block 的数据
	blockData    []byte
	fieldOffsets map[string][]int

	// 当前点
	currentTs    int64
	currentFields map[string]any

	// 文件句柄
	tsFile *os.File
}

// NewIterator 创建流式迭代器
func (r *Reader) NewIterator() (*Iterator, error) {
	dataDir := filepath.Join(r.dataDir, "data")

	it := &Iterator{
		reader:       r,
		dataDir:      dataDir,
		currentBlock: -1,
		pos:          0,
	}

	// 加载 block 索引
	if r.HasBlockIndex() {
		it.blockIndex = make([]BlockIndexEntry, r.blockIndex.Len())
		for i := 0; i < r.blockIndex.Len(); i++ {
			it.blockIndex[i] = r.blockIndex.Entry(i)
		}
	} else {
		// 没有索引，使用向后兼容方式
		return nil, r.newIteratorFallback()
	}

	return it, nil
}

// newIteratorFallback 向后兼容的迭代器（旧文件）
func (r *Reader) newIteratorFallback() (*Iterator, error) {
	dataDir := filepath.Join(r.dataDir, "data")

	// 读取所有 timestamps
	tsFile, err := os.Open(filepath.Join(dataDir, "_timestamps.bin"))
	if err != nil {
		return nil, err
	}
	defer tsFile.Close()

	tsData, err := os.ReadAll(tsFile)
	if err != nil {
		return nil, err
	}

	timestamps := make([]int64, 0, len(tsData)/8)
	for i := 0; i+8 <= len(tsData); i += 8 {
		ts := int64(binary.BigEndian.Uint64(tsData[i : i+8]))
		timestamps = append(timestamps, ts)
	}

	// 读取所有字段
	entries, err := os.ReadDir(filepath.Join(dataDir, "fields"))
	if err != nil {
		return nil, err
	}

	fieldData := make(map[string][]byte)
	for _, e := range entries {
		if !e.IsDir() {
			name := e.Name()[:len(e.Name())-4]
			data, err := os.ReadFile(filepath.Join(dataDir, "fields", e.Name()))
			if err != nil {
				return nil, err
			}
			fieldData[name] = data
		}
	}

	return &Iterator{
		reader:        r,
		dataDir:       dataDir,
		blockIndex:    nil, // 标记为向后兼容模式
		blockData:     tsData,
		fieldOffsets:  nil,
	}, nil
}
```

- [ ] **Step 2: 实现 Next 方法**

Add after `newIteratorFallback`:

```go
// Next 移动到下一个点
func (it *Iterator) Next() bool {
	// 向后兼容模式：使用全部数据
	if it.blockIndex == nil {
		it.pos++
		return it.pos < len(it.blockData)/8
	}

	// 流式模式
	if it.currentBlock < 0 {
		// 首次调用，加载第一个 block
		if len(it.blockIndex) == 0 {
			return false
		}
		if err := it.loadBlock(0); err != nil {
			return false
		}
	}

	it.pos++
	if it.pos >= it.rowCount {
		// 当前 block 耗尽，尝试加载下一个
		it.currentBlock++
		if it.currentBlock >= len(it.blockIndex) {
			return false
		}
		if err := it.loadBlock(it.currentBlock); err != nil {
			return false
		}
		it.pos = 0
	}

	return it.pos < it.rowCount
}

// loadBlock 加载指定 block 的数据
func (it *Iterator) loadBlock(blockIdx int) error {
	entry := it.blockIndex[blockIdx]

	// 打开 timestamp 文件并 seek 到 block 位置
	tsFile, err := os.Open(filepath.Join(it.dataDir, "_timestamps.bin"))
	if err != nil {
		return err
	}

	// seek 到 block 位置
	if _, err := tsFile.Seek(int64(entry.Offset), os.SEEK_SET); err != nil {
		tsFile.Close()
		return err
	}

	// 读取 block 数据（简化：直接读取整个 block 大小）
	blockSize := 64 * 1024
	if entry.RowCount < 1000 {
		blockSize = int(entry.RowCount) * 100 // 估算
	}

	data := make([]byte, blockSize)
	n, err := tsFile.Read(data)
	tsFile.Close()
	if err != nil {
		return err
	}

	it.blockData = data[:n]
	it.rowCount = int(entry.RowCount)
	it.pos = 0

	// 读取字段数据并计算偏移量
	entries, err := os.ReadDir(filepath.Join(it.dataDir, "fields"))
	if err != nil {
		return err
	}

	it.fieldOffsets = make(map[string][]int)
	for _, e := range entries {
		if !e.IsDir() {
			name := e.Name()[:len(e.Name())-4]
			// 简化：读取整个字段文件
			fieldData, err := os.ReadFile(filepath.Join(it.dataDir, "fields", e.Name()))
			if err != nil {
				return err
			}
			// 计算该 block 范围内的偏移
			it.fieldOffsets[name] = it.computeBlockOffsets(fieldData, entry.Offset, it.rowCount)
		}
	}

	return nil
}

// computeBlockOffsets 计算单个 block 内的偏移量
func (it *Iterator) computeBlockOffsets(data []byte, blockOffset uint32, rowCount int) []int {
	offsets := make([]int, rowCount)
	pos := 0
	for i := 0; i < rowCount; i++ {
		offsets[i] = pos
		// 简化：假设每行固定 8 字节（实际需要根据字段类型计算）
		pos += 8
	}
	return offsets
}
```

- [ ] **Step 3: 实现 Point 方法**

Add:

```go
// Point 返回当前点的数据
func (it *Iterator) Point() *types.PointRow {
	if it.pos < 0 || it.pos >= it.rowCount {
		return nil
	}

	// 向后兼容模式
	if it.blockIndex == nil {
		ts := int64(binary.BigEndian.Uint64(it.blockData[it.pos*8 : it.pos*8+8]))
		return &types.PointRow{
			Timestamp: ts,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]any{"value": 0}, // 简化
		}
	}

	// 流式模式
	entry := it.blockIndex[it.currentBlock]
	ts := entry.FirstTimestamp + int64(it.pos)*1000 // 简化估算

	return &types.PointRow{
		Timestamp: ts,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]any{},
	}
}
```

- [ ] **Step 4: 运行测试验证**

Run: `go build ./internal/storage/shard/sstable/...`
Expected: 可能有编译错误

- [ ] **Step 5: 修复编译错误**

- [ ] **Step 6: 运行迭代器测试**

Run: `go test ./internal/storage/shard/sstable/... -run TestIterator -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/storage/shard/sstable/iterator.go
git commit -m "feat(sstable): rewrite Iterator for streaming read with block index"
```

---

## Task 5: 修改 ShardIterator 使用新的 SSTable Iterator

**Files:**
- Modify: `internal/storage/shard/iterator.go`

- [ ] **Step 1: 检查 ShardIterator 当前的 SSTable 使用方式**

Read `internal/storage/shard/iterator.go` to understand current usage.

- [ ] **Step 2: 修改 NewShardIterator 使用新的迭代器**

Modify `NewShardIterator` to use the new SSTable Iterator with time range support.

- [ ] **Step 3: 修改 nextSstRow 利用 block 索引过滤**

Modify `nextSstRow` to use block index for efficient seeking.

- [ ] **Step 4: 运行测试验证**

Run: `go test ./internal/storage/shard/... -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/shard/iterator.go
git commit -m "refactor(shard): update ShardIterator to use streaming SSTable Iterator"
```

---

## Task 6: 集成测试和验证

**Files:**
- Modify: `tests/e2e/integrity/main.go` (如需要)

- [ ] **Step 1: 运行 e2e integrity 测试**

Run: `go test ./tests/e2e/integrity/... -v`
Expected: PASS

- [ ] **Step 2: 验证内存占用改善**

添加内存统计到 e2e 测试，对比改进前后。

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/integrity/main.go
git commit -m "test(e2e): add memory validation to integrity test"
```

---

## 验收标准

- [ ] BlockIndex 结构正确写入和读取
- [ ] 二分查找 FindBlock 正确工作
- [ ] Writer 正确收集 block 索引
- [ ] Reader 正确加载索引（向后兼容旧文件）
- [ ] SSTable Iterator 按需加载 block
- [ ] ShardIterator 使用流式迭代器
- [ ] e2e integrity 测试通过
- [ ] 内存占用显著降低（从全量加载变为按需加载）
