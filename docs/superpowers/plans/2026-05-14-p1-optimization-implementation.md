# P1 优化项实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将代码检视报告中 3 个 P1 改进项落地实现：Tombstone SID 索引、WriteBatch 批量写入、SSTable 字符串段流式读取+字典编码。

**Architecture:** 三个独立工作流按依赖排序。P1-1 在 TombstoneSet 中加 SID→[]Tombstone map 索引（不入盘）。P1-2 在 Engine.WriteBatch 中按 Shard 分组后走 Shard.WriteBatch → WAL.WriteBatch 批量路径。P1-3 将 writer_close.go 中字符串段的 os.ReadFile 全量加载替换为逐块 io.ReadFull 流式读取，同时接入已有的 DictEncode 字典编码库，per-block 增加 1 字节格式标志。

**Tech Stack:** Go 1.22+, container/heap, encoding/binary, sync.RWMutex, JSON (tombstones)

---

### Task 1: TombstoneSet — 新增 SID 索引字段与构建方法

**Files:**
- Modify: `internal/storage/compaction/tombstone.go:19-22,25-33`

- [ ] **Step 1: 在 TombstoneSet 中新增 index 字段和 BuildIndex 方法**

在 `TombstoneSet` 结构体增加不入盘的 SID 索引字段，添加 `BuildIndex()` 方法：

```go
// TombstoneSet 表示一组删除标记。
type TombstoneSet struct {
	Tombstones []Tombstone `json:"tombstones"`
	index      map[uint64][]Tombstone // SID → 匹配的 tombstones（运行时索引，不入盘）
}

// BuildIndex 构建 SID 索引，用于加速 ShouldDelete 查找。
// 调用方在 collectInputTombstones 之后必须调用此方法。
func (ts *TombstoneSet) BuildIndex() {
	if len(ts.Tombstones) == 0 {
		ts.index = nil
		return
	}
	ts.index = make(map[uint64][]Tombstone, len(ts.Tombstones))
	for _, t := range ts.Tombstones {
		ts.index[t.SID] = append(ts.index[t.SID], t)
	}
}
```

- [ ] **Step 2: 修改 ShouldDelete 使用索引**

```go
// ShouldDelete 检查给定的 (sid, timestamp) 是否应被删除。
func (ts *TombstoneSet) ShouldDelete(sid uint64, timestamp int64) bool {
	if ts.index == nil {
		// 未构建索引，回退线性扫描（测试兼容）
		for i := range ts.Tombstones {
			t := &ts.Tombstones[i]
			if t.SID == sid && timestamp >= t.MinTime && timestamp <= t.MaxTime {
				return true
			}
		}
		return false
	}
	list := ts.index[sid]
	for i := range list {
		t := &list[i]
		if timestamp >= t.MinTime && timestamp <= t.MaxTime {
			return true
		}
	}
	return false
}
```

- [ ] **Step 3: 运行现有测试验证不改功能行为**

```bash
go test ./internal/storage/compaction/ -run TestTombstone -v -count=1
```
预期: 所有 TestTombstone* 测试 PASS

- [ ] **Step 4: Commit**

```bash
git add internal/storage/compaction/tombstone.go
git commit -m "perf(compaction): TombstoneSet 新增 SID 索引加速 ShouldDelete 查找"
```

---

### Task 2: 调用方 — collectInputTombstones 后 BuildIndex

**Files:**
- Modify: `internal/storage/compaction/merge.go:81`
- Modify: `internal/storage/compaction/level.go:310`

- [ ] **Step 1: 在 merge.go 的 collectInputTombstones 调用后添加 BuildIndex**

```go
// merge.go:81-82 改为：
tombstones := collectInputTombstones(task.InputFiles)
tombstones.BuildIndex()
```

- [ ] **Step 2: 在 level.go 的 collectInputTombstones 调用后添加 BuildIndex**

```go
// level.go:310-311 改为：
tombstones := collectInputTombstones(inputPaths)
tombstones.BuildIndex()
```

- [ ] **Step 3: 新增索引正确性测试**

在 `internal/storage/compaction/tombstone_test.go` 末尾追加：

```go
func TestTombstoneSet_BuildIndex(t *testing.T) {
	ts := &TombstoneSet{
		Tombstones: []Tombstone{
			{SID: 1, MinTime: 100, MaxTime: 200, DeletedAt: 300},
			{SID: 1, MinTime: 500, MaxTime: 600, DeletedAt: 300},
			{SID: 2, MinTime: 150, MaxTime: 250, DeletedAt: 300},
			{SID: 3, MinTime: 0, MaxTime: 1000, DeletedAt: 300},
		},
	}
	ts.BuildIndex()

	tests := []struct {
		name      string
		sid       uint64
		timestamp int64
		want      bool
	}{
		{"match first of same SID", 1, 100, true},
		{"match second of same SID", 1, 550, true},
		{"gap between same SID ranges", 1, 350, false},
		{"different SID match", 2, 200, true},
		{"SID with single range", 3, 500, true},
		{"SID not in index", 99, 100, false},
		{"empty set with index", 0, 0, false}, // tested via nil index
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ts.ShouldDelete(tt.sid, tt.timestamp)
			if got != tt.want {
				t.Errorf("ShouldDelete(%d, %d) = %v, want %v", tt.sid, tt.timestamp, got, tt.want)
			}
		})
	}
}

func TestTombstoneSet_BuildIndex_Empty(t *testing.T) {
	ts := &TombstoneSet{}
	ts.BuildIndex()
	if ts.ShouldDelete(1, 100) {
		t.Error("empty indexed TombstoneSet.ShouldDelete should return false")
	}
}
```

- [ ] **Step 4: 运行测试**

```bash
go test ./internal/storage/compaction/ -run TestTombstone -v -count=1
```
预期: 所有 TestTombstone* 测试 PASS（含新增的 TestTombstoneSet_BuildIndex*）

- [ ] **Step 5: Commit**

```bash
git add internal/storage/compaction/merge.go internal/storage/compaction/level.go internal/storage/compaction/tombstone_test.go
git commit -m "perf(compaction): collectInputTombstones 后构建 SID 索引"
```

---

### Task 3: Shard — 新增 WriteBatch 方法

**Files:**
- Modify: `internal/storage/shard/shard_io.go:31-90`

- [ ] **Step 1: 添加 Shard.WriteBatch 方法**

在 `shard_io.go` 的 `Write` 方法之后新增 `WriteBatch`：

```go
// WriteBatch 批量写入数据点到 Shard，使用单次锁获取 + 单次 WAL 批量写入。
//
// 与多次调用 Write 的区别：
//   - 只获取一次 Shard 锁（减少锁竞争）
//   - 通过 WAL.WriteBatch 批量持久化（减少 fsync 次数）
//
// 参数：
//   - points: 要写入的数据点切片
//
// 返回：
//   - int: 成功写入的点数
//   - error: 首个失败点的错误
func (s *Shard) WriteBatch(points []*types.Point) (int, error) {
	if len(points) == 0 {
		return 0, nil
	}

	// 背压检查
	for s.memTable.ActiveFull() {
		if !s.memTable.IsFlushing() {
			s.tryTriggerAsyncFlush()
		}
		time.Sleep(time.Millisecond)
		if s.closed.Load() {
			return 0, fmt.Errorf("shard closed during backpressure wait")
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 预序列化所有 point
	ips := make([]types.InternalPoint, 0, len(points))
	walData := make([][]byte, 0, len(points))

	for i, point := range points {
		sid, err := s.seriesStore.AllocateSID(point.Tags)
		if err != nil {
			return i, fmt.Errorf("allocate SID for point %d: %w", i, err)
		}
		if err := s.ValidateFieldTypes(point); err != nil {
			return i, fmt.Errorf("validate field types for point %d: %w", i, err)
		}
		ip := types.PointToInternal(point, sid)
		ips = append(ips, ip)

		if s.wal != nil {
			data, err := serializeInternalPoint(ip)
			if err != nil {
				return i, fmt.Errorf("serialize point %d: %w", i, err)
			}
			walData = append(walData, data)
		}
	}

	// 批量写入 WAL
	if s.wal != nil && len(walData) > 0 {
		if _, err := s.wal.WriteBatch(walData); err != nil {
			return 0, fmt.Errorf("wal write batch: %w", err)
		}
	}

	// 批量写入 MemTable
	for i, ip := range ips {
		if err := s.memTable.Write(ip); err != nil {
			return i, fmt.Errorf("write to memtable at %d: %w", i, err)
		}
	}

	// 检查是否需要异步 flush
	shouldFlush := s.memTable.ShouldSwap()

	if shouldFlush {
		s.tryTriggerAsyncFlush()
	}

	return len(ips), nil
}
```

- [ ] **Step 2: 运行现有测试确保持编译通过且无回归**

```bash
go build ./internal/storage/shard/...
go test ./internal/storage/shard/ -run TestShard -v -count=1 -timeout 60s
```
预期: 编译通过，现有 TestShard* 测试 PASS

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/shard_io.go
git commit -m "feat(shard): 新增 WriteBatch 方法支持批量写入单次锁获取"
```

---

### Task 4: Engine — WriteBatch 按 Shard 分组批量写入

**Files:**
- Modify: `internal/engine/engine_write.go:59-73`

- [ ] **Step 1: 重写 Engine.WriteBatch 使用分组批量写入**

将 `engine_write.go` 中的 `WriteBatch` 方法替换为：

```go
// WriteBatch 批量写入数据点。
//
// 优化策略：按 Shard 分组后对每组调用 Shard.WriteBatch，
// 减少锁获取次数并利用 WAL 批量写入减少 fsync。
//
// 批量写入不是原子操作，部分失败不会回滚已写入的点。
func (e *Engine) WriteBatch(ctx context.Context, points []*types.Point) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if e.isClosed() {
		return fmt.Errorf("engine is closed")
	}

	if len(points) == 0 {
		return nil
	}

	// 验证并自动创建 database/measurement，按 *Shard 分组
	groups := make(map[*shard.Shard][]*types.Point)

	for _, p := range points {
		if p == nil {
			return ErrNilPoint
		}
		if p.Database == "" {
			return ErrEmptyDatabase
		}
		if p.Measurement == "" {
			return ErrEmptyMeasurement
		}
		if p.Timestamp < 0 {
			return ErrInvalidTimestamp
		}

		cat := e.manager.Catalog()
		if !cat.DatabaseExists(p.Database) {
			if err := cat.CreateDatabase(p.Database); err != nil {
				slog.Warn("auto-create database failed", "database", p.Database, "error", err)
			}
		}
		if !cat.MeasurementExists(p.Database, p.Measurement) {
			if err := cat.CreateMeasurement(p.Database, p.Measurement); err != nil {
				slog.Warn("auto-create measurement failed", "database", p.Database, "measurement", p.Measurement, "error", err)
			}
		}

		s, err := e.shardManager.GetShard(p.Database, p.Measurement, p.Timestamp)
		if err != nil {
			return fmt.Errorf("get shard: %w", err)
		}

		groups[s] = append(groups[s], p)
	}

	// 对每组调用 Shard.WriteBatch
	for s, group := range groups {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := s.WriteBatch(group)
		if err != nil {
			return fmt.Errorf("write batch to shard: wrote %d/%d: %w", n, len(group), err)
		}
	}

	return nil
}
```

需要在 `engine_write.go` 顶部 import 中添加 `"codeberg.org/micro-ts/mts/internal/storage/shard"`。

- [ ] **Step 3: 运行编译和测试**

```bash
go build ./internal/engine/...
go test ./internal/engine/ -v -count=1 -timeout 60s
```
预期: 编译通过，engine 测试 PASS

- [ ] **Step 4: Commit**

```bash
git add internal/engine/engine_write.go
git commit -m "perf(engine): WriteBatch 按 Shard 分组批量写入减少锁竞争"
```

---

### Task 5: Writer — 记录字段块级字节偏移（flushBlock 阶段）

**Files:**
- Modify: `internal/storage/shard/sstable/writer.go:30-53` (Writer struct)
- Modify: `internal/storage/shard/sstable/writer_close.go:17-51` (flushBlock)

- [ ] **Step 1: 在 Writer struct 中新增 fieldByteOffsets 字段**

在 `writer.go` 的 `Writer` struct 中，紧接 `fieldBufs` 和 `fieldSizes` 之后添加：

```go
	fieldBufs       map[string][]byte
	fieldSizes      map[string]int
	fieldByteOffsets map[string][]int64 // 每个 block 在各 field temp 文件中的字节起始偏移
```

同时在 `NewWriter` 函数中（writer.go:85-102），初始化该字段：

```go
		w := &Writer{
			// ... 现有字段 ...
			fieldBufs:        make(map[string][]byte),
			fieldSizes:       make(map[string]int),
			fieldByteOffsets: make(map[string][]int64),
			// ...
		}
```

- [ ] **Step 2: 在 flushBlock 中记录每个字段的当前文件字节偏移**

修改 `flushBlock()` 中写入字段数据的部分（writer_close.go:35-40）：

```go
	for name, buf := range w.fieldBufs {
		// 记录此 block 在 temp 文件中的字节起始偏移
		curOff, err := w.fields[name].Seek(0, io.SeekCurrent)
		if err != nil {
			curOff = 0
		}
		w.fieldByteOffsets[name] = append(w.fieldByteOffsets[name], curOff)

		if _, err := w.fields[name].Write(buf); err != nil {
			return fmt.Errorf("write field block %s: %w", name, err)
		}
		w.fieldBufs[name] = w.fieldBufs[name][:0]
	}
```

需要在 `writer_close.go` 顶部已有 import 中添加 `"io"`（已存在无需额外操作）。

- [ ] **Step 3: 运行现有测试验证 flushBlock 改动无回归**

```bash
go test ./internal/storage/shard/sstable/ -run TestWriter -v -count=1 -timeout 60s
```
预期: TestWriter* 测试 PASS

- [ ] **Step 4: Commit**

```bash
git add internal/storage/shard/sstable/writer.go internal/storage/shard/sstable/writer_close.go
git commit -m "feat(sstable): flushBlock 记录字段块级字节偏移用于流式读取"
```

---

### Task 6: Writer — 实现字符串段逐块流式编码 + 接入 DictEncode

**Files:**
- Modify: `internal/storage/shard/sstable/writer_close.go:317-327` (encodeFieldSection string case)
- Add: 新函数 `encodeStringFieldSection`

- [ ] **Step 1: 新增 encodeStringFieldSection 流式编码函数**

在 `encodeFieldSection` 函数之后（writer_close.go:337 之前）插入新函数：

```go
// encodeStringFieldSection 逐块流式编码字符串字段段（变长类型）。
//
// 与 encodeFixedFieldSection 相似，使用 fieldByteOffsets 中记录的字节偏移
// 逐块从 temp 文件中读取原始字符串数据，避免全量 os.ReadFile。
// 每块独立进行字典编码（有收益时自动启用），prepend 1 字节格式标志。
func (w *Writer) encodeStringFieldSection(name string, rowCount int) ([]byte, []uint64, EncodingType, error) {
	rawPath := filepath.Join(w.tmpDir, "fields", name+".bin")
	f, err := os.Open(rawPath)
	if err != nil {
		return nil, nil, EncodingRaw, fmt.Errorf("open field temp %s: %w", rawPath, err)
	}
	defer func() { _ = f.Close() }()

	byteOffsets := w.fieldByteOffsets[name]
	if len(byteOffsets) != w.blockIndex.Len() {
		return nil, nil, EncodingRaw, fmt.Errorf("field %s: byte offset count %d != block count %d",
			name, len(byteOffsets), w.blockIndex.Len())
	}

	var encoded []byte
	offsets := make([]uint64, 0, w.blockIndex.Len()+1)
	off := uint64(0)
	offsets = append(offsets, off)

	for i := 0; i < w.blockIndex.Len(); i++ {
		entry := w.blockIndex.Entry(i)
		n := int(entry.RowCount)

		// 定位到该 block 的字节偏移
		if _, err := f.Seek(byteOffsets[i], io.SeekStart); err != nil {
			return nil, nil, EncodingRaw, fmt.Errorf("seek field %s block %d: %w", name, i, err)
		}

		// 计算该 block 的字节大小
		var blockSize int64
		if i+1 < len(byteOffsets) {
			blockSize = byteOffsets[i+1] - byteOffsets[i]
		} else {
			fi, err := f.Stat()
			if err != nil {
				return nil, nil, EncodingRaw, fmt.Errorf("stat field %s temp: %w", name, err)
			}
			blockSize = fi.Size() - byteOffsets[i]
		}

		raw := make([]byte, blockSize)
		if _, err := io.ReadFull(f, raw); err != nil {
			return nil, nil, EncodingRaw, fmt.Errorf("read field %s block %d: %w", name, i, err)
		}

		values := compression.ExtractStringData(raw, n)
		blockData, isDict := compression.EncodeStringValues(values)

		// prepend 1 字节格式标志: 0=raw, 1=dict
		var flag byte
		if isDict {
			flag = 1
		}
		blockData = append([]byte{flag}, blockData...)

		compressed, _ := CompressBlock(blockData, w.compressAlgo)
		encoded = append(encoded, compressed...)
		off += uint64(len(compressed))
		offsets = append(offsets, off)
	}

	return encoded, offsets, EncodingDictString, nil
}
```

- [ ] **Step 2: 修改 encodeFieldSection 的 string case**

将 writer_close.go 中 `case FieldTypeString:`（行 317-327）改为调用新函数：

```go
		case FieldTypeString:
			return w.encodeStringFieldSection(name, rowCount)
```

- [ ] **Step 3: 运行编译确认无语法错误**

```bash
go build ./internal/storage/shard/sstable/...
```
预期: 编译通过

- [ ] **Step 4: Commit**

```bash
git add internal/storage/shard/sstable/writer_close.go
git commit -m "feat(sstable): 字符串段改用逐块流式编码并接入 DictEncode 字典压缩"
```

---

### Task 7: Reader — 更新 EncodingDictString 解码支持 per-block 格式标志

**Files:**
- Modify: `internal/storage/shard/sstable/reader_blocks.go:244-249` (decodeFieldSectionBlock 的 EncodingDictString case)

- [ ] **Step 1: 读取当前 reader_blocks.go 中 EncodingDictString 的处理代码**

```bash
grep -n -A 5 "EncodingDictString" internal/storage/shard/sstable/reader_blocks.go
```

查看 `decodeFieldSectionBlock` 函数（约 244 行）和 block 数据读取上下文，理解压缩数据如何传入解码函数。

- [ ] **Step 2: 修改 EncodingDictString 解码逻辑**

将当前：
```go
	case EncodingDictString:
		strVals, err := compression.DecodeStringValues(data, rowCount, true)
		if err != nil {
			return nil, fmt.Errorf("decode dict string field %s: %w", name, err)
		}
		return stringValuesToFieldValues(strVals), nil
```

改为检查 per-block 1 字节格式标志：
```go
	case EncodingDictString:
		if len(data) < 1 {
			return nil, fmt.Errorf("decode dict string field %s: empty block data", name)
		}
		isDict := data[0] == 1
		strVals, err := compression.DecodeStringValues(data[1:], rowCount, isDict)
		if err != nil {
			return nil, fmt.Errorf("decode dict string field %s: %w", name, err)
		}
		return stringValuesToFieldValues(strVals), nil
```

- [ ] **Step 3: 检查其他 EncodingDictString 处理位置**

```bash
grep -n -B 2 -A 8 "EncodingDictString" internal/storage/shard/sstable/reader_blocks.go
```

对 reader_blocks.go 第 335 行和第 375 行附近的 `EncodingDictString` 处理做同样修改。

- [ ] **Step 4: 运行现有测试验证 reader 改动无回归**

```bash
go test ./internal/storage/shard/sstable/ -v -count=1 -timeout 60s
```
预期: 所有测试 PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/shard/sstable/reader_blocks.go
git commit -m "feat(sstable): reader 支持 per-block 字典编码格式标志"
```

---

### Task 8: SSTable 字符串编码端到端测试

**Files:**
- Test file (use existing): `internal/storage/shard/sstable/merge_iterator_test.go` 或创建新测试

- [ ] **Step 1: 新增字符串字典编码写入-读取往返测试**

在 `internal/storage/shard/sstable/` 下已有 `merge_iterator_test.go`，追加测试：

```go
func TestWriter_DictEncodingRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// 低基数字符串：字典编码应有收益
	points := make([]types.InternalPoint, 200)
	statuses := []string{"ok", "error", "timeout", "ok", "ok", "error"}
	for i := range points {
		points[i] = types.InternalPoint{
			Timestamp: int64((i + 1) * 100),
			Sid:       uint64(i % 10),
			Fields: []types.InternalField{
				{Key: "status", Value: types.NewFieldValue(statuses[i%len(statuses)])},
			},
		}
	}

	w, err := NewWriter(dir, 0, 512, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WritePoints(points); err != nil {
		t.Fatalf("WritePoints: %v", err)
	}
	schema := w.Schema()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 验证 schema 正确记录
	if ft, ok := schema.Fields["status"]; !ok || ft != FieldTypeString {
		t.Fatalf("expected string field 'status', got %v", schema.Fields)
	}

	// 读取验证
	r, err := NewReader(filepath.Join(dir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}

	var count int
	for it.Next() {
		row := it.Point()
		if row.Fields["status"] == nil {
			t.Errorf("row %d missing status field", count)
		}
		count++
	}
	if count != 200 {
		t.Errorf("expected 200 rows, got %d", count)
	}
}

func TestWriter_DictEncodingLargeDataset(t *testing.T) {
	dir := t.TempDir()

	// 大数据集：10000 行低基数字符串，验证 streaming 不 OOM
	points := make([]types.InternalPoint, 10000)
	values := []string{"a", "b", "c", "d", "a", "b"}
	for i := range points {
		points[i] = types.InternalPoint{
			Timestamp: int64((i + 1) * 100),
			Sid:       uint64(i % 50),
			Fields: []types.InternalField{
				{Key: "label", Value: types.NewFieldValue(values[i%len(values)])},
			},
		}
	}

	w, err := NewWriter(dir, 0, 64*1024, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WritePoints(points); err != nil {
		t.Fatalf("WritePoints: %v", err)
	}
	schema := w.Schema()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 完整读取验证行数
	r, err := NewReader(filepath.Join(dir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}

	count := 0
	for it.Next() {
		count++
	}
	if count != 10000 {
		t.Errorf("expected 10000 rows, got %d", count)
	}
}

func TestWriter_DictEncodingFallback(t *testing.T) {
	dir := t.TempDir()

	// 高基数随机字符串：字典编码应自动回退为 raw
	points := make([]types.InternalPoint, 100)
	for i := range points {
		points[i] = types.InternalPoint{
			Timestamp: int64((i + 1) * 100),
			Sid:       uint64(i),
			Fields: []types.InternalField{
				{Key: "uuid", Value: types.NewFieldValue(fmt.Sprintf("id-%d-%x", i, i*37))},
			},
		}
	}

	w, err := NewWriter(dir, 0, 64*1024, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WritePoints(points); err != nil {
		t.Fatalf("WritePoints: %v", err)
	}
	schema := w.Schema()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(filepath.Join(dir, "data", "sst_0.bin"), schema)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = r.Close() }()

	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}

	count := 0
	for it.Next() {
		row := it.Point()
		if row.Fields["uuid"] == nil {
			t.Errorf("row %d missing uuid field", count)
		}
		count++
	}
	if count != 100 {
		t.Errorf("expected 100 rows, got %d", count)
	}
}
```

注意：需要在测试文件顶部 import 中已有 `"fmt"` 和 `"path/filepath"`。

- [ ] **Step 2: 运行新测试**

```bash
go test ./internal/storage/shard/sstable/ -run TestWriter_Dict -v -count=1 -timeout 60s
```
预期: 3 个新测试 PASS

- [ ] **Step 3: 运行全部 sstable 测试确认无回归**

```bash
go test ./internal/storage/shard/sstable/ -v -count=1 -timeout 120s
```
预期: 全部测试 PASS

- [ ] **Step 4: Commit**

```bash
git add internal/storage/shard/sstable/merge_iterator_test.go
git commit -m "test(sstable): 新增字符串字典编码往返与大数据集流式测试"
```

---

### Task 9: 集成验证 — Shard WriteBatch 端到端测试

**Files:**
- Modify: `internal/storage/shard/shard_extra_test.go` (追加测试)

- [ ] **Step 1: 新增 WriteBatch 测试**

在 `shard_extra_test.go` 末尾追加：

```go
func TestShard_WriteBatch(t *testing.T) {
	dir := t.TempDir()
	shard := createTestShard(t, dir)

	points := make([]*types.Point, 50)
	for i := range points {
		points[i] = &types.Point{
			Timestamp: int64((i + 1) * 100),
			Tags:      map[string]string{"host": "srv1"},
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(float64(i))},
		}
	}

	n, err := shard.WriteBatch(points)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if n != 50 {
		t.Errorf("wrote %d, want 50", n)
	}

	// 验证数据可读
	iter := NewShardIterator(shard, 0, 0, 0)
	defer iter.Close()

	count := 0
	for iter.Next() != nil {
		count++
	}
	if count != 50 {
		t.Errorf("read %d rows, want 50", count)
	}
}

func TestShard_WriteBatch_EmptyInput(t *testing.T) {
	dir := t.TempDir()
	shard := createTestShard(t, dir)

	n, err := shard.WriteBatch(nil)
	if err != nil {
		t.Fatalf("WriteBatch(nil): %v", err)
	}
	if n != 0 {
		t.Errorf("wrote %d, want 0", n)
	}

	n, err = shard.WriteBatch([]*types.Point{})
	if err != nil {
		t.Fatalf("WriteBatch([]): %v", err)
	}
	if n != 0 {
		t.Errorf("wrote %d, want 0", n)
	}
}
```

- [ ] **Step 2: 运行测试**

```bash
go test ./internal/storage/shard/ -run TestShard_WriteBatch -v -count=1 -timeout 60s
```
预期: 2 个新测试 PASS

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/shard_extra_test.go
git commit -m "test(shard): 新增 WriteBatch 批量写入测试"
```

---

### Task 10: 终极验证 — golangci-lint + 全部单元测试 + e2e 测试

- [ ] **Step 1: 运行 golangci-lint**

```bash
golangci-lint run ./...
```
预期: 0 issues

- [ ] **Step 2: 运行全项目单元测试**

```bash
go test ./... -count=1 -timeout 300s 2>&1 | tail -50
```
预期: 全部 PASS

- [ ] **Step 3: 运行关键 e2e 测试**

```bash
cd tests/e2e/write_and_compact && go build -o test_bin . && ./test_bin && rm -f test_bin
cd tests/e2e/grpc_write_query && go build -o test_bin . && ./test_bin && rm -f test_bin
cd tests/e2e/simple_integrity && go build -o test_bin . && ./test_bin && rm -f test_bin
```
预期: 3/3 e2e PASS

- [ ] **Step 4: Commit（如有 lint/格式化修复）**

```bash
git add -A
git commit -m "chore: golangci-lint 格式化与最终验证"
```

---

## 依赖关系与执行顺序

```
P1-1 (Tombstone)         P1-2 (WriteBatch)        P1-3 (String Streaming)
Task 1 ──────────────────────────────────────────────────────────────────
  ↓                        Task 3                    Task 5
Task 2                       ↓                         ↓
  ✓                        Task 4                    Task 6
                             ↓                         ↓
                           Task 9                    Task 7
                             ↓                         ↓
                             ✓                       Task 8
                                                       ↓
                                                       ✓

所有工作流在 Task 10 汇合 ─── 终极验证
```

- P1-1 和 P1-2 的初期任务（1-2, 3-4）可并行执行
- P1-3 任务（5-8）依赖 SSTable 格式理解较深，建议串行
- Task 9 依赖 Task 3 完成
- Task 10 在所有任务完成后执行

---

## 风险与注意事项

1. **P1-1 Tombstone index 不入盘**：`index` 字段是运行时结构，JSON 序列化时自动忽略（小写非导出字段）。`collectInputTombstones` 后必须调用 `BuildIndex`。
2. **P1-2 Shard.ID() 方法**：若 Shard 没有公开的 ID 方法，分组 key 需用 Shard 引用或调整实现方式。
3. **P1-3 per-block 格式标志**：旧格式 SSTable 文件（EncodingRaw 的字符串段）与新格式（EncodingDictString + 1字节标志）不兼容。旧 reader 读新文件会报错，新 reader 读旧文件走 EncodingRaw 路径不受影响。
4. **P1-3 fieldByteOffsets 清理**：Writer 复用场景需确认 fieldByteOffsets 在每次 Close 后正确重置（当前 Writer 不可复用，无需额外处理）。
