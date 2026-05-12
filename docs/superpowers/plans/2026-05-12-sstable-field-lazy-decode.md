# SSTable 字段惰性解码与查询截断 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 SSTable block 内字段解码从全量预加载改为惰性加载 + 字段投影 + ReadRange 早期截断。

**Architecture:** Iterator 新增 `blockFieldData`（解压缓存）和 `projectedFields`（字段投影），`loadBlock` 只解压不解码，`Point()` 首次访问时触发解码并缓存。`ReadRange` 传入截断行数，仅解码需要的行。

**Tech Stack:** Go 1.26, 纯标准库 + 内部 compression 包

---

## 文件变更图谱

```
internal/storage/shard/sstable/
  iterator.go          — Iterator struct + NewIterator 签名变更
  iterator_block.go    — loadBlock 拆分字段解码
  iterator_next.go     — Point() 惰性解码 + block 切换清理
  reader_range.go      — readRangeBlocks 早期截断
  iterator_test.go     — 字段投影测试
  iterator_extra_test.go — ReadRange 截断测试
  sstable_test.go      — NewIterator 调用适配 (nil)

internal/storage/compaction/
  merge.go             — NewIterator() → NewIterator(nil)
  level.go             — NewIterator() → NewIterator(nil)
  compaction_test.go   — NewIterator() → NewIterator(nil)

internal/storage/shard/
  compaction_test.go   — NewIterator() → NewIterator(nil)
```

---

### Task 1: Iterator struct 更新 + NewIterator 签名变更 + 所有调用方适配

**Files:**
- Modify: `internal/storage/shard/sstable/iterator.go:9-40`
- Modify: `internal/storage/shard/sstable/iterator_block.go:8-40`
- Modify: `internal/storage/shard/sstable/iterator_next.go:33-57`
- Modify: `internal/storage/compaction/merge.go:95`
- Modify: `internal/storage/compaction/level.go:314`
- Modify: `internal/storage/compaction/compaction_test.go:259,263,349,353`
- Modify: `internal/storage/shard/compaction_test.go:1194,1265,1479,1484`
- Modify: `internal/storage/shard/sstable/sstable_test.go:42,94,142,190,603,1051`
- Modify: `internal/storage/shard/sstable/iterator_test.go:33,82,149,208`
- Modify: `internal/storage/shard/sstable/iterator_extra_test.go:44,88,294,337,379,411,459,511,553,590,634,678,996`

- [ ] **Step 1: 更新 Iterator struct**

```go
// iterator.go
type Iterator struct {
	reader *Reader

	blockIndex   []BlockIndexEntry
	currentBlock int

	blockTimestamps  []int64
	blockSids        []uint64
	blockFieldData   map[string][]byte              // 解压后原始字节（惰性解码源）
	blockFieldValues map[string][]*types.FieldValue // 解码后缓存
	blockRowCount    int
	pos              int

	projectedFields []string // nil=全部字段
}
```

- [ ] **Step 2: 更新 NewIterator 签名和实现**

```go
// iterator.go — 替换 NewIterator
func (r *Reader) NewIterator(fields []string) (*Iterator, error) {
	it := &Iterator{
		reader:          r,
		currentBlock:    -1,
		pos:             -1,
		projectedFields: fields,
	}

	if r.blockIndex != nil {
		idx := r.blockIndex
		it.blockIndex = make([]BlockIndexEntry, idx.Len())
		for i := 0; i < idx.Len(); i++ {
			it.blockIndex[i] = idx.Entry(i)
		}
	}

	return it, nil
}
```

- [ ] **Step 3: 更新 loadBlock — timestamps + SIDs 立即解码，字段仅解压不解码**

```go
// iterator_block.go — 替换 loadBlock
func (it *Iterator) loadBlock(blockIdx int) error {
	if blockIdx < 0 || blockIdx >= len(it.blockIndex) {
		return nil
	}

	entry := it.blockIndex[blockIdx]
	it.currentBlock = blockIdx
	it.blockRowCount = int(entry.RowCount)

	ts, err := it.reader.readTimestampsBlock(blockIdx)
	if err != nil {
		return err
	}
	it.blockTimestamps = ts

	sids, err := it.reader.readSidsBlock(blockIdx)
	if err != nil {
		return err
	}
	it.blockSids = sids

	// 清除上一 block 的字段缓存
	it.blockFieldData = nil
	it.blockFieldValues = nil

	// 确定需要解压的字段
	fieldNames := it.projectedFields
	if fieldNames == nil {
		fieldNames = it.reader.sectionTable.FieldNames()
	}

	// 仅解压原始字节，不解码
	it.blockFieldData = make(map[string][]byte, len(fieldNames))
	for _, name := range fieldNames {
		data, err := it.reader.readFieldBlockRaw(name, blockIdx)
		if err != nil {
			return err
		}
		it.blockFieldData[name] = data
	}

	return nil
}
```

- [ ] **Step 4: 新增 Reader.readFieldBlockRaw — 仅解压不解码**

在 `reader_blocks.go` 末尾添加新方法，从 `decodeFieldSectionBlock` 中提取"读取+解压"部分：

```go
// reader_blocks.go — 新增
func (r *Reader) readFieldBlockRaw(name string, blockIdx int) ([]byte, error) {
	bso := r.blockSectionMap.Lookup(name)
	if bso == nil {
		return nil, nil
	}
	offset, size := bso.BlockRange(blockIdx)
	if size == 0 {
		return nil, nil
	}

	secOffset, _ := r.sectionTable.Lookup(name)
	data := make([]byte, size)
	if _, err := r.file.ReadAt(data, int64(secOffset+offset)); err != nil {
		return nil, err
	}

	comp := r.sectionTable.LookupCompression(name)
	return DecompressBlock(data, comp)
}
```

- [ ] **Step 5: 更新 Point() — 惰性解码字段**

```go
// iterator_next.go — 替换 Point
func (it *Iterator) Point() *types.PointRow {
	if it.currentBlock < 0 || it.currentBlock >= len(it.blockIndex) {
		return nil
	}
	if it.pos < 0 || it.pos >= it.blockRowCount || it.pos >= len(it.blockTimestamps) {
		return nil
	}

	row := &types.PointRow{
		Timestamp: it.blockTimestamps[it.pos],
		Fields:    make(map[string]*types.FieldValue),
	}
	if it.pos < len(it.blockSids) {
		row.Sid = it.blockSids[it.pos]
	}

	// 惰性解码：首次访问字段时解码全部行并缓存
	if it.blockFieldValues == nil {
		it.blockFieldValues = make(map[string][]*types.FieldValue)
	}
	for name, rawData := range it.blockFieldData {
		if _, ok := it.blockFieldValues[name]; !ok {
			vals, err := it.reader.decodeFieldSectionBlockFromData(name, rawData, it.blockRowCount)
			if err != nil {
				it.blockFieldValues[name] = nil
				continue
			}
			it.blockFieldValues[name] = vals
		}
		if vals := it.blockFieldValues[name]; vals != nil && it.pos < len(vals) {
			row.Fields[name] = vals[it.pos]
		}
	}

	return row
}
```

- [ ] **Step 6: 新增 Reader.decodeFieldSectionBlockFromData — 从已解压数据解码**

在 `reader_blocks.go` 末尾添加。从 `decodeFieldSectionBlock` 中提取解码逻辑（不含文件读取和解压）：

```go
// reader_blocks.go — 新增
func (r *Reader) decodeFieldSectionBlockFromData(name string, data []byte, rowCount int) ([]*types.FieldValue, error) {
	if data == nil {
		ft := r.schema.Fields[name]
		values := make([]*types.FieldValue, rowCount)
		for i := 0; i < rowCount; i++ {
			values[i] = zeroFieldValue(ft)
		}
		return values, nil
	}

	enc := r.sectionTable.LookupEncoding(name)
	ft := r.schema.Fields[name]

	switch enc {
	case EncodingXORFloat:
		floatVals, err := compression.DecodeFloat64Values(data, rowCount)
		if err != nil {
			return nil, fmt.Errorf("decode xor float field %s: %w", name, err)
		}
		return float64ValuesToFieldValues(floatVals), nil
	case EncodingZigZagVarint:
		intVals, err := compression.DecodeInt64Values(data, rowCount)
		if err != nil {
			return nil, fmt.Errorf("decode zigzag int field %s: %w", name, err)
		}
		return int64ValuesToFieldValues(intVals), nil
	case EncodingDictString:
		strVals, err := compression.DecodeStringValues(data, rowCount, true)
		if err != nil {
			return nil, fmt.Errorf("decode dict string field %s: %w", name, err)
		}
		return stringValuesToFieldValues(strVals), nil
	case EncodingBitmapBool:
		boolVals := compression.DecodeBoolValues(data, rowCount)
		return boolValuesToFieldValues(boolVals), nil
	default:
		return r.decodeRawFieldSection(data, rowCount, ft, name), nil
	}
}
```

需要添加 `"fmt"` 和 `compression` 的 import。

- [ ] **Step 7: 更新所有 NewIterator() 调用点为 NewIterator(nil)**

使用 sed 批量替换（编译阶段会确保无遗漏）：

```bash
# merge.go
sed -i 's/r\.NewIterator()/r.NewIterator(nil)/' internal/storage/compaction/merge.go

# level.go
sed -i 's/r\.NewIterator()/r.NewIterator(nil)/' internal/storage/compaction/level.go

# 所有测试文件
sed -i 's/\.NewIterator()/\.NewIterator(nil)/g' \
  internal/storage/compaction/compaction_test.go \
  internal/storage/shard/compaction_test.go \
  internal/storage/shard/sstable/sstable_test.go \
  internal/storage/shard/sstable/iterator_test.go \
  internal/storage/shard/sstable/iterator_extra_test.go
```

- [ ] **Step 8: 编译验证**

```bash
go build ./...
```
Expected: 编译成功

- [ ] **Step 9: 运行现有测试确认行为等价**

```bash
go test ./internal/storage/shard/sstable/... -count=1 -timeout 60s
go test ./internal/storage/compaction/... -count=1 -timeout 60s
go test ./internal/storage/shard/... -count=1 -timeout 60s
```
Expected: 全部 PASS

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "feat(sstable): 字段惰性解码 — NewIterator 支持字段投影，loadBlock 仅解压不解码"
```

---

### Task 2: Iterator 字段投影测试

**Files:**
- Modify: `internal/storage/shard/sstable/iterator_test.go`

- [ ] **Step 1: 添加字段投影测试**

在 `iterator_test.go` 末尾添加：

```go
func TestIterator_ProjectedFields(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := NewWriter(tmpDir, 1, 2, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1_000_000_000,
			Tags:      map[string]string{"host": "a"},
			Fields: map[string]*types.FieldValue{
				"cpu":  types.NewFieldValue(float64(1.5)),
				"mem":  types.NewFieldValue(float64(60.0)),
				"disk": types.NewFieldValue(float64(30.0)),
			},
		},
		{
			Timestamp: 2_000_000_000,
			Tags:      map[string]string{"host": "a"},
			Fields: map[string]*types.FieldValue{
				"cpu":  types.NewFieldValue(float64(2.0)),
				"mem":  types.NewFieldValue(float64(65.0)),
				"disk": types.NewFieldValue(float64(35.0)),
			},
		},
	}
	if err := w.WritePoints(pointsToInternalWithSids(points, nil)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sstPath := fmt.Sprintf("%s/data/sst_1.bin", tmpDir)
	r, err := NewReader(sstPath, w.Schema())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	// 仅投影 cpu 字段
	it, err := r.NewIterator([]string{"cpu"})
	if err != nil {
		t.Fatalf("NewIterator with fields failed: %v", err)
	}

	if !it.Next() {
		t.Fatal("expected first row")
	}
	row := it.Point()
	if row == nil {
		t.Fatal("expected non-nil row")
	}
	if _, ok := row.Fields["cpu"]; !ok {
		t.Error("expected cpu field")
	}
	if _, ok := row.Fields["mem"]; ok {
		t.Error("mem should not be present with field projection")
	}
	if _, ok := row.Fields["disk"]; ok {
		t.Error("disk should not be present with field projection")
	}
	if row.Fields["cpu"].GetFloatValue() != float64(1.5) {
		t.Errorf("expected cpu=1.5, got %v", row.Fields["cpu"])
	}

	// 验证缓存命中：第二次 Point() 也应该正常
	if !it.Next() {
		t.Fatal("expected second row")
	}
	row2 := it.Point()
	if row2.Fields["cpu"].GetFloatValue() != float64(2.0) {
		t.Errorf("expected cpu=2.0, got %v", row2.Fields["cpu"])
	}
}

func TestIterator_AllFieldsNil(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := NewWriter(tmpDir, 1, 2, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1_000_000_000,
			Tags:      map[string]string{"host": "a"},
			Fields: map[string]*types.FieldValue{
				"cpu": types.NewFieldValue(float64(1.0)),
				"mem": types.NewFieldValue(float64(60.0)),
			},
		},
	}
	if err := w.WritePoints(pointsToInternalWithSids(points, nil)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sstPath := fmt.Sprintf("%s/data/sst_1.bin", tmpDir)
	r, err := NewReader(sstPath, w.Schema())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	// nil = 全部字段
	it, err := r.NewIterator(nil)
	if err != nil {
		t.Fatalf("NewIterator(nil) failed: %v", err)
	}

	if !it.Next() {
		t.Fatal("expected row")
	}
	row := it.Point()
	if len(row.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(row.Fields))
	}
}
```

需要添加 `"fmt"` 和 `"codeberg.org/micro-ts/mts/types"` 的 import（检查现有 imports）。

- [ ] **Step 2: 运行新测试**

```bash
go test ./internal/storage/shard/sstable/... -run "TestIterator_ProjectedFields|TestIterator_AllFieldsNil" -v -count=1
```
Expected: 2 PASS

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/sstable/iterator_test.go
git commit -m "test(sstable): 添加 Iterator 字段投影测试"
```

---

### Task 3: ReadRange 早期截断

**Files:**
- Modify: `internal/storage/shard/sstable/reader_range.go:39-85`
- Modify: `internal/storage/shard/sstable/iterator_extra_test.go` (新增测试)

- [ ] **Step 1: 添加 ReadRange 截断测试**

在 `iterator_extra_test.go` 末尾添加：

```go
func TestReadRange_EarlyTermination(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := NewWriter(tmpDir, 1, 2, CompressionNone)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 写入 200 个数据点，全部在同一 block 内
	points := make([]*types.Point, 200)
	for i := 0; i < 200; i++ {
		points[i] = &types.Point{
			Timestamp: int64(i+1) * 1_000_000_000,
			Tags:      map[string]string{"host": "a"},
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(float64(i)),
			},
		}
	}
	if err := w.WritePoints(pointsToInternalWithSids(points, nil)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sstPath := fmt.Sprintf("%s/data/sst_1.bin", tmpDir)
	r, err := NewReader(sstPath, w.Schema())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	// 请求 LIMIT 10，验证仅返回 10 行
	rows, err := r.ReadRange(0, 0, 10)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if len(rows) != 10 {
		t.Errorf("expected 10 rows with maxRows=10, got %d", len(rows))
	}

	// 验证数据正确性（前 10 个点）
	for i, row := range rows {
		expectedVal := float64(i)
		got := row.Fields["value"].GetFloatValue()
		if got != expectedVal {
			t.Errorf("row[%d]: expected value=%f, got %f", i, expectedVal, got)
		}
	}
}
```

需要检查 imports（`"fmt"` 和 `types`）。

- [ ] **Step 2: 运行测试确认失败**

```bash
go test ./internal/storage/shard/sstable/... -run "TestReadRange_EarlyTermination" -v -count=1
```
Expected: PASS（当前逻辑也能通过，因为 maxRows 截断在行循环中已实现）。但字段解码量仍为全部 200 行，我们将在 Step 3 优化。

- [ ] **Step 3: 实现 readRangeBlocks 字段解码截断**

替换 `readRangeBlocks` 中字段解码循环，传入截断后的行数：

```go
// reader_range.go — 替换 readRangeBlocks
func (r *Reader) readRangeBlocks(matchingBlocks []int, startTime, endTime int64, fields []string, maxRows int) ([]*types.PointRow, error) {
	var rows []*types.PointRow

	for _, blockIdx := range matchingBlocks {
		timestamps, err := r.readTimestampsBlock(blockIdx)
		if err != nil {
			return nil, err
		}
		sids, err := r.readSidsBlock(blockIdx)
		if err != nil {
			return nil, err
		}

		entry := r.blockIndex.Entry(blockIdx)

		// 先扫描 timestamps 找出匹配行，确定需要解码的最大行号
		maxNeeded := 0
		matchCount := 0
		remaining := maxRows - len(rows)
		for i, ts := range timestamps {
			if ts >= startTime && (endTime <= 0 || ts < endTime) {
				if i+1 > maxNeeded {
					maxNeeded = i + 1
				}
				matchCount++
				if maxRows > 0 && matchCount >= remaining {
					break
				}
			}
		}
		if maxNeeded == 0 {
			continue
		}

		// 仅解码到最后一个匹配行的位置，而非全部 rowCount
		decodedFields := make(map[string][]*types.FieldValue, len(fields))
		for _, name := range fields {
			vals, err := r.decodeFieldSectionBlockUpTo(name, blockIdx, maxNeeded)
			if err != nil {
				return nil, err
			}
			decodedFields[name] = vals
		}

		// 逐行组装结果
		for i, ts := range timestamps {
			if ts >= startTime && (endTime <= 0 || ts < endTime) {
				row := &types.PointRow{
					Timestamp: ts,
					Tags:      nil,
					Fields:    make(map[string]*types.FieldValue),
				}
				if i < len(sids) {
					row.Sid = sids[i]
				}
				for _, name := range fields {
					if vals, ok := decodedFields[name]; ok && i < len(vals) {
						row.Fields[name] = vals[i]
					}
				}
				rows = append(rows, row)
				if maxRows > 0 && len(rows) >= maxRows {
					return rows, nil
				}
			}
		}
		_ = entry
	}

	return rows, nil
}
```

- [ ] **Step 4: 新增 decodeFieldSectionBlockUpTo 方法**

在 `reader_blocks.go` 末尾添加。该方法与 `decodeFieldSectionBlock` 类似，但 rowCount 可以小于 block 实际行数：

```go
// reader_blocks.go — 新增
func (r *Reader) decodeFieldSectionBlockUpTo(name string, blockIdx int, maxRow int) ([]*types.FieldValue, error) {
	bso := r.blockSectionMap.Lookup(name)
	if bso == nil {
		values := make([]*types.FieldValue, maxRow)
		for i := 0; i < maxRow; i++ {
			values[i] = zeroFieldValue(r.schema.Fields[name])
		}
		return values, nil
	}
	offset, size := bso.BlockRange(blockIdx)
	if size == 0 {
		values := make([]*types.FieldValue, maxRow)
		ft := r.schema.Fields[name]
		for i := 0; i < maxRow; i++ {
			values[i] = zeroFieldValue(ft)
		}
		return values, nil
	}

	secOffset, _ := r.sectionTable.Lookup(name)
	data := make([]byte, size)
	if _, err := r.file.ReadAt(data, int64(secOffset+offset)); err != nil {
		return nil, err
	}

	comp := r.sectionTable.LookupCompression(name)
	var decErr error
	data, decErr = DecompressBlock(data, comp)
	if decErr != nil {
		return nil, fmt.Errorf("decompress field %s block %d: %w", name, blockIdx, decErr)
	}

	enc := r.sectionTable.LookupEncoding(name)
	switch enc {
	case EncodingXORFloat:
		floatVals, err := compression.DecodeFloat64Values(data, maxRow)
		if err != nil {
			return nil, fmt.Errorf("decode xor float field %s: %w", name, err)
		}
		return float64ValuesToFieldValues(floatVals), nil
	case EncodingZigZagVarint:
		intVals, err := compression.DecodeInt64Values(data, maxRow)
		if err != nil {
			return nil, fmt.Errorf("decode zigzag int field %s: %w", name, err)
		}
		return int64ValuesToFieldValues(intVals), nil
	case EncodingDictString:
		strVals, err := compression.DecodeStringValues(data, maxRow, true)
		if err != nil {
			return nil, fmt.Errorf("decode dict string field %s: %w", name, err)
		}
		return stringValuesToFieldValues(strVals), nil
	case EncodingBitmapBool:
		boolVals := compression.DecodeBoolValues(data, maxRow)
		return boolValuesToFieldValues(boolVals), nil
	default:
		ft := r.schema.Fields[name]
		return r.decodeRawFieldSection(data, maxRow, ft, name), nil
	}
}
```

- [ ] **Step 5: 运行全部 sstable 测试**

```bash
go test ./internal/storage/shard/sstable/... -count=1 -timeout 60s
```
Expected: 全部 PASS

- [ ] **Step 6: Commit**

```bash
git add internal/storage/shard/sstable/reader_range.go internal/storage/shard/sstable/reader_blocks.go internal/storage/shard/sstable/iterator_extra_test.go
git commit -m "feat(sstable): ReadRange 字段解码早期截断，LIMIT 下推到位"
```

---

### Task 4: 全量验证 + E2E

- [ ] **Step 1: 运行全量单元测试**

```bash
go test ./... -count=1 -timeout 300s
```
Expected: 全部 PASS

- [ ] **Step 2: 运行 golangci-lint**

```bash
golangci-lint run ./...
```
Expected: 0 issues

- [ ] **Step 3: 运行 goimports-reviser**

```bash
goimports-reviser -format ./internal/storage/shard/sstable/...
goimports-reviser -format ./internal/storage/compaction/...
```

- [ ] **Step 4: 构建并运行 E2E 测试**

```bash
# 关键 E2E 测试
cd tests/e2e/compression_test && go build -o ct . && ./ct; rm -f ct
cd tests/e2e/compaction_test && go build -o ct . && timeout 120 ./ct; rm -f ct
cd tests/e2e/integrity && go build -o it . && timeout 120 ./it; rm -f it
cd tests/e2e/simple_integrity && go build -o si . && ./si; rm -f si
cd tests/e2e/restart_recovery && go build -o rr . && ./rr; rm -f rr
```
Expected: 全部 PASS

- [ ] **Step 5: 清理临时文件并最终 commit**

```bash
git add -A
git commit -m "chore(sstable): 全量验证通过，E2E 通过"
```
