# SSTable Block-Level Compression Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add configurable per-block compression (Snappy/LZ4) to SSTable writes, applied after type-specific encoding, with format-version backward compatibility.

**Architecture:** After each block's data is encoded (Delta/XOR/ZigZag), compress the encoded bytes with the configured algorithm and prepend a 4-byte uncompressed-length header, then write to the final SSTable file. The SectionTable records which compression (if any) was used per section. On read, use the 4-byte header to size the decompression buffer, then decompress block data before type-specific decoding. Compression is configured once at engine creation and propagates down through shard config to the SSTable writer.

**Tech Stack:** Go 1.26, `github.com/golang/snappy`, `github.com/pierrec/lz4/v4`

---

### Task 1: Add compression dependencies

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: Add snappy and promote lz4 to direct dependency**

```bash
cd /root/projects/mts && go get github.com/golang/snappy && go get github.com/pierrec/lz4/v4@v4.1.26
```

Run: `go mod tidy`
Expected: `go.mod` shows both as `require` (direct).

- [ ] **Step 2: Verify build works with new deps**

```bash
cd /root/projects/mts && go build ./...
```
Expected: build succeeds.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add snappy dependency, promote lz4 to direct"
```

---

### Task 2: Define CompressionAlgorithm type

**Files:**
- Create: `internal/storage/shard/sstable/compress.go`

- [ ] **Step 1: Create compress.go**

```go
package sstable

import (
	"encoding/binary"
	"fmt"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

// CompressionAlgorithm 通用块压缩算法。
type CompressionAlgorithm uint8

const (
	CompressionNone   CompressionAlgorithm = 0 // 无压缩（默认）
	CompressionSnappy CompressionAlgorithm = 1 // Snappy 压缩
	CompressionLZ4    CompressionAlgorithm = 2 // LZ4 压缩
)

// String 返回算法名称。
func (c CompressionAlgorithm) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionSnappy:
		return "snappy"
	case CompressionLZ4:
		return "lz4"
	default:
		return fmt.Sprintf("unknown(%d)", c)
	}
}

// CompressBlock 压缩已编码的 block 数据。
// 压缩后格式: [uncompressedLen:4B BigEndian][compressed_data]
// 对于 CompressionNone，直接返回原始数据（无 header）。
func CompressBlock(data []byte, algo CompressionAlgorithm) ([]byte, error) {
	switch algo {
	case CompressionNone:
		return data, nil
	case CompressionSnappy:
		encoded := snappy.Encode(nil, data)
		result := make([]byte, 4+len(encoded))
		binary.BigEndian.PutUint32(result[:4], uint32(len(data)))
		copy(result[4:], encoded)
		return result, nil
	case CompressionLZ4:
		buf := make([]byte, lz4.CompressBlockBound(len(data)))
		n, err := lz4.CompressBlock(data, buf, nil)
		if err != nil {
			return nil, fmt.Errorf("lz4 compress: %w", err)
		}
		result := make([]byte, 4+n)
		binary.BigEndian.PutUint32(result[:4], uint32(len(data)))
		copy(result[4:], buf[:n])
		return result, nil
	default:
		return nil, fmt.Errorf("unknown compression algorithm: %d", algo)
	}
}

// DecompressBlock 解压 CompressBlock 压缩的数据。
// algo=CompressionNone 时直接返回原始数据。
func DecompressBlock(data []byte, algo CompressionAlgorithm) ([]byte, error) {
	switch algo {
	case CompressionNone:
		return data, nil
	case CompressionSnappy:
		if len(data) < 4 {
			return nil, fmt.Errorf("snappy data too short: %d bytes", len(data))
		}
		origLen := binary.BigEndian.Uint32(data[:4])
		decoded, err := snappy.Decode(nil, data[4:])
		if err != nil {
			return nil, fmt.Errorf("snappy decode: %w", err)
		}
		_ = origLen // snappy 自带长度验证
		return decoded, nil
	case CompressionLZ4:
		if len(data) < 4 {
			return nil, fmt.Errorf("lz4 data too short: %d bytes", len(data))
		}
		origLen := binary.BigEndian.Uint32(data[:4])
		decoded := make([]byte, origLen)
		n, err := lz4.UncompressBlock(data[4:], decoded)
		if err != nil {
			return nil, fmt.Errorf("lz4 decode: %w", err)
		}
		return decoded[:n], nil
	default:
		return nil, fmt.Errorf("unknown compression algorithm: %d", algo)
	}
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/storage/shard/sstable/compress.go
git commit -m "feat(sstable): add CompressionAlgorithm type and compress/decompress helpers"
```

---

### Task 3: Update SectionEntry format for compression

**Files:**
- Modify: `internal/storage/shard/sstable/format.go`

- [ ] **Step 1: Add Compression field to SectionEntry**

```go
type SectionEntry struct {
	Type        SectionType
	Name        string
	Offset      uint64
	Size        uint64
	Encoding    EncodingType
	Compression CompressionAlgorithm
}
```

- [ ] **Step 2: Update sectionEntrySize from 19 → 20**

```go
const sectionEntrySize = 20
```

- [ ] **Step 3: Update Marshal() — insert compression byte**

The per-entry write becomes:

```go
for _, e := range st.Entries {
	buf = append(buf, byte(e.Type), byte(e.Encoding), byte(e.Compression), byte(len(e.Name)))
	var off [8]byte
	binary.BigEndian.PutUint64(off[:], e.Offset)
	buf = append(buf, off[:]...)
	binary.BigEndian.PutUint64(off[:], e.Size)
	buf = append(buf, off[:]...)
	if len(e.Name) > 0 {
		buf = append(buf, e.Name...)
	}
}
```

- [ ] **Step 4: Update UnmarshalSectionTable() — read compression byte, adjust offsets**

```go
e := SectionEntry{
	Type:        SectionType(data[pos]),
	Encoding:    EncodingType(data[pos+1]),
	Compression: CompressionAlgorithm(data[pos+2]),
	Name:        "",
	Offset:      binary.BigEndian.Uint64(data[pos+4 : pos+12]),
	Size:        binary.BigEndian.Uint64(data[pos+12 : pos+20]),
}
nameLen := int(data[pos+3])
pos += sectionEntrySize
```

- [ ] **Step 5: Add LookupCompression to SectionTable**

```go
// LookupCompression 按名称查找段压缩算法。未找到返回 CompressionNone。
func (st *SectionTable) LookupCompression(name string) CompressionAlgorithm {
	for _, e := range st.Entries {
		if e.Name == name {
			return e.Compression
		}
	}
	return CompressionNone
}
```

- [ ] **Step 6: Commit**

```bash
git add internal/storage/shard/sstable/format.go
git commit -m "feat(sstable): add Compression field to SectionEntry format"
```

---

### Task 4: Add compressAlgo to Writer and NewWriter

**Files:**
- Modify: `internal/storage/shard/sstable/writer.go`

- [ ] **Step 1: Add compressAlgo field to Writer**

```go
type Writer struct {
	shardDir   string
	seq        uint64
	blockSize  int
	dataDir    string
	tmpDir     string
	timestamp  *os.File
	sids       *os.File
	fields     map[string]*os.File
	schema     Schema
	blockIndex *BlockIndex

	buf       []byte
	bufPos    int
	firstTs   int64
	rowCount  uint32
	totalRows uint32

	sidBuf     []uint64
	fieldBufs  map[string][]byte
	fieldSizes map[string]int

	compressAlgo CompressionAlgorithm
}
```

- [ ] **Step 2: Update NewWriter signature**

```go
func NewWriter(shardDir string, seq uint64, blockSize int, compressAlgo CompressionAlgorithm) (*Writer, error) {
```

Set the field in initialization:

```go
w := &Writer{
	// ... existing fields ...
	compressAlgo: compressAlgo,
}
```

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/sstable/writer.go
git commit -m "feat(sstable): add compressAlgo parameter to Writer and NewWriter"
```

---

### Task 5: Apply compression in writer_close.go

**Files:**
- Modify: `internal/storage/shard/sstable/writer_close.go`

- [ ] **Step 1: Update encodePerBlock to compress after encoding**

After `encodeFn(values[start:end])` produces `blockData`, call `CompressBlock`:

```go
func encodePerBlock[T any](w *Writer, values []T, encodeFn func([]T) []byte) ([]byte, []uint64) {
	var encoded []byte
	offsets := make([]uint64, 0, w.blockIndex.Len()+1)
	offset := uint64(0)
	offsets = append(offsets, offset)
	for i := 0; i < w.blockIndex.Len(); i++ {
		entry := w.blockIndex.Entry(i)
		start := int(entry.Offset)
		end := start + int(entry.RowCount)
		blockData := encodeFn(values[start:end])
		compressed, _ := CompressBlock(blockData, w.compressAlgo)
		encoded = append(encoded, compressed...)
		offset += uint64(len(compressed))
		offsets = append(offsets, offset)
	}
	return encoded, offsets
}
```

- [ ] **Step 2: Update encodePerBlockRaw similarly**

```go
func encodePerBlockRaw(w *Writer, raw []byte, rowCount int) ([]byte, []uint64) {
	var encoded []byte
	offsets := make([]uint64, 0, w.blockIndex.Len()+1)
	offset := uint64(0)
	offsets = append(offsets, offset)

	bytesPerRow := len(raw) / rowCount

	for i := 0; i < w.blockIndex.Len(); i++ {
		entry := w.blockIndex.Entry(i)
		start := int(entry.Offset) * bytesPerRow
		end := start + int(entry.RowCount)*bytesPerRow
		blockData := raw[start:end]
		compressed, _ := CompressBlock(blockData, w.compressAlgo)
		encoded = append(encoded, compressed...)
		offset += uint64(len(compressed))
		offsets = append(offsets, offset)
	}
	return encoded, offsets
}
```

- [ ] **Step 3: Update Close() SectionTable entries**

Data sections get `w.compressAlgo`, metadata sections get `CompressionNone`:

```go
sectionTable := SectionTable{
	Entries: []SectionEntry{
		{Type: SectionTimestamps, Name: "_timestamps", Offset: timestampsOffset, Size: timestampsSize, Encoding: tsEncoding, Compression: w.compressAlgo},
		{Type: SectionSids, Name: "_sids", Offset: sidsOffset, Size: sidsSize, Encoding: EncodingVarint, Compression: w.compressAlgo},
		{Type: SectionIndex, Name: "_index", Offset: blockIndexOffset, Size: uint64(len(indexData)), Encoding: EncodingRaw, Compression: CompressionNone},
		{Type: SectionIndex, Name: "_block_map", Offset: blockMapOffset, Size: uint64(len(blockMapData)), Encoding: EncodingRaw, Compression: CompressionNone},
	},
}
for _, name := range fieldNames {
	fi := fieldInfoMap[name]
	sectionTable.Entries = append(sectionTable.Entries, SectionEntry{
		Type: SectionField, Name: name, Offset: fi.offset, Size: fi.size, Encoding: fi.encoding, Compression: w.compressAlgo,
	})
}
```

- [ ] **Step 4: Commit**

```bash
git add internal/storage/shard/sstable/writer_close.go
git commit -m "feat(sstable): apply block compression after encoding in writer close"
```

---

### Task 6: Decompress on read path

**Files:**
- Modify: `internal/storage/shard/sstable/reader_blocks.go`

- [ ] **Step 1: Update readTimestampsBlock to decompress**

After reading `data` from file and before decoding, add:

```go
compression := r.sectionTable.LookupCompression("_timestamps")
data, err := DecompressBlock(data, compression)
if err != nil {
	return nil, fmt.Errorf("decompress timestamps block %d: %w", blockIdx, err)
}
```

- [ ] **Step 2: Update readSidsBlock to decompress**

Same pattern — add after ReadAt, before DecodeSidsDelta:

```go
compression := r.sectionTable.LookupCompression("_sids")
data, err := DecompressBlock(data, compression)
if err != nil {
	return nil, fmt.Errorf("decompress sids block %d: %w", blockIdx, err)
}
```

- [ ] **Step 3: Update decodeFieldSectionBlock to decompress**

After reading `data` from file and before the encoding `switch`, add:

```go
compression := r.sectionTable.LookupCompression(name)
data, err := DecompressBlock(data, compression)
if err != nil {
	return nil, fmt.Errorf("decompress field %s block %d: %w", name, blockIdx, err)
}
```

- [ ] **Step 4: Commit**

```bash
git add internal/storage/shard/sstable/reader_blocks.go
git commit -m "feat(sstable): decompress blocks on read path"
```

---

### Task 7: Propagate compression config through the chain

**Files:**
- Modify: `mts.go`
- Modify: `internal/engine/engine.go`
- Modify: `internal/storage/shard/manager.go`
- Modify: `internal/storage/shard/shard.go`

- [ ] **Step 1: Add to public Config (mts.go)**

Add import `"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"` and field:

```go
type Config struct {
	DataDir                string
	ShardDuration          time.Duration
	MemTableCfg            *types.MemTableConfig
	CompactionCfg          *types.CompactionConfig
	CompressionAlgorithm   sstable.CompressionAlgorithm // 新增
	RetentionPeriod        time.Duration
	RetentionCheckInterval time.Duration
}
```

Pass in `Open()`:

```go
eng, err := engine.New(&engine.Config{
	// ... existing fields ...
	CompressionAlgorithm:   cfg.CompressionAlgorithm,
})
```

- [ ] **Step 2: Add to engine.Config and pass to ShardManager**

```go
type Config struct {
	// ... existing fields ...
	CompressionAlgorithm   sstable.CompressionAlgorithm
}
```

```go
engine := &Engine{
	cfg:          cfg,
	shardManager: shard.NewShardManager(cfg.DataDir, cfg.ShardDuration, memTableCfg, cfg.CompactionCfg, mgr, cfg.CompressionAlgorithm),
	manager:      mgr,
}
```

- [ ] **Step 3: Add to ShardManager and pass to ShardConfig**

```go
type ShardManager struct {
	// ... existing fields ...
	compressionAlgo        sstable.CompressionAlgorithm
}
```

Update `NewShardManager` signature to accept `compressionAlgo sstable.CompressionAlgorithm`.

In `GetShard`, `discoverShardsLocked`, `loadShardFromIndex`, add to ShardConfig:

```go
CompressionAlgorithm: m.compressionAlgo,
```

- [ ] **Step 4: Add to ShardConfig and Shard struct**

```go
type ShardConfig struct {
	// ... existing fields ...
	CompressionAlgorithm sstable.CompressionAlgorithm
}
```

In Shard struct, add field and set in `NewShard()`:

```go
type Shard struct {
	// ... existing fields ...
	compressionAlgo sstable.CompressionAlgorithm
}

func (s *Shard) CompressionAlgorithm() sstable.CompressionAlgorithm {
	return s.compressionAlgo
}
```

- [ ] **Step 5: Commit**

```bash
git add mts.go internal/engine/engine.go internal/storage/shard/manager.go internal/storage/shard/shard.go
git commit -m "feat(config): propagate CompressionAlgorithm from public Config to Shard"
```

---

### Task 8: Update production callers + ShardAccess interface

**Files:**
- Modify: `internal/storage/shard/shard_flush.go`
- Modify: `internal/storage/shard/shard_lifecycle.go`
- Modify: `internal/storage/compaction/merge.go`
- Modify: `internal/storage/compaction/level.go`
- Modify: `internal/storage/compaction/shard_access.go`

- [ ] **Step 1: Update shard_flush.go line 52**

```go
w, err := sstable.NewWriter(s.dir, sstSeq, 0, s.compressionAlgo)
```

- [ ] **Step 2: Update shard_lifecycle.go line 71**

```go
w, wErr := sstable.NewWriter(s.dir, s.sstSeq, 0, s.compressionAlgo)
```

- [ ] **Step 3: Add to ShardAccess interface**

```go
type ShardAccess interface {
	Dir() string
	DataDir() string
	NextSSTSeq() uint64
	IsSSTUnused(path string) bool
	GetSchema() (sstable.Schema, error)
	CompressionAlgorithm() sstable.CompressionAlgorithm
	AcquireSSTRef(path string) bool
	ReleaseSSTRef(path string)
}
```

- [ ] **Step 4: Update merge.go line 88**

```go
w, err := sstable.NewWriter(cm.ShardAccess.Dir(), outputSeq, 0, cm.ShardAccess.CompressionAlgorithm())
```

- [ ] **Step 5: Update level.go line 316**

```go
w, err := sstable.NewWriter(lcm.shard.Dir(), seq, 0, lcm.shard.CompressionAlgorithm())
```

- [ ] **Step 6: Commit**

```bash
git add internal/storage/shard/shard_flush.go internal/storage/shard/shard_lifecycle.go internal/storage/compaction/shard_access.go internal/storage/compaction/merge.go internal/storage/compaction/level.go
git commit -m "feat(compression): pass CompressionAlgorithm to all production NewWriter calls"
```

---

### Task 9: Update all test callers of NewWriter

**Files:**
- Modify: `internal/storage/shard/sstable/sstable_test.go`
- Modify: `internal/storage/shard/sstable/writer_test.go`
- Modify: `internal/storage/shard/sstable/reader_test.go`
- Modify: `internal/storage/shard/sstable/iterator_test.go`
- Modify: `internal/storage/shard/sstable/iterator_extra_test.go`
- Modify: `internal/storage/shard/sstable/reader_bench_test.go`
- Modify: `internal/storage/shard/iterator_test.go`
- Modify: `internal/storage/shard/level_compaction_e2e_test.go`
- Modify: `internal/storage/compaction/compaction_test.go`

- [ ] **Step 1: Batch replace NewWriter calls in test files**

```bash
cd /root/projects/mts

# Pattern 1: NewWriter(xxx, N, 0) → NewWriter(xxx, N, 0, CompressionNone)
grep -rln 'NewWriter(' --include='*_test.go' internal/ | while read f; do
  sed -i 's/NewWriter(\([^,]*\), \([^,]*\), 0)/NewWriter(\1, \2, 0, CompressionNone)/g' "$f"
done

# Pattern 2: NewWriter(xxx, N, sstable.BlockSize) → NewWriter(xxx, N, sstable.BlockSize, CompressionNone)
grep -rln 'NewWriter(' --include='*_test.go' internal/ | while read f; do
  sed -i 's/NewWriter(\([^,]*\), \([^,]*\), sstable\.BlockSize)/NewWriter(\1, \2, sstable.BlockSize, CompressionNone)/g' "$f"
done
```

- [ ] **Step 2: Manually verify any remaining unmatched calls**

```bash
grep -rn 'NewWriter(' --include='*_test.go' internal/ | grep -v CompressionNone
```

Fix any remaining calls manually.

- [ ] **Step 3: Verify build and tests**

```bash
cd /root/projects/mts && go build ./... && go test ./internal/storage/shard/sstable/... ./internal/storage/shard/... ./internal/storage/compaction/... -count=1 -timeout 120s
```

- [ ] **Step 4: Commit**

```bash
git add internal/
git commit -m "test: update all NewWriter callers to pass CompressionNone"
```

---

### Task 10: Add unit tests for compress.go

**Files:**
- Create: `internal/storage/shard/sstable/compress_test.go`

- [ ] **Step 1: Create compress_test.go**

```go
package sstable

import (
	"bytes"
	"testing"
)

func TestCompressDecompress_Roundtrip_AllAlgorithms(t *testing.T) {
	input := make([]byte, 4096)
	for i := range input {
		input[i] = byte(i % 64) // 高度可压缩
	}

	algos := []CompressionAlgorithm{CompressionNone, CompressionSnappy, CompressionLZ4}
	for _, algo := range algos {
		t.Run(algo.String(), func(t *testing.T) {
			compressed, err := CompressBlock(input, algo)
			if err != nil {
				t.Fatalf("CompressBlock failed: %v", err)
			}
			decoded, err := DecompressBlock(compressed, algo)
			if err != nil {
				t.Fatalf("DecompressBlock failed: %v", err)
			}
			if !bytes.Equal(decoded, input) {
				t.Fatalf("roundtrip mismatch for %s: len=%d vs %d", algo, len(input), len(decoded))
			}
		})
	}
}

func TestCompressBlock_None_ReturnsOriginal(t *testing.T) {
	input := []byte("hello world test data")
	result, err := CompressBlock(input, CompressionNone)
	if err != nil {
		t.Fatalf("CompressBlock None failed: %v", err)
	}
	if !bytes.Equal(result, input) {
		t.Fatal("CompressionNone should return original data unchanged")
	}
}

func TestDecompressBlock_InvalidData(t *testing.T) {
	_, err := DecompressBlock([]byte("ab"), CompressionSnappy)
	if err == nil {
		t.Fatal("expected error for invalid snappy data")
	}
	_, err = DecompressBlock([]byte("ab"), CompressionLZ4)
	if err == nil {
		t.Fatal("expected error for invalid lz4 data")
	}
}

func TestCompressBlock_UnknownAlgorithm(t *testing.T) {
	_, err := CompressBlock([]byte("data"), CompressionAlgorithm(99))
	if err == nil {
		t.Fatal("expected error for unknown algorithm")
	}
}

func TestDecompressBlock_UnknownAlgorithm(t *testing.T) {
	_, err := DecompressBlock([]byte("data"), CompressionAlgorithm(99))
	if err == nil {
		t.Fatal("expected error for unknown algorithm")
	}
}

func TestCompressBlock_LZ4_OutputFormat(t *testing.T) {
	input := []byte("repeating data repeating data repeating data")
	result, err := CompressBlock(input, CompressionLZ4)
	if err != nil {
		t.Fatalf("CompressBlock LZ4 failed: %v", err)
	}
	if len(result) < 4 {
		t.Fatal("lz4 result too short, missing size header")
	}
}

func TestCompressBlock_Snappy_OutputFormat(t *testing.T) {
	input := []byte("repeating data repeating data repeating data")
	result, err := CompressBlock(input, CompressionSnappy)
	if err != nil {
		t.Fatalf("CompressBlock Snappy failed: %v", err)
	}
	if len(result) < 4 {
		t.Fatal("snappy result too short, missing size header")
	}
}
```

- [ ] **Step 2: Run tests**

```bash
cd /root/projects/mts && go test ./internal/storage/shard/sstable/... -run TestCompress -v -count=1
```

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/sstable/compress_test.go
git commit -m "test(sstable): add unit tests for CompressBlock/DecompressBlock"
```

---

### Task 11: Add SSTable write+read roundtrip tests with compression

**Files:**
- Modify: `internal/storage/shard/sstable/sstable_test.go`

- [ ] **Step 1: Add TestWriter_Compression_Roundtrip**

```go
func TestWriter_Compression_Roundtrip(t *testing.T) {
	algos := []CompressionAlgorithm{CompressionNone, CompressionSnappy, CompressionLZ4}

	for _, algo := range algos {
		t.Run(algo.String(), func(t *testing.T) {
			tmpDir := t.TempDir()

			w, err := NewWriter(tmpDir, 0, 0, algo)
			if err != nil {
				t.Fatalf("NewWriter failed: %v", err)
			}

			now := int64(1000000000)
			points := []types.InternalPoint{
				{Timestamp: now, Sid: 1, Fields: map[string]*types.FieldValue{
					"value": types.NewFieldValue(float64(1.5)),
					"count": types.NewFieldValue(int64(10)),
				}},
				{Timestamp: now + 1000, Sid: 2, Fields: map[string]*types.FieldValue{
					"value": types.NewFieldValue(float64(2.5)),
					"count": types.NewFieldValue(int64(20)),
				}},
				{Timestamp: now + 2000, Sid: 3, Fields: map[string]*types.FieldValue{
					"value": types.NewFieldValue(float64(3.5)),
					"count": types.NewFieldValue(int64(30)),
				}},
			}

			if err := w.WritePoints(points); err != nil {
				t.Fatalf("WritePoints failed: %v", err)
			}
			schema := w.Schema()

			if err := w.Close(); err != nil {
				t.Fatalf("Close failed: %v", err)
			}

			reader, err := NewReader(filepath.Join(tmpDir, "data", "sst_0.bin"), schema)
			if err != nil {
				t.Fatalf("NewReader failed: %v", err)
			}
			defer reader.Close()

			rows, err := reader.ReadAll(nil)
			if err != nil {
				t.Fatalf("ReadAll failed: %v", err)
			}

			if len(rows) != 3 {
				t.Fatalf("expected 3 rows, got %d", len(rows))
			}

			if rows[0].Timestamp != now {
				t.Errorf("rows[0].Timestamp: expected %d, got %d", now, rows[0].Timestamp)
			}
			if rows[2].Timestamp != now+2000 {
				t.Errorf("rows[2].Timestamp: expected %d, got %d", now+2000, rows[2].Timestamp)
			}

			val := rows[1].Fields["value"]
			if val == nil {
				t.Fatal("missing field 'value'")
			}
			if val.GetFloatValue() != 2.5 {
				t.Errorf("float value: expected 2.5, got %f", val.GetFloatValue())
			}
		})
	}
}
```

- [ ] **Step 2: Run the test**

```bash
cd /root/projects/mts && go test ./internal/storage/shard/sstable/... -run TestWriter_Compression_Roundtrip -v -count=1
```

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/sstable/sstable_test.go
git commit -m "test(sstable): add roundtrip test for all compression algorithms"
```

---

### Task 12: Update e2e framework to support compression

**Files:**
- Modify: `tests/e2e/pkg/framework/framework.go`

- [ ] **Step 1: Add CompressionAlgorithm to framework Config**

```go
type Config struct {
	DBName                  string
	MeasurementName         string
	ShardDuration           time.Duration
	MaxSize                 int64
	MaxCount                int32
	IdleDurationNanos       int64
	RetentionPeriod         time.Duration
	RetentionCheckInterval  time.Duration
	CompactionMaxParts      int
	CompactionCheckInterval time.Duration
	CompressionAlgorithm    sstable.CompressionAlgorithm // 新增
}
```

Add import: `"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"`

- [ ] **Step 2: Pass CompressionAlgorithm through in NewTestHarness**

In `NewTestHarness`, update the `dbCfg`:

```go
dbCfg := microts.Config{
	DataDir:                tmpDir,
	ShardDuration:          cfg.ShardDuration,
	CompressionAlgorithm:   cfg.CompressionAlgorithm, // 新增
	MemTableCfg: &microts.MemTableConfig{
		MaxSize:           cfg.MaxSize,
		MaxCount:          cfg.MaxCount,
		IdleDurationNanos: cfg.IdleDurationNanos,
	},
	CompactionCfg:          compCfg,
	RetentionPeriod:        cfg.RetentionPeriod,
	RetentionCheckInterval: cfg.RetentionCheckInterval,
}
```

- [ ] **Step 3: Add WithCompressionAlgorithm option**

```go
// WithCompressionAlgorithm 设置 SSTable 块压缩算法。
func WithCompressionAlgorithm(algo sstable.CompressionAlgorithm) func(*Config) {
	return func(c *Config) {
		c.CompressionAlgorithm = algo
	}
}
```

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/pkg/framework/framework.go
git commit -m "feat(framework): add CompressionAlgorithm support to e2e test harness"
```

---

### Task 13: Create e2e compression test

**Files:**
- Create: `tests/e2e/compression_test/main.go`

- [ ] **Step 1: Create directory**

```bash
mkdir -p /root/projects/mts/tests/e2e/compression_test
```

- [ ] **Step 2: Write main.go**

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/tests/e2e/pkg/framework"
)

func main() {
	tests := []struct {
		name  string
		algo  sstable.CompressionAlgorithm
		count int
	}{
		{"none_1k", sstable.CompressionNone, 1000},
		{"snappy_10k", sstable.CompressionSnappy, 10000},
		{"lz4_10k", sstable.CompressionLZ4, 10000},
	}

	failed := false

	for _, tt := range tests {
		fmt.Printf("\n=== Test: %s (count=%d) ===\n", tt.name, tt.count)
		if err := runTest(tt.name, tt.algo, tt.count); err != nil {
			log.Printf("FAIL %s: %v", tt.name, err)
			failed = true
		} else {
			fmt.Printf("PASS %s\n", tt.name)
		}
	}

	fmt.Printf("\n=== Test: restart_recovery (lz4) ===\n")
	if err := runRestartTest(); err != nil {
		log.Printf("FAIL restart_recovery: %v", err)
		failed = true
	} else {
		fmt.Printf("PASS restart_recovery\n")
	}

	if failed {
		os.Exit(1)
	}
	fmt.Println("\nAll tests passed.")
}

func runTest(name string, algo sstable.CompressionAlgorithm, count int) error {
	h, err := framework.NewTestHarness(name,
		framework.WithMaxCount(int32(count+100)),
		framework.WithIdleDuration(5*time.Second),
		framework.WithCompressionAlgorithm(algo),
	)
	if err != nil {
		return fmt.Errorf("create harness: %w", err)
	}
	defer h.Close()

	ctx := context.Background()
	if err := h.WritePoints(ctx, count, 1*time.Microsecond); err != nil {
		return fmt.Errorf("write points: %w", err)
	}

	// Wait for flush
	time.Sleep(3 * time.Second)

	// Verify data integrity
	if err := h.VerifyDataIntegrity(count, 1*time.Microsecond); err != nil {
		return fmt.Errorf("data integrity: %w", err)
	}

	diskUsage := h.DiskUsage()
	sstCount := h.SSTableCount()
	fmt.Printf("  Rows: %d, Disk: %d bytes, SSTables: %d\n", count, diskUsage, sstCount)
	return nil
}

func runRestartTest() error {
	tmpDir, err := os.MkdirTemp("", "microts_comp_restart")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	h, err := framework.NewTestHarness("restart_test",
		framework.WithConfig(&framework.Config{
			DBName:               "db1",
			MeasurementName:      "cpu",
			ShardDuration:        time.Hour,
			MaxSize:              64 * 1024 * 1024,
			MaxCount:             3000,
			IdleDurationNanos:    int64(5 * time.Second),
			CompressionAlgorithm: sstable.CompressionLZ4,
		}),
	)
	if err != nil {
		return fmt.Errorf("create harness: %w", err)
	}

	ctx := context.Background()
	count := 1000
	if err := h.WritePoints(ctx, count, 1*time.Microsecond); err != nil {
		h.Close()
		return fmt.Errorf("write points: %w", err)
	}
	time.Sleep(3 * time.Second)

	if err := h.Close(); err != nil {
		return fmt.Errorf("close harness: %w", err)
	}

	// Reopen with LZ4
	h2, err := framework.NewTestHarness("restart_test2",
		framework.WithConfig(&framework.Config{
			DBName:               "db1",
			MeasurementName:      "cpu",
			ShardDuration:        time.Hour,
			MaxSize:              64 * 1024 * 1024,
			MaxCount:             3000,
			IdleDurationNanos:    int64(5 * time.Second),
			CompressionAlgorithm: sstable.CompressionLZ4,
		}),
	)
	if err != nil {
		return fmt.Errorf("reopen harness: %w", err)
	}
	defer h2.Close()

	time.Sleep(2 * time.Second)

	if err := h2.VerifyDataIntegrity(count, 1*time.Microsecond); err != nil {
		return fmt.Errorf("data integrity after restart: %w", err)
	}

	fmt.Printf("  Restart recovery OK: %d rows verified\n", count)
	return nil
}
```

- [ ] **Step 2: Build and run e2e test**

```bash
cd /root/projects/mts/tests/e2e/compression_test && go build -o /tmp/comp_test && /tmp/comp_test
```

Expected: all tests PASS. Then:

```bash
rm -f /tmp/comp_test
```

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/compression_test/main.go
git commit -m "test(e2e): add compression e2e tests for snappy/lz4/none with restart"
```

---

### Task 14: Format, lint, and final verification

- [ ] **Step 1: Run goimports-reviser**

```bash
cd /root/projects/mts && goimports-reviser -format ./...
```

- [ ] **Step 2: Run golangci-lint**

```bash
cd /root/projects/mts && golangci-lint run ./...
```

Fix any issues.

- [ ] **Step 3: Run all unit tests**

```bash
cd /root/projects/mts && go test ./... -count=1 -timeout 180s
```

- [ ] **Step 4: Run e2e test**

```bash
cd /root/projects/mts/tests/e2e/compression_test && go build && ./compression_test && rm -f compression_test
```

- [ ] **Step 5: Commit formatting fixes if needed**

```bash
git add -A && git diff --cached --quiet || git commit -m "chore: format and lint fixes"
```

---

### Summary of all commits

| # | Commit message | Key files |
|---|---------------|-----------|
| 1 | `chore: add snappy dependency, promote lz4 to direct` | go.mod, go.sum |
| 2 | `feat(sstable): add CompressionAlgorithm type and compress/decompress helpers` | compress.go |
| 3 | `feat(sstable): add Compression field to SectionEntry format` | format.go |
| 4 | `feat(sstable): add compressAlgo parameter to Writer and NewWriter` | writer.go |
| 5 | `feat(sstable): apply block compression after encoding in writer close` | writer_close.go |
| 6 | `feat(sstable): decompress blocks on read path` | reader_blocks.go |
| 7 | `feat(config): propagate CompressionAlgorithm from public Config to Shard` | mts.go, engine.go, manager.go, shard.go |
| 8 | `feat(compression): pass CompressionAlgorithm to all production NewWriter calls` | shard_flush.go, shard_lifecycle.go, shard_access.go, merge.go, level.go |
| 9 | `test: update all NewWriter callers to pass CompressionNone` | *_test.go files |
| 10 | `test(sstable): add unit tests for CompressBlock/DecompressBlock` | compress_test.go |
| 11 | `test(sstable): add roundtrip test for all compression algorithms` | sstable_test.go |
| 12 | `feat(framework): add CompressionAlgorithm support to e2e test harness` | framework/framework.go |
| 13 | `test(e2e): add compression e2e tests for snappy/lz4/none with restart` | compression_test/main.go |
| 14 | `chore: format and lint fixes` | (if needed) |
