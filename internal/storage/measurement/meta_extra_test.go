package measurement

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

func TestWriteReadString(t *testing.T) {
	tests := []struct {
		name string
		val  string
	}{
		{"short", "hello"},
		{"long", "this is a longer string with special chars: 日本語"},
		{"special", "line\nbreak\ttab"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeString(&buf, tt.val); err != nil {
				t.Fatalf("writeString failed: %v", err)
			}

			result, err := readString(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("readString failed: %v", err)
			}
			if result != tt.val {
				t.Errorf("expected %q, got %q", tt.val, result)
			}
		})
	}
}

func TestWriteReadBytes(t *testing.T) {
	tests := []struct {
		name string
		val  []byte
	}{
		{"short", []byte("hello")},
		{"binary", []byte{0x00, 0xFF, 0x01, 0x02, 0xFE}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeBytes(&buf, tt.val); err != nil {
				t.Fatalf("writeBytes failed: %v", err)
			}

			result, err := readBytes(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("readBytes failed: %v", err)
			}
			if !bytes.Equal(result, tt.val) {
				t.Errorf("expected %v, got %v", tt.val, result)
			}
		})
	}
}

func TestMemoryMetaStore_Persist_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "meta.bin")

	store := NewMemoryMetaStore()
	if err := store.Persist(t.Context(), metaPath); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Load 应该成功
	store2 := NewMemoryMetaStore()
	if err := store2.Load(t.Context(), metaPath); err != nil {
		t.Fatalf("Load failed: %v", err)
	}
}

func TestMemoryMetaStore_Persist_DirectoryCreation(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "subdir", "nested", "meta.bin")

	store := NewMemoryMetaStore()
	if err := store.Persist(t.Context(), metaPath); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// 验证目录创建成功
	if _, err := os.Stat(filepath.Dir(metaPath)); os.IsNotExist(err) {
		t.Error("directory should be created")
	}
}

func TestMemoryMetaStore_Load_InvalidMagic(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "invalid_magic.bin")

	// 写入错误的 magic 头
	f, err := os.Create(metaPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	// 写入一个错误的 magic (不是 0x4D545348)
	if err := binary.Write(f, binary.BigEndian, uint32(0xDEADBEEF)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	_ = f.Close()

	store := NewMemoryMetaStore()
	err = store.Load(t.Context(), metaPath)
	if err == nil {
		t.Errorf("expected error for invalid magic, got nil")
	}
}

func TestMemoryMetaStore_Load_InvalidVersion(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "invalid_version.bin")

	// 写入错误的版本
	f, err := os.Create(metaPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, uint32(0x4D545348)); err != nil {
		t.Fatalf("Write magic failed: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, int64(999)); err != nil {
		t.Fatalf("Write version failed: %v", err)
	}
	_ = f.Close()

	store := NewMemoryMetaStore()
	err = store.Load(t.Context(), metaPath)
	if err == nil {
		t.Errorf("expected error for invalid version, got nil")
	}
}

func TestMemoryMetaStore_Load_TruncatedFile(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "truncated.bin")

	// 写入被截断的文件
	f, err := os.Create(metaPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	// 只写入 magic 和 version，没有数据
	if err := binary.Write(f, binary.BigEndian, uint32(0x4D545348)); err != nil {
		t.Fatalf("Write magic failed: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, int64(1)); err != nil {
		t.Fatalf("Write version failed: %v", err)
	}
	_ = f.Close()

	store := NewMemoryMetaStore()
	err = store.Load(t.Context(), metaPath)
	if err == nil {
		t.Errorf("expected error for truncated file, got nil")
	}
}

func TestMemoryMetaStore_GetAllSeries(t *testing.T) {
	store := NewMemoryMetaStore()

	// 添加一些 series
	_ = store.SetSeries(t.Context(), 1, []byte("server1"))
	_ = store.SetSeries(t.Context(), 2, []byte("server2"))
	_ = store.SetSeries(t.Context(), 3, []byte("server3"))

	all, err := store.GetAllSeries(t.Context())
	if err != nil {
		t.Fatalf("GetAllSeries failed: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("expected 3 series, got %d", len(all))
	}
}

func TestMemoryMetaStore_AddTagIndex_Multiple(t *testing.T) {
	store := NewMemoryMetaStore()

	// 添加同一个 tag value 对应多个 sid
	_ = store.AddTagIndex(t.Context(), "region", "us-west", 1)
	_ = store.AddTagIndex(t.Context(), "region", "us-west", 2)
	_ = store.AddTagIndex(t.Context(), "region", "us-west", 3)

	sids, _ := store.GetSidsByTag(t.Context(), "region", "us-west")
	if len(sids) != 3 {
		t.Errorf("expected 3 sids, got %d", len(sids))
	}
}

func TestTagsEqual_EdgeCases(t *testing.T) {
	// 相同的 map
	a := map[string]string{"host": "server1"}
	b := map[string]string{"host": "server1"}
	if !tagsEqual(a, b) {
		t.Error("identical maps should be equal")
	}

	// 不同的长度
	c := map[string]string{"host": "server1", "region": "us"}
	if tagsEqual(a, c) {
		t.Error("maps of different length should not be equal")
	}

	// 空 map
	empty1 := map[string]string{}
	empty2 := map[string]string{}
	if !tagsEqual(empty1, empty2) {
		t.Error("both empty maps should be equal")
	}

	// nil map (copyTags 返回 nil)
	var nilMap map[string]string
	if copyTags(nilMap) != nil {
		t.Error("copyTags of nil should return nil")
	}
}

func TestCopyTags(t *testing.T) {
	original := map[string]string{"host": "server1", "region": "us"}
	copied := copyTags(original)

	if !tagsEqual(original, copied) {
		t.Error("copy should be equal to original")
	}

	// 修改副本不应该影响原始
	copied["host"] = "server2"
	if tagsEqual(original, copied) {
		t.Error("modifying copy should not affect original")
	}
}

func TestTagsHash_EdgeCases(t *testing.T) {
	// 大量 tags
	manyTags := map[string]string{}
	for i := 0; i < 100; i++ {
		manyTags[string(rune('a'+i%26))+string(rune(i/26))] = string(rune('0' + i%10))
	}
	h := tagsHash(manyTags)
	if h == 0 {
		t.Error("hash of many tags should not be 0")
	}

	// 特殊字符
	specialTags := map[string]string{"key": "value\n\t\r\x00"}
	h2 := tagsHash(specialTags)
	if h2 == 0 {
		t.Error("hash of special chars should not be 0")
	}
}

func TestMeasurementMetaStore_GetTagsBySID_NotFound(t *testing.T) {
	m := NewMeasurementMetaStore()

	// 分配一个 sid
	tags := map[string]string{"host": "server1"}
	sid := m.AllocateSID(tags)

	// 获取存在的 sid
	retrieved, ok := m.GetTagsBySID(sid)
	if !ok {
		t.Error("GetTagsBySID should return true for existing SID")
	}
	if retrieved["host"] != "server1" {
		t.Error("retrieved tags should have host=server1")
	}

	// 获取不存在的 sid
	_, ok = m.GetTagsBySID(9999)
	if ok {
		t.Error("GetTagsBySID should return false for non-existent SID")
	}
}

func TestTagsEqual_DifferentValues(t *testing.T) {
	a := map[string]string{"host": "server1"}
	b := map[string]string{"host": "server2"}
	if tagsEqual(a, b) {
		t.Error("maps with different values should not be equal")
	}
}

func TestWriteReadString_LongString(t *testing.T) {
	var buf bytes.Buffer
	longStr := ""
	for i := 0; i < 1000; i++ {
		longStr += "a"
	}
	if err := writeString(&buf, longStr); err != nil {
		t.Fatalf("writeString failed: %v", err)
	}

	result, err := readString(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("readString failed: %v", err)
	}
	if result != longStr {
		t.Errorf("expected %d chars, got %d", len(longStr), len(result))
	}
}

func TestWriteReadBytes_LargeData(t *testing.T) {
	var buf bytes.Buffer
	largeData := make([]byte, 10000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	if err := writeBytes(&buf, largeData); err != nil {
		t.Fatalf("writeBytes failed: %v", err)
	}

	result, err := readBytes(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("readBytes failed: %v", err)
	}
	if !bytes.Equal(result, largeData) {
		t.Error("read bytes should match written bytes")
	}
}

func TestMeasurementMetaStore_GetSidsByTag_NotFound(t *testing.T) {
	m := NewMeasurementMetaStore()

	sids := m.GetSidsByTag("nonexistent", "value")
	if len(sids) != 0 {
		t.Errorf("expected 0 sids, got %d", len(sids))
	}
}

func TestWriteString_Error(t *testing.T) {
	// bytes.Buffer 写入不会失败，所以这个测试主要验证函数正常工作
	var buf bytes.Buffer
	err := writeString(&buf, "test")
	if err != nil {
		t.Fatalf("writeString failed: %v", err)
	}
}

func TestReadString_TruncatedData(t *testing.T) {
	// 只写入部分数据，readString 应该失败
	truncated := make([]byte, 4)
	binary.BigEndian.PutUint32(truncated, uint32(100)) // length = 100 but no data

	_, err := readString(bytes.NewReader(truncated))
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

func TestWriteBytes_Error(t *testing.T) {
	var buf bytes.Buffer
	err := writeBytes(&buf, []byte("test"))
	if err != nil {
		t.Fatalf("writeBytes failed: %v", err)
	}
}

func TestReadBytes_TruncatedData(t *testing.T) {
	truncated := make([]byte, 4)
	binary.BigEndian.PutUint32(truncated, uint32(100)) // length = 100 but no data

	_, err := readBytes(bytes.NewReader(truncated))
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

func TestMemoryMetaStore_Persist_WithMeta(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "meta.bin")

	store := NewMemoryMetaStore()

	// 设置 meta
	meta := &types.MeasurementMeta{
		Version: 1,
		FieldSchema: []*types.FieldDef{
			{Name: "cpu", Type: types.FieldType_FIELD_TYPE_FLOAT64},
			{Name: "memory", Type: types.FieldType_FIELD_TYPE_INT64},
		},
		TagKeys: []string{"host", "region"},
		NextSid: 100,
	}
	if err := store.SetMeta(t.Context(), meta); err != nil {
		t.Fatalf("SetMeta failed: %v", err)
	}

	// Persist
	if err := store.Persist(t.Context(), metaPath); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Load 并验证
	store2 := NewMemoryMetaStore()
	if err := store2.Load(t.Context(), metaPath); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	loadedMeta, err := store2.GetMeta(t.Context())
	if err != nil {
		t.Fatalf("GetMeta failed: %v", err)
	}
	if loadedMeta.Version != 1 {
		t.Errorf("expected version 1, got %d", loadedMeta.Version)
	}
	if len(loadedMeta.FieldSchema) != 2 {
		t.Errorf("expected 2 fields, got %d", len(loadedMeta.FieldSchema))
	}
	if loadedMeta.FieldSchema[0].Name != "cpu" {
		t.Errorf("expected first field 'cpu', got %s", loadedMeta.FieldSchema[0].Name)
	}
	if len(loadedMeta.TagKeys) != 2 {
		t.Errorf("expected 2 tag keys, got %d", len(loadedMeta.TagKeys))
	}
	if loadedMeta.NextSid != 100 {
		t.Errorf("expected NextSid 100, got %d", loadedMeta.NextSid)
	}
}

func TestMemoryMetaStore_Persist_WithSeries(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "meta.bin")

	store := NewMemoryMetaStore()

	// 设置 series
	_ = store.SetSeries(t.Context(), 1, []byte("host\x00server1"))
	_ = store.SetSeries(t.Context(), 2, []byte("host\x00server2"))

	// Persist
	if err := store.Persist(t.Context(), metaPath); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Load 并验证
	store2 := NewMemoryMetaStore()
	if err := store2.Load(t.Context(), metaPath); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	all, err := store2.GetAllSeries(t.Context())
	if err != nil {
		t.Fatalf("GetAllSeries failed: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("expected 2 series, got %d", len(all))
	}
	if !bytes.Equal(all[1], []byte("host\x00server1")) {
		t.Errorf("expected series 1 to be 'host\\x00server1', got %v", all[1])
	}
}

func TestMemoryMetaStore_Persist_WithTagIndex(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "meta.bin")

	store := NewMemoryMetaStore()

	// 设置 tagIndex
	_ = store.AddTagIndex(t.Context(), "region", "us-west", 1)
	_ = store.AddTagIndex(t.Context(), "region", "us-west", 2)
	_ = store.AddTagIndex(t.Context(), "region", "us-east", 3)

	// Persist
	if err := store.Persist(t.Context(), metaPath); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Load 并验证
	store2 := NewMemoryMetaStore()
	if err := store2.Load(t.Context(), metaPath); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	sids, err := store2.GetSidsByTag(t.Context(), "region", "us-west")
	if err != nil {
		t.Fatalf("GetSidsByTag failed: %v", err)
	}
	if len(sids) != 2 {
		t.Errorf("expected 2 sids for us-west, got %d", len(sids))
	}

	sids2, err := store2.GetSidsByTag(t.Context(), "region", "us-east")
	if err != nil {
		t.Fatalf("GetSidsByTag failed: %v", err)
	}
	if len(sids2) != 1 {
		t.Errorf("expected 1 sid for us-east, got %d", len(sids2))
	}
}

func TestMemoryMetaStore_Persist_FullData(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "meta.bin")

	store := NewMemoryMetaStore()

	// 设置完整的 meta
	meta := &types.MeasurementMeta{
		Version: 1,
		FieldSchema: []*types.FieldDef{
			{Name: "cpu", Type: types.FieldType_FIELD_TYPE_FLOAT64},
			{Name: "memory", Type: types.FieldType_FIELD_TYPE_INT64},
			{Name: "status", Type: types.FieldType_FIELD_TYPE_STRING},
		},
		TagKeys: []string{"host", "region", "env"},
		NextSid: 50,
	}
	_ = store.SetMeta(t.Context(), meta)

	// 设置 series
	_ = store.SetSeries(t.Context(), 1, []byte("host\x00server1"))
	_ = store.SetSeries(t.Context(), 2, []byte("host\x00server2"))
	_ = store.SetSeries(t.Context(), 3, []byte("host\x00server3"))

	// 设置 tagIndex
	_ = store.AddTagIndex(t.Context(), "region", "us-west", 1)
	_ = store.AddTagIndex(t.Context(), "region", "us-west", 2)
	_ = store.AddTagIndex(t.Context(), "region", "us-east", 3)
	_ = store.AddTagIndex(t.Context(), "env", "prod", 1)
	_ = store.AddTagIndex(t.Context(), "env", "prod", 2)
	_ = store.AddTagIndex(t.Context(), "env", "prod", 3)

	// Persist
	if err := store.Persist(t.Context(), metaPath); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Load 并验证
	store2 := NewMemoryMetaStore()
	if err := store2.Load(t.Context(), metaPath); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// 验证 meta
	loadedMeta, err := store2.GetMeta(t.Context())
	if err != nil {
		t.Fatalf("GetMeta failed: %v", err)
	}
	if loadedMeta.Version != 1 {
		t.Errorf("expected version 1, got %d", loadedMeta.Version)
	}
	if len(loadedMeta.FieldSchema) != 3 {
		t.Errorf("expected 3 fields, got %d", len(loadedMeta.FieldSchema))
	}
	if len(loadedMeta.TagKeys) != 3 {
		t.Errorf("expected 3 tag keys, got %d", len(loadedMeta.TagKeys))
	}
	if loadedMeta.NextSid != 50 {
		t.Errorf("expected NextSid 50, got %d", loadedMeta.NextSid)
	}

	// 验证 series
	all, err := store2.GetAllSeries(t.Context())
	if err != nil {
		t.Fatalf("GetAllSeries failed: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("expected 3 series, got %d", len(all))
	}

	// 验证 tagIndex
	sids, _ := store2.GetSidsByTag(t.Context(), "region", "us-west")
	if len(sids) != 2 {
		t.Errorf("expected 2 sids for us-west, got %d", len(sids))
	}

	sidsProd, _ := store2.GetSidsByTag(t.Context(), "env", "prod")
	if len(sidsProd) != 3 {
		t.Errorf("expected 3 sids for prod, got %d", len(sidsProd))
	}
}

func TestMemoryMetaStore_Load_TrailingData(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "trailing.bin")

	// 创建一个有效的 meta 文件但在末尾添加额外数据
	f, err := os.Create(metaPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// 写入有效的 magic 和 version
	if err := binary.Write(f, binary.BigEndian, uint32(0x4D545348)); err != nil {
		t.Fatalf("Write magic failed: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, int64(1)); err != nil {
		t.Fatalf("Write version failed: %v", err)
	}

	// 写入一些有效但空的数据
	if err := binary.Write(f, binary.BigEndian, int64(0)); err != nil { // meta version
		t.Fatalf("Write meta version failed: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, int64(0)); err != nil { // field count
		t.Fatalf("Write field count failed: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, int64(0)); err != nil { // tag key count
		t.Fatalf("Write tag key count failed: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, uint64(0)); err != nil { // next sid
		t.Fatalf("Write next sid failed: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, int64(0)); err != nil { // series count
		t.Fatalf("Write series count failed: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, int64(0)); err != nil { // tag index count
		t.Fatalf("Write tag index count failed: %v", err)
	}

	// 添加额外的 trailing 数据
	if _, err := f.Write([]byte("extra data at end")); err != nil {
		t.Fatalf("Write trailing data failed: %v", err)
	}
	_ = f.Close()

	// Load 应该仍然成功，因为 trailing 数据会被忽略
	store := NewMemoryMetaStore()
	err = store.Load(t.Context(), metaPath)
	if err != nil {
		t.Errorf("Load with trailing data failed: %v", err)
	}
}

func TestMemoryMetaStore_GetSeries_NotFound(t *testing.T) {
	store := NewMemoryMetaStore()

	// 获取不存在的 series
	_, err := store.GetSeries(t.Context(), 9999)
	if err != nil {
		t.Fatalf("GetSeries failed: %v", err)
	}
}

func TestMemoryMetaStore_SetSeries(t *testing.T) {
	store := NewMemoryMetaStore()

	// 设置 series
	err := store.SetSeries(t.Context(), 1, []byte("host\x00server1"))
	if err != nil {
		t.Fatalf("SetSeries failed: %v", err)
	}

	// 获取 series
	tags, err := store.GetSeries(t.Context(), 1)
	if err != nil {
		t.Fatalf("GetSeries failed: %v", err)
	}
	if !bytes.Equal(tags, []byte("host\x00server1")) {
		t.Errorf("expected 'host\\x00server1', got %v", tags)
	}
}

func TestMemoryMetaStore_GetMeta_Nil(t *testing.T) {
	store := NewMemoryMetaStore()

	// meta 为 nil
	meta, err := store.GetMeta(t.Context())
	if err != nil {
		t.Fatalf("GetMeta failed: %v", err)
	}
	if meta != nil {
		t.Error("expected nil meta for new store")
	}
}
