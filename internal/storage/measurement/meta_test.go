// internal/storage/measurement/meta_test.go
package measurement

import (
	"os"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

func TestMemoryMetaStore_SetAndGetMeta(t *testing.T) {
	store := NewMemoryMetaStore()

	meta := &types.MeasurementMeta{
		Version: 1,
		FieldSchema: []*types.FieldDef{
			{Name: "usage", Type: types.FieldType_FIELD_TYPE_FLOAT64},
		},
		TagKeys: []string{"host"},
		NextSid: 1,
	}

	err := store.SetMeta(t.Context(), meta)
	if err != nil {
		t.Fatalf("SetMeta failed: %v", err)
	}

	got, err := store.GetMeta(t.Context())
	if err != nil {
		t.Fatalf("GetMeta failed: %v", err)
	}

	if got.Version != meta.Version {
		t.Errorf("expected version %d, got %d", meta.Version, got.Version)
	}
	if len(got.FieldSchema) != len(meta.FieldSchema) {
		t.Errorf("expected %d fields, got %d", len(meta.FieldSchema), len(got.FieldSchema))
	}
}

func TestMemoryMetaStore_Series(t *testing.T) {
	store := NewMemoryMetaStore()

	tags := []byte{0x02, 0x04, 'h', 'o', 's', 't', 0x07, 's', 'e', 'r', 'v', 'e', 'r', '1'}

	err := store.SetSeries(t.Context(), 1, tags)
	if err != nil {
		t.Fatalf("SetSeries failed: %v", err)
	}

	got, err := store.GetSeries(t.Context(), 1)
	if err != nil {
		t.Fatalf("GetSeries failed: %v", err)
	}

	if len(got) != len(tags) {
		t.Errorf("expected %d bytes, got %d", len(tags), len(got))
	}
}

func TestMemoryMetaStore_TagIndex(t *testing.T) {
	store := NewMemoryMetaStore()

	err := store.AddTagIndex(t.Context(), "host", "server1", 1)
	if err != nil {
		t.Fatalf("AddTagIndex failed: %v", err)
	}

	sids, err := store.GetSidsByTag(t.Context(), "host", "server1")
	if err != nil {
		t.Fatalf("GetSidsByTag failed: %v", err)
	}

	if len(sids) != 1 || sids[0] != 1 {
		t.Errorf("expected [1], got %v", sids)
	}
}

func TestMemoryMetaStore_Close(t *testing.T) {
	store := NewMemoryMetaStore()
	err := store.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestMemoryMetaStore_NextSID(t *testing.T) {
	store := NewMemoryMetaStore()

	meta := &types.MeasurementMeta{
		Version:     1,
		FieldSchema: []*types.FieldDef{},
		TagKeys:     []string{},
		NextSid:     1,
	}

	err := store.SetMeta(t.Context(), meta)
	if err != nil {
		t.Fatalf("SetMeta failed: %v", err)
	}

	// 获取当前 sid
	m, _ := store.GetMeta(t.Context())
	if m.NextSid != 1 {
		t.Errorf("expected NextSid 1, got %d", m.NextSid)
	}

	// 分配新 sid
	newSID := m.NextSid
	m.NextSid++
	if err := store.SetMeta(t.Context(), m); err != nil {
		t.Fatalf("SetMeta failed: %v", err)
	}

	m, _ = store.GetMeta(t.Context())
	if m.NextSid != newSID+1 {
		t.Errorf("expected NextSid %d, got %d", newSID+1, m.NextSid)
	}
}

func TestMemoryMetaStore_PersistAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "meta.bin")

	// 创建并填充 MetaStore
	store := NewMemoryMetaStore()

	// 设置 MeasurementMeta
	meta := &types.MeasurementMeta{
		Version: 1,
		FieldSchema: []*types.FieldDef{
			{Name: "usage", Type: types.FieldType_FIELD_TYPE_FLOAT64},
			{Name: "count", Type: types.FieldType_FIELD_TYPE_INT64},
		},
		TagKeys: []string{"host", "region"},
		NextSid: 100,
	}
	if err := store.SetMeta(t.Context(), meta); err != nil {
		t.Fatalf("SetMeta failed: %v", err)
	}

	// 添加 series
	if err := store.SetSeries(t.Context(), 1, []byte(`{"host":"server1"}`)); err != nil {
		t.Fatalf("SetSeries failed: %v", err)
	}
	if err := store.SetSeries(t.Context(), 2, []byte(`{"host":"server2"}`)); err != nil {
		t.Fatalf("SetSeries failed: %v", err)
	}

	// 添加 tag index
	if err := store.AddTagIndex(t.Context(), "host", "server1", 1); err != nil {
		t.Fatalf("AddTagIndex failed: %v", err)
	}
	if err := store.AddTagIndex(t.Context(), "host", "server2", 2); err != nil {
		t.Fatalf("AddTagIndex failed: %v", err)
	}
	if err := store.AddTagIndex(t.Context(), "region", "us-west", 1); err != nil {
		t.Fatalf("AddTagIndex failed: %v", err)
	}

	// Persist
	if err := store.Persist(t.Context(), metaPath); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// 验证文件存在
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Fatalf("meta file not created")
	}

	// 创建新 store 并 Load
	store2 := NewMemoryMetaStore()
	if err := store2.Load(t.Context(), metaPath); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// 验证 meta
	loadedMeta, err := store2.GetMeta(t.Context())
	if err != nil {
		t.Fatalf("GetMeta failed: %v", err)
	}
	if loadedMeta.Version != meta.Version {
		t.Errorf("Version mismatch: expected %d, got %d", meta.Version, loadedMeta.Version)
	}
	if len(loadedMeta.FieldSchema) != len(meta.FieldSchema) {
		t.Errorf("FieldSchema length mismatch: expected %d, got %d", len(meta.FieldSchema), len(loadedMeta.FieldSchema))
	}
	if len(loadedMeta.TagKeys) != len(meta.TagKeys) {
		t.Errorf("TagKeys length mismatch: expected %d, got %d", len(meta.TagKeys), len(loadedMeta.TagKeys))
	}
	if loadedMeta.NextSid != meta.NextSid {
		t.Errorf("NextSid mismatch: expected %d, got %d", meta.NextSid, loadedMeta.NextSid)
	}

	// 验证 series
	series, err := store2.GetAllSeries(t.Context())
	if err != nil {
		t.Fatalf("GetAllSeries failed: %v", err)
	}
	if len(series) != 2 {
		t.Errorf("series count mismatch: expected 2, got %d", len(series))
	}

	// 验证 tag index
	sids, err := store2.GetSidsByTag(t.Context(), "host", "server1")
	if err != nil {
		t.Fatalf("GetSidsByTag failed: %v", err)
	}
	if len(sids) != 1 || sids[0] != 1 {
		t.Errorf("GetSidsByTag host:server1 mismatch: expected [1], got %v", sids)
	}

	sids, err = store2.GetSidsByTag(t.Context(), "region", "us-west")
	if err != nil {
		t.Fatalf("GetSidsByTag failed: %v", err)
	}
	if len(sids) != 1 || sids[0] != 1 {
		t.Errorf("GetSidsByTag region:us-west mismatch: expected [1], got %v", sids)
	}
}

func TestMemoryMetaStore_Load_FileNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "nonexistent.bin")

	store := NewMemoryMetaStore()
	err := store.Load(t.Context(), metaPath)
	if err == nil {
		t.Errorf("expected error for nonexistent file, got nil")
	}
}

func TestMemoryMetaStore_Load_InvalidFile(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "invalid.bin")

	// 写入无效数据
	if err := os.WriteFile(metaPath, []byte("invalid"), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	store := NewMemoryMetaStore()
	err := store.Load(t.Context(), metaPath)
	if err == nil {
		t.Errorf("expected error for invalid file, got nil")
	}
}
