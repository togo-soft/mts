// internal/storage/measurement/meta_test.go
package measurement

import (
	"context"
	"testing"

	"micro-ts/internal/types"
)

func TestMemoryMetaStore_SetAndGetMeta(t *testing.T) {
	store := NewMemoryMetaStore()

	meta := &types.MeasurementMeta{
		Version: 1,
		FieldSchema: []types.FieldDef{
			{Name: "usage", Type: types.FieldTypeFloat64},
		},
		TagKeys: []string{"host"},
		NextSID: 1,
	}

	err := store.SetMeta(context.Background(), meta)
	if err != nil {
		t.Fatalf("SetMeta failed: %v", err)
	}

	got, err := store.GetMeta(context.Background())
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

	err := store.SetSeries(context.Background(), 1, tags)
	if err != nil {
		t.Fatalf("SetSeries failed: %v", err)
	}

	got, err := store.GetSeries(context.Background(), 1)
	if err != nil {
		t.Fatalf("GetSeries failed: %v", err)
	}

	if len(got) != len(tags) {
		t.Errorf("expected %d bytes, got %d", len(tags), len(got))
	}
}

func TestMemoryMetaStore_TagIndex(t *testing.T) {
	store := NewMemoryMetaStore()

	err := store.AddTagIndex(context.Background(), "host", "server1", 1)
	if err != nil {
		t.Fatalf("AddTagIndex failed: %v", err)
	}

	sids, err := store.GetSidsByTag(context.Background(), "host", "server1")
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
		FieldSchema: []types.FieldDef{},
		TagKeys:     []string{},
		NextSID:     1,
	}

	err := store.SetMeta(context.Background(), meta)
	if err != nil {
		t.Fatalf("SetMeta failed: %v", err)
	}

	// 获取当前 sid
	m, _ := store.GetMeta(context.Background())
	if m.NextSID != 1 {
		t.Errorf("expected NextSID 1, got %d", m.NextSID)
	}

	// 分配新 sid
	newSID := m.NextSID
	m.NextSID++
	if err := store.SetMeta(context.Background(), m); err != nil {
		t.Fatalf("SetMeta failed: %v", err)
	}

	m, _ = store.GetMeta(context.Background())
	if m.NextSID != newSID+1 {
		t.Errorf("expected NextSID %d, got %d", newSID+1, m.NextSID)
	}
}
