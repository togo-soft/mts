package metadata

import (
	"testing"
)

func TestNewManager(t *testing.T) {
	m, err := NewManager("/tmp/test_metadata")
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}
	if m == nil {
		t.Fatal("expected non-nil Manager")
	}
	if m.Catalog() == nil {
		t.Error("expected non-nil Catalog")
	}
	if m.Series() == nil {
		t.Error("expected non-nil SeriesStore")
	}
	if m.Shards() == nil {
		t.Error("expected non-nil ShardIndex")
	}
	if m.DataDir() != "/tmp/test_metadata" {
		t.Errorf("expected DataDir /tmp/test_metadata, got %s", m.DataDir())
	}
	if m.MetaDir() != "/tmp/test_metadata/_metadata" {
		t.Errorf("expected MetaDir /tmp/test_metadata/_metadata, got %s", m.MetaDir())
	}
}

func TestManager_Close(t *testing.T) {
	m, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}

	// 创建一些数据
	_ = m.Catalog().CreateDatabase("db1")
	_ = m.Catalog().CreateMeasurement("db1", "cpu")

	// 关闭
	if err := m.Close(); err != nil {
		t.Fatal("Close failed:", err)
	}
	// 可以多次关闭
	if err := m.Close(); err != nil {
		t.Fatal("second Close failed:", err)
	}
}

func TestManager_Persist_NoOp(t *testing.T) {
	m, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}
	defer func() { _ = m.Close() }()

	// Persist 目前为空操作
	if err := m.Persist(); err != nil {
		t.Fatal("Persist failed:", err)
	}
}
