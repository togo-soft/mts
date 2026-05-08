package metadata

import (
	"testing"
)

func TestNewManager(t *testing.T) {
	m, err := NewManager(t.TempDir())
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
}
