package metadata

import (
	"testing"
)

func TestSimpleSeriesStore_AllocateSID_New(t *testing.T) {
	s := NewSimpleSeriesStore()

	sid, err := s.AllocateSID(map[string]string{"host": "s1"})
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid != 0 {
		t.Errorf("expected SID 0, got %d", sid)
	}
}

func TestSimpleSeriesStore_AllocateSID_SameTags(t *testing.T) {
	s := NewSimpleSeriesStore()

	sid1, _ := s.AllocateSID(map[string]string{"host": "s1", "region": "us"})
	sid2, err := s.AllocateSID(map[string]string{"host": "s1", "region": "us"})
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid1 != sid2 {
		t.Errorf("expected same SID %d, got %d", sid1, sid2)
	}
}

func TestSimpleSeriesStore_GetTagsBySID(t *testing.T) {
	s := NewSimpleSeriesStore()

	sid, _ := s.AllocateSID(map[string]string{"host": "s1", "region": "us"})
	tags, ok := s.GetTagsBySID(sid)
	if !ok {
		t.Fatal("GetTagsBySID returned false")
	}
	if tags["host"] != "s1" {
		t.Errorf("expected host=s1, got %s", tags["host"])
	}
}

func TestSimpleSeriesStore_GetTagsBySID_NotFound(t *testing.T) {
	s := NewSimpleSeriesStore()

	_, ok := s.GetTagsBySID(999)
	if ok {
		t.Error("expected false for nonexistent SID")
	}
}
