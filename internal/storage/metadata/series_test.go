package metadata

import (
	"testing"
)

func TestSeriesStore_AllocateSID_New(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	s := m.Series()

	sid, err := s.AllocateSID("db", "cpu", map[string]string{"host": "s1"})
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid != 0 {
		t.Errorf("expected SID 0, got %d", sid)
	}
}

func TestSeriesStore_AllocateSID_SecondNew(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	s := m.Series()

	sid1, _ := s.AllocateSID("db", "cpu", map[string]string{"host": "s1"})
	sid2, err := s.AllocateSID("db", "cpu", map[string]string{"host": "s2"})
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid2 != sid1+1 {
		t.Errorf("expected SID %d, got %d", sid1+1, sid2)
	}
}

func TestSeriesStore_AllocateSID_SameTags(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	s := m.Series()

	sid1, _ := s.AllocateSID("db", "cpu", map[string]string{"host": "s1", "region": "us"})
	sid2, err := s.AllocateSID("db", "cpu", map[string]string{"host": "s1", "region": "us"})
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid1 != sid2 {
		t.Errorf("expected same SID %d, got %d", sid1, sid2)
	}
}

func TestSeriesStore_AllocateSID_DifferentOrder(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	s := m.Series()

	sid1, _ := s.AllocateSID("db", "cpu", map[string]string{"host": "s1", "region": "us"})
	sid2, err := s.AllocateSID("db", "cpu", map[string]string{"region": "us", "host": "s1"})
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid1 != sid2 {
		t.Errorf("expected same SID %d for different order, got %d", sid1, sid2)
	}
}

func TestSeriesStore_AllocateSID_DifferentMeasurements(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	s := m.Series()

	sidCPU, _ := s.AllocateSID("db", "cpu", map[string]string{"host": "s1"})
	sidMem, err := s.AllocateSID("db", "memory", map[string]string{"host": "s1"})
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sidCPU != 0 {
		t.Errorf("expected cpu SID 0, got %d", sidCPU)
	}
	if sidMem != 0 {
		t.Errorf("expected memory SID 0, got %d", sidMem)
	}
}

func TestSeriesStore_GetTags(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	s := m.Series()

	sid, _ := s.AllocateSID("db", "cpu", map[string]string{"host": "s1", "region": "us"})
	tags, ok := s.GetTags("db", "cpu", sid)
	if !ok {
		t.Fatal("GetTags returned false")
	}
	if tags["host"] != "s1" {
		t.Errorf("expected host=s1, got %s", tags["host"])
	}
	if tags["region"] != "us" {
		t.Errorf("expected region=us, got %s", tags["region"])
	}
}

func TestSeriesStore_GetTags_NotFound(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	s := m.Series()

	_, ok := s.GetTags("db", "cpu", 999)
	if ok {
		t.Error("expected false for nonexistent SID")
	}
}

func TestSeriesStore_GetTags_MeasurementNotFound(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	s := m.Series()

	_, ok := s.GetTags("db", "nonexistent", 0)
	if ok {
		t.Error("expected false for nonexistent measurement")
	}
}

func TestSeriesStore_GetSIDsByTag(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	s := m.Series()

	sid1, _ := s.AllocateSID("db", "cpu", map[string]string{"host": "s1", "region": "us"})
	sid2, _ := s.AllocateSID("db", "cpu", map[string]string{"host": "s1", "region": "eu"})
	_ = sid2

	sids := s.GetSIDsByTag("db", "cpu", "host", "s1")
	if len(sids) != 2 {
		t.Fatalf("expected 2 SIDs, got %d", len(sids))
	}

	found := false
	for _, sid := range sids {
		if sid == sid1 {
			found = true
		}
	}
	if !found {
		t.Errorf("expected sid1=%d in results, got %v", sid1, sids)
	}
}

func TestSeriesStore_GetSIDsByTag_NotFound(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	s := m.Series()

	sids := s.GetSIDsByTag("db", "cpu", "nonexistent", "value")
	if len(sids) != 0 {
		t.Errorf("expected empty list, got %v", sids)
	}
}

func TestSeriesStore_GetSIDsByTag_MeasurementNotFound(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	s := m.Series()

	sids := s.GetSIDsByTag("db", "nonexistent", "key", "val")
	if sids != nil {
		t.Errorf("expected nil, got %v", sids)
	}
}

func TestSeriesStore_SeriesCount(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	s := m.Series()

	if count := s.SeriesCount("db", "cpu"); count != 0 {
		t.Errorf("expected 0, got %d", count)
	}

	_, _ = s.AllocateSID("db", "cpu", map[string]string{"host": "s1"})
	_, _ = s.AllocateSID("db", "cpu", map[string]string{"host": "s2"})
	_, _ = s.AllocateSID("db", "cpu", map[string]string{"host": "s3"})

	if count := s.SeriesCount("db", "cpu"); count != 3 {
		t.Errorf("expected 3, got %d", count)
	}
}

func TestSeriesStore_SeriesCount_MeasurementNotFound(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	s := m.Series()

	if count := s.SeriesCount("db", "nonexistent"); count != 0 {
		t.Errorf("expected 0, got %d", count)
	}
}
