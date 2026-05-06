package measurement

import "testing"

func TestTagsHash(t *testing.T) {
	emptyTags := map[string]string{}
	singleTag := map[string]string{"host": "server1"}
	multipleTags := map[string]string{"host": "server1", "region": "us"}
	differentTags := map[string]string{"host": "server2", "region": "eu"}

	tests := []struct {
		name string
		tags map[string]string
	}{
		{"empty", emptyTags},
		{"single", singleTag},
		{"multiple", multipleTags},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := tagsHash(tt.tags)
			if h == 0 && len(tt.tags) > 0 {
				t.Error("hash should not be 0 for non-empty tags")
			}
		})
	}

	// Verify different inputs produce different hashes
	h1 := tagsHash(singleTag)
	h2 := tagsHash(multipleTags)
	h3 := tagsHash(differentTags)

	if h1 == h2 {
		t.Error("hash of single tag and multiple tags should differ")
	}
	if h1 == h3 {
		t.Error("hash of single tag and different tags should differ")
	}
	if h2 == h3 {
		t.Error("hash of multiple tags and different tags should differ")
	}

	// Empty tags must hash to 0
	if h := tagsHash(emptyTags); h != 0 {
		t.Errorf("empty tags should hash to 0, got %d", h)
	}
}

func TestTagsHashConsistency(t *testing.T) {
	tags := map[string]string{"host": "server1", "region": "us", "env": "prod"}

	h1 := tagsHash(tags)
	h2 := tagsHash(tags)

	if h1 != h2 {
		t.Errorf("hash should be consistent: got %d and %d", h1, h2)
	}
}

func TestTagsHashOrderIndependence(t *testing.T) {
	tags1 := map[string]string{"host": "server1", "region": "us"}
	tags2 := map[string]string{"region": "us", "host": "server1"}

	h1 := tagsHash(tags1)
	h2 := tagsHash(tags2)

	if h1 != h2 {
		t.Errorf("hash should be order-independent: got %d and %d", h1, h2)
	}
}

func TestTagsEqual(t *testing.T) {
	tags1 := map[string]string{"host": "server1", "region": "us"}
	tags2 := map[string]string{"region": "us", "host": "server1"}
	tags3 := map[string]string{"host": "server2"}

	if !tagsEqual(tags1, tags2) {
		t.Error("tags with same content but different order should be equal")
	}
	if tagsEqual(tags1, tags3) {
		t.Error("tags with different content should not be equal")
	}
}

func TestMeasurementMetaStore_AllocateSID(t *testing.T) {
	m := NewMeasurementMetaStore()

	tags1 := map[string]string{"host": "server1"}
	sid1, err := m.AllocateSID(tags1)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	if sid1 != 0 {
		t.Errorf("first SID should be 0, got %d", sid1)
	}

	// 相同 tags 应该返回相同 SID
	sid1Again, err := m.AllocateSID(tags1)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	if sid1Again != sid1 {
		t.Errorf("same tags should return same SID, got %d vs %d", sid1Again, sid1)
	}

	// 不同 tags 应该返回新 SID
	tags2 := map[string]string{"host": "server2"}
	sid2, err := m.AllocateSID(tags2)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	if sid2 != 1 {
		t.Errorf("second SID should be 1, got %d", sid2)
	}
}

func TestMeasurementMetaStore_GetTagsBySID(t *testing.T) {
	m := NewMeasurementMetaStore()

	tags := map[string]string{"host": "server1", "region": "us"}
	sid, err := m.AllocateSID(tags)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}

	retrieved, ok := m.GetTagsBySID(sid)
	if !ok {
		t.Error("GetTagsBySID should return true")
	}
	if !tagsEqual(retrieved, tags) {
		t.Error("retrieved tags should match original")
	}

	// 不存在的 SID
	_, ok = m.GetTagsBySID(999)
	if ok {
		t.Error("GetTagsBySID for non-existent SID should return false")
	}
}

func TestMeasurementMetaStore_GetSidsByTag(t *testing.T) {
	m := NewMeasurementMetaStore()

	tags1 := map[string]string{"host": "server1", "region": "us"}
	tags2 := map[string]string{"host": "server2", "region": "us"}
	tags3 := map[string]string{"host": "server3", "region": "eu"}

	_, err := m.AllocateSID(tags1)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	_, err = m.AllocateSID(tags2)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	_, err = m.AllocateSID(tags3)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}

	sids := m.GetSidsByTag("region", "us")
	if len(sids) != 2 {
		t.Errorf("region=us should have 2 sids, got %d", len(sids))
	}
}
