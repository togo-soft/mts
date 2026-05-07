package measurement

import (
	"testing"
)

func TestTagsEqual_EdgeCases(t *testing.T) {
	a := map[string]string{"host": "server1"}
	b := map[string]string{"host": "server1"}
	if !tagsEqual(a, b) {
		t.Error("identical maps should be equal")
	}

	c := map[string]string{"host": "server1", "region": "us"}
	if tagsEqual(a, c) {
		t.Error("maps of different length should not be equal")
	}

	empty1 := map[string]string{}
	empty2 := map[string]string{}
	if !tagsEqual(empty1, empty2) {
		t.Error("both empty maps should be equal")
	}

	var nilMap map[string]string
	if CopyTags(nilMap) != nil {
		t.Error("copyTags of nil should return nil")
	}
}

func TestCopyTags(t *testing.T) {
	original := map[string]string{"host": "server1", "region": "us"}
	copied := CopyTags(original)

	if !tagsEqual(original, copied) {
		t.Error("copy should be equal to original")
	}

	copied["host"] = "server2"
	if tagsEqual(original, copied) {
		t.Error("modifying copy should not affect original")
	}
}

func TestTagsHash_EdgeCases(t *testing.T) {
	manyTags := map[string]string{}
	for i := 0; i < 100; i++ {
		manyTags[string(rune('a'+i%26))+string(rune(i/26))] = string(rune('0' + i%10))
	}
	h := tagsHash(manyTags)
	if h == 0 {
		t.Error("hash of many tags should not be 0")
	}

	specialTags := map[string]string{"key": "value\n\t\r\x00"}
	h2 := tagsHash(specialTags)
	if h2 == 0 {
		t.Error("hash of special chars should not be 0")
	}
}

func TestMeasurementMetaStore_GetTagsBySID_NotFound(t *testing.T) {
	m := NewMeasurementMetaStore()

	tags := map[string]string{"host": "server1"}
	sid, err := m.AllocateSID(tags)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}

	retrieved, ok := m.GetTagsBySID(sid)
	if !ok {
		t.Error("GetTagsBySID should return true for existing SID")
	}
	if retrieved["host"] != "server1" {
		t.Error("retrieved tags should have host=server1")
	}

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

func TestMeasurementMetaStore_GetSidsByTag_NotFound(t *testing.T) {
	m := NewMeasurementMetaStore()

	sids := m.GetSidsByTag("nonexistent", "value")
	if len(sids) != 0 {
		t.Errorf("expected 0 sids, got %d", len(sids))
	}
}
