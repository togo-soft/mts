package measurement

import "testing"

func TestDatabaseMetaStore_GetOrCreate(t *testing.T) {
	db := NewDatabaseMetaStore()

	m1 := db.GetOrCreate("cpu")
	m2 := db.GetOrCreate("cpu")
	m3 := db.GetOrCreate("memory")

	if m1 != m2 {
		t.Error("GetOrCreate with same name should return same instance")
	}
	if m1 == m3 {
		t.Error("GetOrCreate with different name should return different instance")
	}
}

func TestDatabaseMetaStore_Close(t *testing.T) {
	db := NewDatabaseMetaStore()
	db.GetOrCreate("cpu")
	db.GetOrCreate("memory")

	if err := db.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}
