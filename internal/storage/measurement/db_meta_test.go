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

func TestDatabaseMetaStore_ListMeasurements(t *testing.T) {
	db := NewDatabaseMetaStore()
	db.GetOrCreate("cpu")
	db.GetOrCreate("memory")
	db.GetOrCreate("disk")

	measurements := db.ListMeasurements()
	if len(measurements) != 3 {
		t.Errorf("expected 3 measurements, got %d", len(measurements))
	}
}

func TestDatabaseMetaStore_DropMeasurement(t *testing.T) {
	db := NewDatabaseMetaStore()
	db.GetOrCreate("cpu")
	db.GetOrCreate("memory")

	// 删除存在的 measurement
	if !db.DropMeasurement("cpu") {
		t.Error("DropMeasurement should return true for existing measurement")
	}

	// 验证已删除
	measurements := db.ListMeasurements()
	if len(measurements) != 1 {
		t.Errorf("expected 1 measurement after drop, got %d", len(measurements))
	}

	// 删除不存在的 measurement
	if db.DropMeasurement("nonexistent") {
		t.Error("DropMeasurement should return false for non-existent measurement")
	}
}

func TestDatabaseMetaStore_MeasurementExists(t *testing.T) {
	db := NewDatabaseMetaStore()

	// 初始不存在
	if db.MeasurementExists("cpu") {
		t.Error("cpu should not exist initially")
	}

	// 创建 measurement
	db.GetOrCreate("cpu")

	// 现在存在
	if !db.MeasurementExists("cpu") {
		t.Error("cpu should exist after GetOrCreate")
	}

	// 其他 measurement 仍然不存在
	if db.MeasurementExists("memory") {
		t.Error("memory should not exist")
	}

	// 删除后不存在
	db.DropMeasurement("cpu")
	if db.MeasurementExists("cpu") {
		t.Error("cpu should not exist after DropMeasurement")
	}
}
