package metadata

import (
	"sort"
	"testing"
)

func TestCatalog_CreateDatabase(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	if err := cat.CreateDatabase("metrics"); err != nil {
		t.Fatal("CreateDatabase failed:", err)
	}
	if !cat.DatabaseExists("metrics") {
		t.Error("expected DatabaseExists=true")
	}
}

func TestCatalog_CreateDatabase_Duplicate(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	_ = cat.CreateDatabase("metrics")
	// 重复创建应成功（幂等）
	if err := cat.CreateDatabase("metrics"); err != nil {
		t.Fatal("duplicate CreateDatabase should succeed:", err)
	}
}

func TestCatalog_CreateDatabase_EmptyName(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	if err := cat.CreateDatabase(""); err == nil {
		t.Error("expected error for empty name")
	}
}

func TestCatalog_DropDatabase(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	_ = cat.CreateDatabase("metrics")
	if err := cat.DropDatabase("metrics"); err != nil {
		t.Fatal("DropDatabase failed:", err)
	}
	if cat.DatabaseExists("metrics") {
		t.Error("expected DatabaseExists=false after drop")
	}
}

func TestCatalog_DropDatabase_NotFound(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	if err := cat.DropDatabase("nonexistent"); err == nil {
		t.Error("expected error for nonexistent database")
	}
}

func TestCatalog_ListDatabases(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	_ = cat.CreateDatabase("zulu")
	_ = cat.CreateDatabase("alpha")
	_ = cat.CreateDatabase("mike")

	names := cat.ListDatabases()
	expected := []string{"alpha", "mike", "zulu"}
	if len(names) != 3 {
		t.Fatalf("expected 3 databases, got %d", len(names))
	}
	for i, name := range expected {
		if names[i] != name {
			t.Errorf("expected names[%d]=%s, got %s", i, name, names[i])
		}
	}
}

func TestCatalog_ListDatabases_Empty(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	names := cat.ListDatabases()
	if len(names) != 0 {
		t.Errorf("expected empty list, got %v", names)
	}
}

func TestCatalog_CreateMeasurement(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	if err := cat.CreateMeasurement("metrics", "cpu"); err != nil {
		t.Fatal("CreateMeasurement failed:", err)
	}
	if !cat.MeasurementExists("metrics", "cpu") {
		t.Error("expected MeasurementExists=true")
	}
}

func TestCatalog_CreateMeasurement_EmptyDatabase(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	if err := cat.CreateMeasurement("", "cpu"); err == nil {
		t.Error("expected error for empty database")
	}
}

func TestCatalog_CreateMeasurement_EmptyName(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	if err := cat.CreateMeasurement("metrics", ""); err == nil {
		t.Error("expected error for empty measurement name")
	}
}

func TestCatalog_DropMeasurement(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	_ = cat.CreateMeasurement("metrics", "cpu")
	if err := cat.DropMeasurement("metrics", "cpu"); err != nil {
		t.Fatal("DropMeasurement failed:", err)
	}
	if cat.MeasurementExists("metrics", "cpu") {
		t.Error("expected MeasurementExists=false after drop")
	}
}

func TestCatalog_DropMeasurement_NotFound(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	_ = cat.CreateDatabase("metrics")
	if err := cat.DropMeasurement("metrics", "nonexistent"); err == nil {
		t.Error("expected error for nonexistent measurement")
	}
}

func TestCatalog_DropMeasurement_DatabaseNotFound(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	if err := cat.DropMeasurement("nonexistent", "cpu"); err == nil {
		t.Error("expected error for nonexistent database")
	}
}

func TestCatalog_ListMeasurements(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	_ = cat.CreateMeasurement("metrics", "cpu")
	_ = cat.CreateMeasurement("metrics", "disk")
	_ = cat.CreateMeasurement("metrics", "memory")

	names, err := cat.ListMeasurements("metrics")
	if err != nil {
		t.Fatal("ListMeasurements failed:", err)
	}

	expected := []string{"cpu", "disk", "memory"}
	sort.Strings(names)
	if len(names) != 3 {
		t.Fatalf("expected 3 measurements, got %d", len(names))
	}
	for i, name := range expected {
		if names[i] != name {
			t.Errorf("expected names[%d]=%s, got %s", i, name, names[i])
		}
	}
}

func TestCatalog_ListMeasurements_DatabaseNotFound(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	_, err := cat.ListMeasurements("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent database")
	}
}

func TestCatalog_GetRetention(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	_ = cat.CreateDatabase("db")
	_ = cat.CreateMeasurement("db", "meas")

	dur, err := cat.GetRetention("db", "meas")
	if err != nil {
		t.Fatal("GetRetention failed:", err)
	}
	if dur != 0 {
		t.Errorf("expected zero retention, got %v", dur)
	}
}

func TestCatalog_SetRetention_NoOp(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	_ = cat.CreateDatabase("db")
	_ = cat.CreateMeasurement("db", "meas")

	if err := cat.SetRetention("db", "meas", 86400); err != nil {
		t.Fatal("SetRetention failed:", err)
	}

	dur, err := cat.GetRetention("db", "meas")
	if err != nil {
		t.Fatal("GetRetention failed:", err)
	}
	if dur != 86400 {
		t.Errorf("expected retention 86400, got %v", dur)
	}
}

func TestCatalog_MeasurementExists(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	cat := m.Catalog()

	if cat.MeasurementExists("nonexistent", "cpu") {
		t.Error("expected false for nonexistent database")
	}

	_ = cat.CreateMeasurement("metrics", "cpu")
	if !cat.MeasurementExists("metrics", "cpu") {
		t.Error("expected true for existing measurement")
	}
}
