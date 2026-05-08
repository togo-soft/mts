package metadata

import (
	"testing"
)

func TestCatalog_CreateDatabase(t *testing.T) {
	m, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}
	cat := m.Catalog()

	if err := cat.CreateDatabase("metrics"); err != nil {
		t.Fatal("CreateDatabase failed:", err)
	}
	if !cat.DatabaseExists("metrics") {
		t.Error("expected DatabaseExists=true")
	}
}

func TestCatalog_CreateMeasurement(t *testing.T) {
	m, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}
	cat := m.Catalog()

	if err := cat.CreateMeasurement("metrics", "cpu"); err != nil {
		t.Fatal("CreateMeasurement failed:", err)
	}
	if !cat.MeasurementExists("metrics", "cpu") {
		t.Error("expected MeasurementExists=true")
	}
}

func TestCatalog_DropDatabase(t *testing.T) {
	m, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}
	cat := m.Catalog()

	_ = cat.CreateDatabase("metrics")
	if err := cat.DropDatabase("metrics"); err != nil {
		t.Fatal("DropDatabase failed:", err)
	}
	if cat.DatabaseExists("metrics") {
		t.Error("expected DatabaseExists=false after drop")
	}
}
