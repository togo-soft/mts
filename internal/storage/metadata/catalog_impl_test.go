package metadata

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func openTestDB(t *testing.T) (*bolt.DB, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "metadata.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, dir
}

func TestCatalogStore_CreateDatabase_Success(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	if err := cs.CreateDatabase("metrics"); err != nil {
		t.Fatal("CreateDatabase failed:", err)
	}
	if !cs.DatabaseExists("metrics") {
		t.Error("expected DatabaseExists=true")
	}
}

func TestCatalogStore_CreateDatabase_EmptyName(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	err := cs.CreateDatabase("")
	if err == nil {
		t.Error("expected error for empty database name")
	}
}

func TestCatalogStore_CreateDatabase_Duplicate(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	if err := cs.CreateDatabase("metrics"); err != nil {
		t.Fatal("duplicate CreateDatabase should succeed:", err)
	}
}

func TestCatalogStore_DropDatabase_Success(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	if err := cs.DropDatabase("metrics"); err != nil {
		t.Fatal("DropDatabase failed:", err)
	}
	if cs.DatabaseExists("metrics") {
		t.Error("expected DatabaseExists=false after drop")
	}
}

func TestCatalogStore_DropDatabase_NotFound(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	err := cs.DropDatabase("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent database")
	}
}

func TestCatalogStore_ListDatabases_Empty(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	names := cs.ListDatabases()
	if len(names) != 0 {
		t.Errorf("expected 0 databases, got %v", names)
	}
}

func TestCatalogStore_ListDatabases_Multiple(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("db2")
	_ = cs.CreateDatabase("db1")
	_ = cs.CreateDatabase("db3")

	names := cs.ListDatabases()
	if len(names) != 3 {
		t.Fatalf("expected 3 databases, got %d", len(names))
	}
	// should be sorted
	if names[0] != "db1" || names[1] != "db2" || names[2] != "db3" {
		t.Errorf("expected sorted names, got %v", names)
	}
}

func TestCatalogStore_ListDatabases_SkipsInternal(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")

	// create an internal-looking bucket (starts with _)
	_ = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("_internal"))
		return err
	})

	names := cs.ListDatabases()
	if len(names) != 1 {
		t.Errorf("expected 1 database (internal skipped), got %d: %v", len(names), names)
	}
}

func TestCatalogStore_DatabaseExists_False(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	if cs.DatabaseExists("nonexistent") {
		t.Error("expected false for nonexistent database")
	}
}

func TestCatalogStore_CreateMeasurement_Success(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	if err := cs.CreateMeasurement("metrics", "cpu"); err != nil {
		t.Fatal("CreateMeasurement failed:", err)
	}
	if !cs.MeasurementExists("metrics", "cpu") {
		t.Error("expected MeasurementExists=true")
	}
}

func TestCatalogStore_CreateMeasurement_EmptyDatabase(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	err := cs.CreateMeasurement("", "cpu")
	if err == nil {
		t.Error("expected error for empty database")
	}
}

func TestCatalogStore_CreateMeasurement_EmptyName(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	err := cs.CreateMeasurement("metrics", "")
	if err == nil {
		t.Error("expected error for empty measurement name")
	}
}

func TestCatalogStore_CreateMeasurement_DatabaseNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	err := cs.CreateMeasurement("nonexistent", "cpu")
	if err == nil {
		t.Error("expected error for nonexistent database")
	}
}

func TestCatalogStore_CreateMeasurement_Duplicate(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	_ = cs.CreateMeasurement("metrics", "cpu")
	if err := cs.CreateMeasurement("metrics", "cpu"); err != nil {
		t.Fatal("duplicate CreateMeasurement should succeed:", err)
	}
}

func TestCatalogStore_DropMeasurement_Success(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	_ = cs.CreateMeasurement("metrics", "cpu")
	if err := cs.DropMeasurement("metrics", "cpu"); err != nil {
		t.Fatal("DropMeasurement failed:", err)
	}
	if cs.MeasurementExists("metrics", "cpu") {
		t.Error("expected MeasurementExists=false after drop")
	}
}

func TestCatalogStore_DropMeasurement_DatabaseNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	err := cs.DropMeasurement("nonexistent", "cpu")
	if err == nil {
		t.Error("expected error for nonexistent database")
	}
}

func TestCatalogStore_DropMeasurement_MeasurementNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	err := cs.DropMeasurement("metrics", "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent measurement")
	}
}

func TestCatalogStore_ListMeasurements_Success(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	_ = cs.CreateMeasurement("metrics", "cpu")
	_ = cs.CreateMeasurement("metrics", "mem")

	names, err := cs.ListMeasurements("metrics")
	if err != nil {
		t.Fatal("ListMeasurements failed:", err)
	}
	if len(names) != 2 {
		t.Fatalf("expected 2 measurements, got %d", len(names))
	}
	if names[0] != "cpu" || names[1] != "mem" {
		t.Errorf("expected sorted [cpu mem], got %v", names)
	}
}

func TestCatalogStore_ListMeasurements_DatabaseNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_, err := cs.ListMeasurements("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent database")
	}
}

func TestCatalogStore_ListMeasurements_Empty(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	names, err := cs.ListMeasurements("metrics")
	if err != nil {
		t.Fatal("ListMeasurements failed:", err)
	}
	if len(names) != 0 {
		t.Errorf("expected 0 measurements, got %d", len(names))
	}
}

func TestCatalogStore_MeasurementExists_False(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	if cs.MeasurementExists("metrics", "nonexistent") {
		t.Error("expected false for nonexistent measurement")
	}
}

func TestCatalogStore_MeasurementExists_DatabaseNotExist(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	if cs.MeasurementExists("nonexistent", "cpu") {
		t.Error("expected false when database doesn't exist")
	}
}

func TestCatalogStore_GetRetention_Default(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	_ = cs.CreateMeasurement("metrics", "cpu")

	d, err := cs.GetRetention("metrics", "cpu")
	if err != nil {
		t.Fatal("GetRetention failed:", err)
	}
	if d != 0 {
		t.Errorf("expected default 0 retention, got %v", d)
	}
}

func TestCatalogStore_SetRetention_Success(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	_ = cs.CreateMeasurement("metrics", "cpu")

	retention := 24 * time.Hour
	if err := cs.SetRetention("metrics", "cpu", retention); err != nil {
		t.Fatal("SetRetention failed:", err)
	}

	d, err := cs.GetRetention("metrics", "cpu")
	if err != nil {
		t.Fatal("GetRetention failed:", err)
	}
	if d != retention {
		t.Errorf("expected %v, got %v", retention, d)
	}
}

func TestCatalogStore_SetRetention_DatabaseNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	err := cs.SetRetention("nonexistent", "cpu", time.Hour)
	if err == nil {
		t.Error("expected error for nonexistent database")
	}
}

func TestCatalogStore_SetRetention_MeasurementNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	err := cs.SetRetention("metrics", "nonexistent", time.Hour)
	if err == nil {
		t.Error("expected error for nonexistent measurement")
	}
}

func TestCatalogStore_GetRetention_DatabaseNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_, err := cs.GetRetention("nonexistent", "cpu")
	if err == nil {
		t.Error("expected error for nonexistent database")
	}
}

func TestCatalogStore_GetRetention_MeasurementNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	_, err := cs.GetRetention("metrics", "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent measurement")
	}
}

func TestCatalogStore_GetSchema_None(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	_ = cs.CreateMeasurement("metrics", "cpu")

	s, err := cs.GetSchema("metrics", "cpu")
	if err != nil {
		t.Fatal("GetSchema failed:", err)
	}
	if s != nil {
		t.Errorf("expected nil schema, got %+v", s)
	}
}

func TestCatalogStore_SetSchema_Success(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	_ = cs.CreateMeasurement("metrics", "cpu")

	schema := &Schema{
		Version: 1,
		Fields:  []FieldDef{{Name: "usage", Type: 2}},
		TagKeys: []string{"host"},
	}
	if err := cs.SetSchema("metrics", "cpu", schema); err != nil {
		t.Fatal("SetSchema failed:", err)
	}

	s, err := cs.GetSchema("metrics", "cpu")
	if err != nil {
		t.Fatal("GetSchema failed:", err)
	}
	if s == nil {
		t.Fatal("expected non-nil schema")
	}
	if s.Version != 1 {
		t.Errorf("expected version 1, got %d", s.Version)
	}
	if len(s.Fields) != 1 || s.Fields[0].Name != "usage" {
		t.Errorf("unexpected fields: %+v", s.Fields)
	}
}

func TestCatalogStore_SetSchema_Nil(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	_ = cs.CreateMeasurement("metrics", "cpu")

	err := cs.SetSchema("metrics", "cpu", nil)
	if err == nil {
		t.Error("expected error for nil schema")
	}
}

func TestCatalogStore_SetSchema_IncompatibleTypeChange(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	_ = cs.CreateMeasurement("metrics", "cpu")

	oldSchema := &Schema{
		Version: 1,
		Fields:  []FieldDef{{Name: "usage", Type: 2}},
	}
	_ = cs.SetSchema("metrics", "cpu", oldSchema)

	newSchema := &Schema{
		Version: 2,
		Fields:  []FieldDef{{Name: "usage", Type: 3}},
	}
	err := cs.SetSchema("metrics", "cpu", newSchema)
	if err == nil {
		t.Error("expected error for incompatible type change")
	}
}

func TestCatalogStore_SetSchema_DatabaseNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	err := cs.SetSchema("nonexistent", "cpu", &Schema{Version: 1})
	if err == nil {
		t.Error("expected error for nonexistent database")
	}
}

func TestCatalogStore_GetSchema_DatabaseNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_, err := cs.GetSchema("nonexistent", "cpu")
	if err == nil {
		t.Error("expected error for nonexistent database")
	}
}

func TestCatalogStore_GetSchema_MeasurementNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	_, err := cs.GetSchema("metrics", "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent measurement")
	}
}

func TestValidateSchemaUpdate_Compatible(t *testing.T) {
	old := &Schema{Fields: []FieldDef{{Name: "a", Type: 1}}}
	new := &Schema{Fields: []FieldDef{{Name: "a", Type: 1}, {Name: "b", Type: 2}}}
	if err := validateSchemaUpdate(old, new); err != nil {
		t.Error("expected no error for compatible change")
	}
}

func TestValidateSchemaUpdate_Incompatible(t *testing.T) {
	old := &Schema{Fields: []FieldDef{{Name: "a", Type: 1}}}
	new := &Schema{Fields: []FieldDef{{Name: "a", Type: 2}}}
	if err := validateSchemaUpdate(old, new); err == nil {
		t.Error("expected error for incompatible type change")
	}
}

func TestCatalog_DropDatabase_WithMeasurements(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	_ = cs.CreateMeasurement("metrics", "cpu")
	_ = cs.CreateMeasurement("metrics", "mem")

	if err := cs.DropDatabase("metrics"); err != nil {
		t.Fatal("DropDatabase with measurements failed:", err)
	}
	if cs.DatabaseExists("metrics") {
		t.Error("expected DatabaseExists=false after drop")
	}
}

func TestCatalog_DropDatabase_WithSchema(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)

	_ = cs.CreateDatabase("metrics")
	_ = cs.CreateMeasurement("metrics", "cpu")
	_ = cs.SetSchema("metrics", "cpu", &Schema{
		Version: 1,
		Fields:  []FieldDef{{Name: "v", Type: 1}},
	})

	if err := cs.DropDatabase("metrics"); err != nil {
		t.Fatal("DropDatabase with schema failed:", err)
	}
}

func TestCatalogStore_NewManagerFromExisting(t *testing.T) {
	dir := t.TempDir()
	m1, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}
	_ = m1.Catalog().CreateDatabase("testdb")
	_ = m1.Close()

	// reopen
	m2, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager reopen failed:", err)
	}
	defer func() { _ = m2.Close() }()
	if err := m2.Load(); err != nil {
		t.Fatal("Load failed:", err)
	}
	if !m2.Catalog().DatabaseExists("testdb") {
		t.Error("expected testdb to exist after reopen")
	}
}

func TestCatalogStore_ListDatabases_SkipsInternalFiles(t *testing.T) {
	dir := t.TempDir()
	// create a file starting with _ in data dir to verify it's not treated as database
	f, _ := os.Create(filepath.Join(dir, "_metadata_old.db"))
	_ = f.Close()

	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("metrics")

	names := cs.ListDatabases()
	if len(names) != 1 || names[0] != "metrics" {
		t.Errorf("expected [metrics], got %v", names)
	}
}
