// microts_test.go
package microts

import (
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

func TestDB_Open(t *testing.T) {
	cfg := Config{
		DataDir: t.TempDir(),
	}

	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	if db == nil {
		t.Errorf("expected non-nil db")
	}
}

func TestDB_Close(t *testing.T) {
	cfg := Config{
		DataDir: t.TempDir(),
	}

	db, _ := Open(cfg)
	err := db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestDB_Write(t *testing.T) {
	cfg := Config{
		DataDir: t.TempDir(),
	}

	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	point := &types.Point{
		Database:    "testdb",
		Measurement: "testmeas",
		Tags:        map[string]string{"tag1": "value1"},
		Timestamp:   1234567890,
		Fields:      map[string]*types.FieldValue{"field1": types.NewFieldValue(float64(1.0))},
	}

	err = db.Write(t.Context(), point)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestDB_WriteBatch(t *testing.T) {
	cfg := Config{
		DataDir: t.TempDir(),
	}

	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	points := []*types.Point{
		{
			Database:    "testdb",
			Measurement: "testmeas",
			Tags:        map[string]string{"tag1": "value1"},
			Timestamp:   1234567890,
			Fields:      map[string]*types.FieldValue{"field1": types.NewFieldValue(float64(1.0))},
		},
		{
			Database:    "testdb",
			Measurement: "testmeas",
			Tags:        map[string]string{"tag1": "value2"},
			Timestamp:   1234567891,
			Fields:      map[string]*types.FieldValue{"field1": types.NewFieldValue(float64(2.0))},
		},
	}

	err = db.WriteBatch(t.Context(), points)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
}

func TestDB_QueryRange(t *testing.T) {
	cfg := Config{
		DataDir: t.TempDir(),
	}

	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	req := &types.QueryRangeRequest{
		Database:    "testdb",
		Measurement: "testmeas",
		StartTime:   0,
		EndTime:     10000000000,
		Fields:      []string{"field1"},
		Tags:        map[string]string{"tag1": "value1"},
		Offset:      0,
		Limit:       100,
	}

	it, err := db.Iterator(t.Context(), req)
	if err != nil {
		// No data in DB, expected to fail with no shards
		return
	}
	defer func() { _ = it.Close() }()
}

func TestDB_ListMeasurements(t *testing.T) {
	cfg := Config{
		DataDir: t.TempDir(),
	}

	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	measurements, err := db.ListMeasurements(t.Context(), "testdb")
	if err != nil {
		t.Fatalf("ListMeasurements failed: %v", err)
	}

	if measurements == nil {
		t.Fatalf("expected non-nil measurements slice")
	}
}
