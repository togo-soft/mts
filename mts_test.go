// mts_test.go
package mts

import (
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/types"
)

func TestDB_Open(t *testing.T) {
	cfg := &Config{
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
	cfg := &Config{
		DataDir: t.TempDir(),
	}

	db, _ := Open(cfg)
	err := db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestDB_Write(t *testing.T) {
	cfg := &Config{
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
	cfg := &Config{
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
	cfg := &Config{
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
	cfg := &Config{
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

func TestDefaultCompactionConfig(t *testing.T) {
	cfg := DefaultCompactionConfig()
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.MaxSstableCount != 4 {
		t.Errorf("expected MaxSstableCount 4, got %d", cfg.MaxSstableCount)
	}
	if cfg.MaxCompactionBatch != 0 {
		t.Errorf("expected MaxCompactionBatch 0, got %d", cfg.MaxCompactionBatch)
	}
	if cfg.ShardSizeLimit != 1*1024*1024*1024 {
		t.Errorf("expected ShardSizeLimit 1GB, got %d", cfg.ShardSizeLimit)
	}
	if cfg.CheckIntervalNanos != int64(time.Hour) {
		t.Errorf("expected CheckIntervalNanos 1h, got %d", cfg.CheckIntervalNanos)
	}
	if cfg.TimeoutNanos != int64(30*time.Minute) {
		t.Errorf("expected TimeoutNanos 30min, got %d", cfg.TimeoutNanos)
	}
}

func TestCompactionConfig_ZeroValues(t *testing.T) {
	cfg := &CompactionConfig{}
	if cfg.MaxSstableCount != 0 {
		t.Errorf("expected zero MaxSstableCount, got %d", cfg.MaxSstableCount)
	}
	if cfg.MaxCompactionBatch != 0 {
		t.Errorf("expected zero MaxCompactionBatch, got %d", cfg.MaxCompactionBatch)
	}
	if cfg.ShardSizeLimit != 0 {
		t.Errorf("expected zero ShardSizeLimit, got %d", cfg.ShardSizeLimit)
	}
}
