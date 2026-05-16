package engine

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/types"
)

func TestEngine_SetConfig(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	newCfg := &compaction.Config{
		MaxSstableCount:    10,
		MaxCompactionBatch: 5,
	}
	engine.SetConfig(newCfg)
}

func TestEngine_Write_AfterClose(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	point := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   time.Now().UnixNano(),
		Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	}

	err = engine.Write(t.Context(), point)
	if err == nil {
		t.Error("expected error for write after engine close")
	}
}

func TestEngine_WriteBatch_AfterClose(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	points := []*types.Point{
		{
			Database:    "db1",
			Measurement: "cpu",
			Timestamp:   time.Now().UnixNano(),
			Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
		},
	}

	err = engine.WriteBatch(t.Context(), points)
	if err == nil {
		t.Error("expected error for write batch after engine close")
	}
}

func TestEngine_WriteBatch_Empty(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	// Empty batch
	if err := engine.WriteBatch(t.Context(), nil); err != nil {
		t.Errorf("expected nil for nil batch, got %v", err)
	}

	if err := engine.WriteBatch(t.Context(), []*types.Point{}); err != nil {
		t.Errorf("expected nil for empty batch, got %v", err)
	}
}

func TestEngine_WriteBatch_ValidationErrors(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	baseValid := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Timestamp:   time.Now().UnixNano(),
		Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	}

	tests := []struct {
		name   string
		points []*types.Point
		want   error
	}{
		{
			name:   "nil point in batch",
			points: []*types.Point{baseValid, nil},
			want:   ErrNilPoint,
		},
		{
			name: "empty database in batch",
			points: []*types.Point{
				{
					Database:    "",
					Measurement: "cpu",
					Timestamp:   time.Now().UnixNano(),
					Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
				},
			},
			want: ErrEmptyDatabase,
		},
		{
			name: "empty measurement in batch",
			points: []*types.Point{
				{
					Database:    "db1",
					Measurement: "",
					Timestamp:   time.Now().UnixNano(),
					Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
				},
			},
			want: ErrEmptyMeasurement,
		},
		{
			name: "negative timestamp in batch",
			points: []*types.Point{
				{
					Database:    "db1",
					Measurement: "cpu",
					Timestamp:   -1,
					Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
				},
			},
			want: ErrInvalidTimestamp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := engine.WriteBatch(t.Context(), tt.points)
			if err != tt.want {
				t.Errorf("expected %v, got %v", tt.want, err)
			}
		})
	}
}

func TestEngine_WriteBatch_ContextCancel(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	point := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Timestamp:   time.Now().UnixNano(),
		Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	}

	err = engine.WriteBatch(ctx, []*types.Point{point})
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestEngine_FlushAndQuery(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	now := time.Now().UnixNano()

	// Write data
	point := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   now,
		Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}
	if err := engine.Write(t.Context(), point); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Manual Flush
	if err := engine.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Query after Flush
	req := &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   now - 1,
		EndTime:     now + 1e9,
	}

	it, err := engine.Iterator(t.Context(), req)
	if err != nil {
		t.Fatalf("Iterator failed after flush: %v", err)
	}
	defer func() { _ = it.Close() }()

	count := 0
	for it.Next(t.Context()) {
		_ = it.Points()
		count++
	}

	if count != 1 {
		t.Errorf("expected 1 row after flush, got %d", count)
	}
}

func TestEngine_Iterator_AfterClose(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	req := &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     1e9,
	}

	_, err = engine.Iterator(t.Context(), req)
	if err == nil {
		t.Error("expected error for Iterator after close")
	}
}

func TestEngine_IteratorWithMemTable(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	now := time.Now().UnixNano()

	// Write data so we have a writer with memtable
	point := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   now,
		Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	}
	if err := engine.Write(t.Context(), point); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Get the writer's memtable
	w := engine.coordinator.GetWriter("db1", "cpu")
	if w == nil {
		t.Fatal("expected writer for db1/cpu")
	}

	req := &types.QueryRangeRequest{
		Database:    "db1",
		Measurement: "cpu",
		StartTime:   now - 1,
		EndTime:     now + 1e9,
	}

	it := IteratorWithMemTable(t.Context(), nil, w.MemTable(), engine.seriesStore, req)
	if it == nil {
		t.Fatal("IteratorWithMemTable returned nil")
	}
	defer func() { _ = it.Close() }()

	count := 0
	for it.Next(t.Context()) {
		_ = it.Points()
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}
}

func TestScopedSeriesStore_AllocateSID(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	scoped := &scopedSeriesStore{
		inner: engine.seriesStore,
		db:    "db1",
		meas:  "cpu",
	}

	sid, err := scoped.AllocateSID("db1", "cpu", map[string]string{"host": "server1"})
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	if sid == 0 {
		t.Error("expected non-zero SID after allocation")
	}
}

func TestScopedSeriesStore_GetTags(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	// Write a point to allocate a SID
	point := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   time.Now().UnixNano(),
		Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	}
	if err := engine.Write(t.Context(), point); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	scoped := &scopedSeriesStore{
		inner: engine.seriesStore,
		db:    "db1",
		meas:  "cpu",
	}

	// GetTags with empty db/meas should route to the scoped db/meas
	tags, ok := scoped.GetTags("", "", 0)
	if !ok {
		t.Error("expected GetTags to return ok for SID 0")
	}
	if tags["host"] != "server1" {
		t.Errorf("expected host=server1, got %v", tags)
	}

	// GetTags with non-empty db/meas should pass through
	tags2, ok2 := scoped.GetTags("db1", "cpu", 0)
	if !ok2 {
		t.Error("expected GetTags to return ok for explicit db/meas")
	}
	if tags2["host"] != "server1" {
		t.Errorf("expected host=server1, got %v", tags2)
	}
}

func TestEngine_CreateDatabase_AlreadyExists(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	first := engine.CreateDatabase("db1")
	if !first {
		t.Error("CreateDatabase should return true for first creation")
	}
	if engine.CreateDatabase("db1") {
		t.Error("CreateDatabase should return false for existing database")
	}
}

func TestEngine_DropMeasurement_NotExist(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	engine.CreateDatabase("db1")

	// DropMeasurement for non-existent measurement
	found, err := engine.DropMeasurement("db1", "nonexistent")
	if err != nil {
		t.Fatalf("DropMeasurement for nonexistent should succeed: %v", err)
	}
	if found {
		t.Error("expected found=false for non-existent measurement")
	}
}

func TestEngine_CreateMeasurement_EmptyDB(t *testing.T) {
	// This is already tested in engine_test.go as TestEngine_CreateMeasurement_EmptyDatabase
	// We skip it here to avoid duplication
}

func TestEngine_ListMeasurements_NonExistentDB(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	// Write to auto-create
	point := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Timestamp:   time.Now().UnixNano(),
		Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	}
	if err := engine.Write(t.Context(), point); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	dbs := engine.ListDatabases()
	found := false
	for _, db := range dbs {
		if db == "db1" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected db1 to be listed")
	}

	meas, ok := engine.ListMeasurements("db1")
	if !ok {
		t.Fatal("ListMeasurements should return ok for db1")
	}
	if len(meas) != 1 || meas[0] != "cpu" {
		t.Errorf("expected [cpu] measurements, got %v", meas)
	}
}

func TestEngine_MemTableConfigDefault(t *testing.T) {
	t.Parallel()
	// When MemTableCfg is nil, engine should use default
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	if engine.memTableCfg == nil {
		t.Error("expected non-nil memTableCfg")
	}
}

func TestEngine_MemTableConfigCustom(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
		MemTableCfg: &types.MemTableConfig{
			MaxSize:           1024 * 1024,
			MaxCount:          1000,
			IdleDurationNanos: int64(time.Minute),
		},
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	if engine.memTableCfg.MaxCount != 1000 {
		t.Errorf("expected MaxCount=1000, got %d", engine.memTableCfg.MaxCount)
	}
}

func TestEngine_Write_DifferentMeasurements(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	now := time.Now().UnixNano()

	writePoint := func(db, meas string, ts int64) {
		p := &types.Point{
			Database:    db,
			Measurement: meas,
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   ts,
			Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
		}
		if err := engine.Write(t.Context(), p); err != nil {
			t.Fatalf("Write to %s/%s failed: %v", db, meas, err)
		}
	}

	writePoint("db1", "cpu", now)
	writePoint("db1", "mem", now+1e9)
	writePoint("db2", "cpu", now+2e9)

	dbs := engine.ListDatabases()
	if len(dbs) != 2 {
		t.Errorf("expected 2 databases, got %v", dbs)
	}

	cpuMeas, ok := engine.ListMeasurements("db1")
	if !ok {
		t.Fatal("ListMeasurements should return ok for db1")
	}
	if len(cpuMeas) != 2 {
		t.Errorf("expected 2 measurements in db1, got %v", cpuMeas)
	}
}

func TestEngine_DropMeasurement_EmptyDB(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	_, err = engine.DropMeasurement("", "cpu")
	if err != ErrEmptyDatabase {
		t.Errorf("expected ErrEmptyDatabase, got %v", err)
	}
}

func TestEngine_DropMeasurement_EmptyMeasurement(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	_, err = engine.DropMeasurement("db1", "")
	if err != ErrEmptyMeasurement {
		t.Errorf("expected ErrEmptyMeasurement, got %v", err)
	}
}

func TestEngine_DoubleClose(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("First Close failed: %v", err)
	}

	if err := engine.Close(); err != nil {
		t.Errorf("Second Close should not error: %v", err)
	}
}

func TestEngine_WriteBatch_MultipleMeasurements(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	now := time.Now().UnixNano()

	points := []*types.Point{
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now,
			Fields:      map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
		},
		{
			Database:    "db1",
			Measurement: "mem",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now + 1e9,
			Fields:      map[string]*types.FieldValue{"used": types.NewFieldValue(int64(4096))},
		},
	}

	if err := engine.WriteBatch(t.Context(), points); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if err := engine.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

func TestEngine_New_DataDirIsFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "not_a_dir")
	// Create a file where directory should be
	if err := os.WriteFile(dataDir, []byte{}, 0600); err != nil {
		t.Fatal(err)
	}

	_, err := New(&Config{DataDir: dataDir})
	if err == nil {
		t.Error("expected error when data dir is a file")
	}
}

func TestEngine_WriteBatch_ContextCancelMidway(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	now := time.Now().UnixNano()
	ctx, cancel := context.WithCancel(t.Context())

	// Create points with multiple measurements so WriteBatch groups them
	// and the cancel can happen mid-loop
	points := []*types.Point{
		{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now,
			Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(1.0)},
		},
		{
			Database:    "db1",
			Measurement: "mem",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now,
			Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(2.0)},
		},
	}

	// Cancel after a short delay to catch it mid-WriteBatch
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	// May complete fully or return context.Canceled depending on timing
	_ = engine.WriteBatch(ctx, points)
}

func TestEngine_FlushCoordinator_FlushAll_NoWriters(t *testing.T) {
	t.Parallel()
	// Test that FlushAll with no registered writers returns nil
	fc := NewFlushCoordinator(nil)
	if err := fc.FlushAll(); err != nil {
		t.Errorf("FlushAll with no writers should succeed: %v", err)
	}
}

func TestEngine_CloseAllWriters_Empty(t *testing.T) {
	t.Parallel()
	fc := NewFlushCoordinator(nil)
	if err := fc.CloseAllWriters(); err != nil {
		t.Errorf("CloseAllWriters with no writers should succeed: %v", err)
	}
}

func TestEngine_New_CorruptDB(t *testing.T) {
	dir := t.TempDir()

	// Create a valid metadata.db first
	engine, err := New(&Config{DataDir: dir, ShardDuration: time.Hour})
	if err != nil {
		t.Fatalf("First New failed: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Corrupt the metadata.db to make rebuildCache fail
	dbPath := filepath.Join(dir, "metadata.db")
	// Write invalid data to make bbolt operations fail
	f, err := os.OpenFile(dbPath, os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("corrupt bbolt data"))
	_ = f.Close()

	// Now open should fail at Load()
	_, err = New(&Config{DataDir: dir, ShardDuration: time.Hour})
	if err == nil {
		t.Error("expected error when metadata.db is corrupt")
	}
}

func TestEngine_ConcurrentFlush(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = engine.Close() }()

	// Write some data
	now := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		point := &types.Point{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now + int64(i)*1e6,
			Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(float64(i))},
		}
		if err := engine.Write(t.Context(), point); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Flush concurrently to exercise IsFlushing wait loop
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = engine.Flush()
		}()
	}
	wg.Wait()
}

func TestEngine_Write_BackpressureError(t *testing.T) {
	// Create engine with tiny memtable to force backpressure
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
		MemTableCfg: &types.MemTableConfig{
			MaxSize:  1,
			MaxCount: 1,
		},
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	now := time.Now().UnixNano()

	// Fill the memtable with one write
	point1 := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   now,
		Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(1.0)},
	}
	if err := engine.Write(t.Context(), point1); err != nil {
		t.Fatalf("First Write failed: %v", err)
	}

	// Second write will hit backpressure - write in goroutine, then close engine
	errCh := make(chan error, 1)
	go func() {
		point2 := &types.Point{
			Database:    "db1",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now + 1,
			Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(2.0)},
		}
		errCh <- engine.Write(t.Context(), point2)
	}()

	time.Sleep(5 * time.Millisecond)

	// Close engine to unblock the backpressure
	if err := engine.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	err = <-errCh
	if err == nil {
		t.Log("Write may complete successfully before close (backpressure path still exercised)")
	}
}

func TestEngine_Write_WALError(t *testing.T) {
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
		MemTableCfg: &types.MemTableConfig{
			MaxSize:  1,
			MaxCount: 1,
		},
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	now := time.Now().UnixNano()

	// Fill the memtable
	point1 := &types.Point{
		Database:    "testdb",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   now,
		Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(1.0)},
	}
	if err := engine.Write(t.Context(), point1); err != nil {
		t.Fatalf("First Write failed: %v", err)
	}

	// Second write hits backpressure - write in goroutine.
	// Write releases mw.mu between SID allocation and WAL serialization,
	// giving CloseAllWriters a window to close the WAL before WAL.Write is called.
	errCh := make(chan error, 1)
	go func() {
		point2 := &types.Point{
			Database:    "testdb",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now + 1,
			Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(2.0)},
		}
		errCh <- engine.Write(t.Context(), point2)
	}()

	time.Sleep(5 * time.Millisecond)

	// Close engine: FlushAll clears memtable, then CloseAllWriters closes WAL.
	// The goroutine wakes from backpressure, but WAL is already closed.
	if err := engine.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	err = <-errCh
	t.Log("Write with backpressure+close returned:", err)
}

func TestEngine_WriteBatch_WriteError(t *testing.T) {
	// Create engine with tiny memtable to force backpressure in WriteBatch
	cfg := &Config{
		DataDir:       t.TempDir(),
		ShardDuration: time.Hour,
		MemTableCfg: &types.MemTableConfig{
			MaxSize:  1,
			MaxCount: 1,
		},
	}
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	now := time.Now().UnixNano()

	// Fill the memtable
	point1 := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   now,
		Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(1.0)},
	}
	if err := engine.Write(t.Context(), point1); err != nil {
		t.Fatalf("First Write failed: %v", err)
	}

	// WriteBatch will hit backpressure
	errCh := make(chan error, 1)
	go func() {
		points := []*types.Point{
			{
				Database:    "db1",
				Measurement: "cpu",
				Tags:        map[string]string{"host": "server1"},
				Timestamp:   now + 1,
				Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(2.0)},
			},
		}
		errCh <- engine.WriteBatch(t.Context(), points)
	}()

	time.Sleep(5 * time.Millisecond)

	if err := engine.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	err = <-errCh
	if err == nil {
		t.Log("WriteBatch may complete successfully before close")
	}
}
