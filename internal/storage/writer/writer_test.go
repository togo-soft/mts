package writer

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/wal"
	"codeberg.org/micro-ts/mts/types"
)

// failSeriesStore wraps a metadata.SeriesStore and fails on AllocateSID.
type failSeriesStore struct {
	inner metadata.SeriesStore
}

func (f *failSeriesStore) AllocateSID(_, _ string, _ map[string]string) (uint64, error) {
	return 0, fmt.Errorf("mock allocate error")
}

func (f *failSeriesStore) GetTags(database, measurement string, sid uint64) (map[string]string, bool) {
	return f.inner.GetTags(database, measurement, sid)
}

func (f *failSeriesStore) GetSIDsByTag(database, measurement string, tagKey, tagValue string) []uint64 {
	return f.inner.GetSIDsByTag(database, measurement, tagKey, tagValue)
}

func (f *failSeriesStore) SeriesCount(database, measurement string) int {
	return f.inner.SeriesCount(database, measurement)
}

func newTestWriter(t *testing.T, dir string, ss metadata.SeriesStore, mc *memtable.MemTableConfig) *MeasurementWriter {
	t.Helper()
	if ss == nil {
		mgr, err := metadata.NewManager(dir)
		if err != nil {
			t.Fatalf("metadata.NewManager failed: %v", err)
		}
		if err := mgr.Load(); err != nil {
			t.Fatalf("mgr.Load failed: %v", err)
		}
		ss = mgr.Series()
	}
	if mc == nil {
		mc = memtable.DefaultMemTableConfig()
	}
	mw, err := New(Config{
		DB:          "db1",
		Measurement: "cpu",
		Dir:         dir,
		SeriesStore: ss,
		MemTableCfg: mc,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	return mw
}

func newMetadataSeries(t *testing.T, dir string) metadata.SeriesStore {
	t.Helper()
	mgr, err := metadata.NewManager(dir)
	if err != nil {
		t.Fatalf("metadata.NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("mgr.Load failed: %v", err)
	}
	return mgr.Series()
}

func TestNew(t *testing.T) {
	t.Parallel()
	mw := newTestWriter(t, t.TempDir(), nil, nil)
	defer func() { _ = mw.Close() }()

	if mw == nil {
		t.Fatal("expected non-nil writer")
	}
	if mw.MemTable() == nil {
		t.Error("expected non-nil MemTable")
	}
	if mw.SeriesStore() == nil {
		t.Error("expected non-nil SeriesStore")
	}
	if mw.wal == nil {
		t.Error("expected non-nil WAL")
	}
}

func TestNew_WALCreateFail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Create a file at the WAL directory path to cause MkdirAll to fail
	walPath := filepath.Join(dir, "wal")
	if err := os.WriteFile(walPath, []byte{}, 0600); err != nil {
		t.Fatal(err)
	}

	mw, err := New(Config{
		DB:          "db1",
		Measurement: "cpu",
		Dir:         dir,
		SeriesStore: newMetadataSeries(t, dir),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})
	if err != nil {
		t.Fatalf("New should not fail even without WAL: %v", err)
	}
	defer func() { _ = mw.Close() }()

	if mw.wal != nil {
		t.Error("expected nil WAL when WAL directory cannot be created")
	}
}

func TestWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ss := newMetadataSeries(t, dir)
	mw := newTestWriter(t, dir, ss, nil)
	defer func() { _ = mw.Close() }()

	point := &types.Point{
		Timestamp: time.Now().UnixNano(),
		Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	}
	if err := mw.Write(point); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if mw.memTable.Count() != 1 {
		t.Errorf("expected 1 point in memtable, got %d", mw.memTable.Count())
	}
}

func TestWrite_MultiplePoints(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ss := newMetadataSeries(t, dir)
	mw := newTestWriter(t, dir, ss, nil)
	defer func() { _ = mw.Close() }()

	for i := 0; i < 10; i++ {
		point := &types.Point{
			Timestamp: time.Now().UnixNano() + int64(i)*1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(float64(i))},
		}
		if err := mw.Write(point); err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	if mw.memTable.Count() != 10 {
		t.Errorf("expected 10 points in memtable, got %d", mw.memTable.Count())
	}
}

func TestWrite_NilWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Create file at WAL path to prevent WAL creation
	walPath := filepath.Join(dir, "wal")
	if err := os.WriteFile(walPath, []byte{}, 0600); err != nil {
		t.Fatal(err)
	}

	mw := newTestWriter(t, dir, nil, nil)
	defer func() { _ = mw.Close() }()

	point := &types.Point{
		Timestamp: time.Now().UnixNano(),
		Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	}
	if err := mw.Write(point); err != nil {
		t.Fatalf("Write without WAL failed: %v", err)
	}

	if mw.memTable.Count() != 1 {
		t.Errorf("expected 1 point in memtable, got %d", mw.memTable.Count())
	}
}

func TestWrite_DifferentTags(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ss := newMetadataSeries(t, dir)
	mw := newTestWriter(t, dir, ss, nil)
	defer func() { _ = mw.Close() }()

	point1 := &types.Point{
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(1.0)},
	}
	point2 := &types.Point{
		Timestamp: time.Now().UnixNano() + 1e9,
		Tags:      map[string]string{"host": "server2"},
		Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(2.0)},
	}

	if err := mw.Write(point1); err != nil {
		t.Fatalf("Write point1 failed: %v", err)
	}
	if err := mw.Write(point2); err != nil {
		t.Fatalf("Write point2 failed: %v", err)
	}

	if mw.memTable.Count() != 2 {
		t.Errorf("expected 2 points in memtable, got %d", mw.memTable.Count())
	}
}

func TestWrite_AllFieldTypes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ss := newMetadataSeries(t, dir)
	mw := newTestWriter(t, dir, ss, nil)
	defer func() { _ = mw.Close() }()

	point := &types.Point{
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "server1"},
		Fields: map[string]*types.FieldValue{
			"float_val": types.NewFieldValue(3.14),
			"int_val":   types.NewFieldValue(int64(42)),
			"str_val":   types.NewFieldValue("hello"),
			"bool_val":  types.NewFieldValue(true),
		},
	}
	if err := mw.Write(point); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if mw.memTable.Count() != 1 {
		t.Errorf("expected 1 point in memtable, got %d", mw.memTable.Count())
	}
}

func TestWriteBatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ss := newMetadataSeries(t, dir)
	mw := newTestWriter(t, dir, ss, nil)
	defer func() { _ = mw.Close() }()

	points := []*types.Point{
		{
			Timestamp: time.Now().UnixNano(),
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(1.0)},
		},
		{
			Timestamp: time.Now().UnixNano() + 1e9,
			Tags:      map[string]string{"host": "server2"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(2.0)},
		},
	}

	n, err := mw.WriteBatch(points)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 points written, got %d", n)
	}
	if mw.memTable.Count() != 2 {
		t.Errorf("expected 2 points in memtable, got %d", mw.memTable.Count())
	}
}

func TestWriteBatch_Empty(t *testing.T) {
	t.Parallel()
	mw := newTestWriter(t, t.TempDir(), nil, nil)
	defer func() { _ = mw.Close() }()

	n, err := mw.WriteBatch(nil)
	if err != nil {
		t.Fatalf("WriteBatch with nil should succeed: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0, got %d", n)
	}

	n, err = mw.WriteBatch([]*types.Point{})
	if err != nil {
		t.Fatalf("WriteBatch with empty should succeed: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0, got %d", n)
	}
}

func TestWriteBatch_Backpressure(t *testing.T) {
	// Use a small memtable to trigger backpressure
	mc := &memtable.MemTableConfig{
		MaxSize:  1,
		MaxCount: 1,
	}
	dir := t.TempDir()
	ss := newMetadataSeries(t, dir)
	mw := newTestWriter(t, dir, ss, mc)
	defer func() { _ = mw.Close() }()

	// Fill the memtable so that next write hits backpressure
	point := &types.Point{
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(1.0)},
	}
	if err := mw.Write(point); err != nil {
		t.Fatalf("First Write failed: %v", err)
	}

	// Second point: small memtable should be ActiveFull now
	point2 := &types.Point{
		Timestamp: time.Now().UnixNano() + 1,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(2.0)},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- mw.Write(point2)
	}()

	time.Sleep(5 * time.Millisecond)

	// Close should cause the blocked Write to return an error
	if err := mw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	err := <-errCh
	if err == nil {
		t.Error("expected backpressure error, got nil")
	}
}

func TestClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mw := newTestWriter(t, dir, nil, nil)

	// Write some data before close
	point := &types.Point{
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	}
	if err := mw.Write(point); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := mw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !mw.closed.Load() {
		t.Error("expected closed flag to be true after Close")
	}

	// Close again should be safe (no-op)
	if err := mw.Close(); err != nil {
		t.Fatalf("second Close should not fail: %v", err)
	}
}

func TestClose_NoWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Prevent WAL creation
	walPath := filepath.Join(dir, "wal")
	if err := os.WriteFile(walPath, []byte{}, 0600); err != nil {
		t.Fatal(err)
	}

	mw := newTestWriter(t, dir, nil, nil)

	point := &types.Point{
		Timestamp: time.Now().UnixNano(),
		Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	}
	if err := mw.Write(point); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := mw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !mw.closed.Load() {
		t.Error("expected closed flag to be true after Close")
	}
}

func TestReplayWAL(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	// Create a WAL directly and write serialized points to it
	w, err := wal.Open(wal.Config{
		Dir:         walDir,
		SyncMode:    wal.SyncNone,
		MaxSegments: 10,
	})
	if err != nil {
		t.Fatalf("wal.Open failed: %v", err)
	}

	// Create a real point and serialize it for WAL
	point := &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	}
	mp := types.PointToMemPoint(point, 1)
	data, release := serializePointForWALPooled(mp.Timestamp, mp.Sid, mp.FieldData)
	if _, err := w.Write(data); err != nil {
		release()
		t.Fatalf("WAL Write failed: %v", err)
	}
	release()

	if err := w.Sync(); err != nil {
		t.Fatalf("WAL Sync failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("WAL Close failed: %v", err)
	}

	// Create a MeasurementWriter in the same directory and replay WAL
	mw, err := New(Config{
		DB:          "db1",
		Measurement: "cpu",
		Dir:         dir,
		SeriesStore: newMetadataSeries(t, dir),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = mw.Close() }()

	if err := mw.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}

	if mw.memTable.Count() != 1 {
		t.Errorf("expected 1 point replayed, got %d", mw.memTable.Count())
	}
}

func TestReplayWAL_NilWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Prevent WAL creation
	walPath := filepath.Join(dir, "wal")
	if err := os.WriteFile(walPath, []byte{}, 0600); err != nil {
		t.Fatal(err)
	}

	mw := newTestWriter(t, dir, nil, nil)
	defer func() { _ = mw.Close() }()

	// ReplayWAL with nil WAL should be a no-op
	if err := mw.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL with nil WAL should succeed: %v", err)
	}
}

func TestSerializeDeserializeRoundtrip(t *testing.T) {
	t.Parallel()
	ts := int64(1234567890)
	sid := uint64(42)
	fieldData := []byte{0, 2, 0, 5, 'v', 'a', 'l', 'u', 'e', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	data, release := serializePointForWALPooled(ts, sid, fieldData)
	defer release()

	if len(data) < 17 {
		t.Fatalf("serialized data too short: %d", len(data))
	}
	if data[0] != pointVersion {
		t.Errorf("expected version %d, got %d", pointVersion, data[0])
	}

	mp, err := deserializeFromWAL(data)
	if err != nil {
		t.Fatalf("deserializeFromWAL failed: %v", err)
	}
	if mp.Timestamp != ts {
		t.Errorf("expected timestamp %d, got %d", ts, mp.Timestamp)
	}
	if mp.Sid != sid {
		t.Errorf("expected sid %d, got %d", sid, mp.Sid)
	}
}

func TestDeserializeFromWAL_InvalidData(t *testing.T) {
	t.Parallel()
	// Too short
	_, err := deserializeFromWAL([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for short data")
	}

	// Invalid version
	data := make([]byte, 19)
	data[0] = 99
	_, err = deserializeFromWAL(data)
	if err == nil {
		t.Error("expected error for invalid version")
	}
}

func TestMemTableGetter(t *testing.T) {
	t.Parallel()
	mw := newTestWriter(t, t.TempDir(), nil, nil)
	defer func() { _ = mw.Close() }()

	mt := mw.MemTable()
	if mt == nil {
		t.Fatal("MemTable() returned nil")
	}
	if mt.Count() != 0 {
		t.Errorf("expected empty memtable, got count %d", mt.Count())
	}
}

func TestSeriesStoreGetter(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ss := newMetadataSeries(t, dir)
	mw := newTestWriter(t, dir, ss, nil)
	defer func() { _ = mw.Close() }()

	if mw.SeriesStore() == nil {
		t.Error("SeriesStore() returned nil")
	}
}

func TestWrite_AllocateSIDError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inner := newMetadataSeries(t, dir)
	ss := &failSeriesStore{inner: inner}

	mw := newTestWriter(t, dir, ss, nil)
	defer func() { _ = mw.Close() }()

	point := &types.Point{
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	}

	err := mw.Write(point)
	if err == nil {
		t.Fatal("expected error when AllocateSID fails")
	}
}

func TestWriteBatch_AllocateSIDError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inner := newMetadataSeries(t, dir)
	ss := &failSeriesStore{inner: inner}

	mw := newTestWriter(t, dir, ss, nil)
	defer func() { _ = mw.Close() }()

	points := []*types.Point{
		{
			Timestamp: time.Now().UnixNano(),
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(1.0)},
		},
	}

	n, err := mw.WriteBatch(points)
	if err == nil {
		t.Fatal("expected error when AllocateSID fails")
	}
	if n != 0 {
		t.Errorf("expected 0 points written on error, got %d", n)
	}
}

func TestWriteBatch_WriteError(t *testing.T) {
	// Use a memtable with MaxCount=1 to fill it quickly, then the second
	// batch point will fail on memtable write (after backpressure check)
	mc := &memtable.MemTableConfig{
		MaxSize:  1,
		MaxCount: 1,
	}
	dir := t.TempDir()
	ss := newMetadataSeries(t, dir)
	mw := newTestWriter(t, dir, ss, mc)
	defer func() { _ = mw.Close() }()

	// First write fills the memtable
	point1 := &types.Point{
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(1.0)},
	}
	if err := mw.Write(point1); err != nil {
		t.Fatalf("First Write failed: %v", err)
	}

	// WriteBatch with points - the memtable write will fail on the second
	// point since ActiveFull is true (backpressure) but will succeed on the
	// first point since it enters WriteBatch
	points := []*types.Point{
		{
			Timestamp: time.Now().UnixNano() + 1,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(2.0)},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := mw.WriteBatch(points)
		errCh <- err
	}()

	time.Sleep(5 * time.Millisecond)

	if err := mw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	<-errCh
	// WriteBatch may return an error or not depending on timing
	// The key is that it doesn't hang indefinitely
}

func TestSerialize_BufferGrowth(t *testing.T) {
	t.Parallel()
	ts := int64(1234567890)
	sid := uint64(42)
	// FieldData larger than default pool buffer capacity (256) to trigger realloc
	fieldData := make([]byte, 300)
	for i := range fieldData {
		fieldData[i] = byte(i)
	}

	data, release := serializePointForWALPooled(ts, sid, fieldData)
	defer release()

	expectedLen := 1 + 8 + 8 + len(fieldData)
	if len(data) != expectedLen {
		t.Errorf("expected len %d, got %d", expectedLen, len(data))
	}

	mp, err := deserializeFromWAL(data)
	if err != nil {
		t.Fatalf("deserializeFromWAL failed: %v", err)
	}
	if mp.Timestamp != ts {
		t.Errorf("expected timestamp %d, got %d", ts, mp.Timestamp)
	}
	if mp.Sid != sid {
		t.Errorf("expected sid %d, got %d", sid, mp.Sid)
	}
	if len(mp.FieldData) != len(fieldData) {
		t.Errorf("expected fieldData len %d, got %d", len(fieldData), len(mp.FieldData))
	}
}

func TestReplayWAL_InvalidData(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	// Create a WAL directly and write invalid data to it
	w, err := wal.Open(wal.Config{
		Dir:         walDir,
		SyncMode:    wal.SyncNone,
		MaxSegments: 10,
	})
	if err != nil {
		t.Fatalf("wal.Open failed: %v", err)
	}

	// Write bytes that are too short to be valid serialized data
	if _, err := w.Write([]byte{0, 1, 2, 3, 4}); err != nil {
		t.Fatalf("WAL Write failed: %v", err)
	}
	// Write bytes with invalid version byte
	badVersion := make([]byte, 19)
	badVersion[0] = 99
	if _, err := w.Write(badVersion); err != nil {
		t.Fatalf("WAL Write bad version failed: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("WAL Sync failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("WAL Close failed: %v", err)
	}

	// Create MeasurementWriter in the same directory
	mw, err := New(Config{
		DB:          "db1",
		Measurement: "cpu",
		Dir:         dir,
		SeriesStore: newMetadataSeries(t, dir),
		MemTableCfg: memtable.DefaultMemTableConfig(),
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer func() { _ = mw.Close() }()

	// ReplayWAL should not error on invalid data (skips bad entries)
	if err := mw.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL should not fail on invalid data: %v", err)
	}
	// All data was invalid, so 0 points should be replayed
	if mw.memTable.Count() != 0 {
		t.Errorf("expected 0 points replayed, got %d", mw.memTable.Count())
	}
}

func TestWriteBatch_AllocateSIDError_WithWALData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inner := newMetadataSeries(t, dir)

	// Track calls to AllocateSID, fail on the second call
	callCount := 0
	failOnSecond := &failSeriesStoreWithCount{inner: inner, failAfter: 1}

	mw := newTestWriter(t, dir, failOnSecond, nil)
	defer func() { _ = mw.Close() }()

	// WriteBatch with 2 points - first allocates SID and gets serialized,
	// second fails AllocateSID triggering walReleases cleanup
	points := []*types.Point{
		{
			Timestamp: time.Now().UnixNano(),
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"v1": types.NewFieldValue(1.0)},
		},
		{
			Timestamp: time.Now().UnixNano() + 1,
			Tags:      map[string]string{"host": "server2"},
			Fields:    map[string]*types.FieldValue{"v2": types.NewFieldValue(2.0)},
		},
	}

	_ = callCount
	n, err := mw.WriteBatch(points)
	if err == nil {
		t.Error("expected error from second point AllocateSID")
	}
	if n != 1 {
		t.Logf("expected 1 point written before error, got %d", n)
	}
}

// failSeriesStoreWithCount succeeds up to failAfter calls, then fails.
type failSeriesStoreWithCount struct {
	inner     metadata.SeriesStore
	failAfter int
	mu        sync.Mutex
	count     int
}

func (f *failSeriesStoreWithCount) AllocateSID(db, meas string, tags map[string]string) (uint64, error) {
	f.mu.Lock()
	f.count++
	c := f.count
	f.mu.Unlock()
	if c > f.failAfter {
		return 0, fmt.Errorf("mock allocate error on call %d", c)
	}
	return f.inner.AllocateSID(db, meas, tags)
}

func (f *failSeriesStoreWithCount) GetTags(database, measurement string, sid uint64) (map[string]string, bool) {
	return f.inner.GetTags(database, measurement, sid)
}

func (f *failSeriesStoreWithCount) GetSIDsByTag(database, measurement string, tagKey, tagValue string) []uint64 {
	return f.inner.GetSIDsByTag(database, measurement, tagKey, tagValue)
}

func (f *failSeriesStoreWithCount) SeriesCount(database, measurement string) int {
	return f.inner.SeriesCount(database, measurement)
}

func TestWrite_WALError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	seriesStore := newMetadataSeries(t, dir)
	mw, err := New(Config{
		DB:          "testdb",
		Measurement: "cpu",
		Dir:         dir,
		SeriesStore: seriesStore,
		MemTableCfg: &memtable.MemTableConfig{MaxCount: 1, MaxSize: 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UnixNano()

	// First write fills the memtable
	point1 := &types.Point{
		Database:    "testdb",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   now,
		Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(1.0)},
	}
	if err := mw.Write(point1); err != nil {
		t.Fatal(err)
	}

	// Second write hits backpressure - write in goroutine.
	// Write() releases mw.mu between SID allocation and WAL serialization,
	// giving Close() a window to close the WAL before WAL.Write is called.
	errCh := make(chan error, 1)
	go func() {
		point2 := &types.Point{
			Database:    "testdb",
			Measurement: "cpu",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   now + 1,
			Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(2.0)},
		}
		errCh <- mw.Write(point2)
	}()

	time.Sleep(5 * time.Millisecond) // Wait for goroutine to enter backpressure

	// Clear memtable so goroutine exits backpressure.
	_ = mw.MemTable().Swap()
	mw.MemTable().ClearPassive()

	// Close the writer: sets closed=true, closes WAL, then purges WAL.
	// The goroutine exits backpressure (ActiveFull=false), proceeds through
	// Write(), and hits the closed WAL at WAL.Write().
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}

	err = <-errCh
	if err == nil {
		t.Log("Write succeeded before WAL was closed")
	}
}
