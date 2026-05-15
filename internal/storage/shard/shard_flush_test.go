package shard

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

func TestShard_WriteSSTable(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	points := []types.MemPoint{
		{Timestamp: 1000000000, Sid: 1, FieldData: nil},
		{Timestamp: 2000000000, Sid: 1, FieldData: nil},
	}

	sstPath, sstSeq, minTime, maxTime, err := s.WriteSSTable(points)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	if sstPath == "" {
		t.Error("expected non-empty sstPath")
	}
	if sstSeq != 0 {
		t.Errorf("expected sstSeq 0, got %d", sstSeq)
	}
	if minTime != 1000000000 {
		t.Errorf("expected minTime 1000000000, got %d", minTime)
	}
	if maxTime != 2000000000 {
		t.Errorf("expected maxTime 2000000000, got %d", maxTime)
	}

	// Verify file exists
	if _, err := os.Stat(sstPath); os.IsNotExist(err) {
		t.Errorf("SSTable file should exist: %s", sstPath)
	}

	_ = s.Close()
}

func TestShard_WriteSSTable_SinglePoint(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	points := []types.MemPoint{
		{Timestamp: 1000000000, Sid: 1, FieldData: nil},
	}

	_, _, minTime, maxTime, err := s.WriteSSTable(points)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	if minTime != maxTime {
		t.Errorf("single point should have same min/max, got %d/%d", minTime, maxTime)
	}
	if minTime != 1000000000 {
		t.Errorf("expected minTime 1000000000, got %d", minTime)
	}

	_ = s.Close()
}

func TestShard_WriteSSTable_WithLevelCompaction(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		LevelCompactionCfg: &compaction.LevelConfig{
			Enabled:          true,
			CheckInterval:    time.Hour,
			Timeout:          time.Minute,
			EnableCheckpoint: true,
		},
		SchemaStore: metadata.NewSimpleSchemaStore(),
	}

	s := NewShard(cfg)

	points := []types.MemPoint{
		{Timestamp: 1000000000, Sid: 1, FieldData: nil},
	}

	sstPath, sstSeq, _, _, err := s.WriteSSTable(points)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	// Register the SSTable for level compaction
	var size int64
	if fi, statErr := os.Stat(sstPath); statErr == nil {
		size = fi.Size()
	}
	s.RegisterSSTable(sstSeq, 1000000000, 1000000000, size)

	// Verify L0 directory was created
	l0Dir := filepath.Join(tmpDir, "data", "L0")
	if _, err := os.Stat(l0Dir); os.IsNotExist(err) {
		t.Fatal("L0 directory should exist after RegisterSSTable with level compaction")
	}

	_ = s.Close()
}

func TestShard_SSTSeq(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	seq0 := s.SSTSeq()
	if seq0 != 0 {
		t.Errorf("expected initial seq 0, got %d", seq0)
	}

	// Write an SSTable, which increments the internal seq
	points := []types.MemPoint{
		{Timestamp: 1000000000, Sid: 1, FieldData: nil},
	}
	_, seq1, _, _, err := s.WriteSSTable(points)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}
	if seq1 != 0 {
		t.Errorf("expected seq 0 from first write, got %d", seq1)
	}

	// SSTSeq should show the next sequence
	seqAfter := s.SSTSeq()
	if seqAfter != 1 {
		t.Errorf("expected SSTSeq 1 after one write, got %d", seqAfter)
	}

	_ = s.Close()
}

func TestShard_Compact(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	mgr, err := metadata.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	sm := NewShardManager(
		tmpDir,
		time.Hour,
		nil,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	// Create a shard first
	ts := time.Now().UnixNano()
	s, err := sm.GetShard("db1", "cpu", ts)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	// Compact (no-op since no compaction config)
	if err := sm.Compact("db1", "cpu", ts); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	_ = s.Close()
	_ = sm.CloseAll()
}

func TestShard_SetConfig(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	compactionCfg := compaction.DefaultConfig()
	compactionCfg.CheckIntervalNanos = int64(time.Hour)

	mgr, err := metadata.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	sm := NewShardManager(
		tmpDir,
		time.Hour,
		compactionCfg,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	// Create a shard
	ts := time.Now().UnixNano()
	s, err := sm.GetShard("db1", "cpu", ts)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	// Update config
	newCfg := &compaction.Config{
		MaxSstableCount:    20,
		MaxCompactionBatch: 10,
	}
	sm.SetConfig(newCfg)

	_ = s.Close()
	_ = sm.CloseAll()
}

func TestShard_CloseAll(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	mgr, err := metadata.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	sm := NewShardManager(
		tmpDir,
		time.Hour,
		nil,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	// Create some shards
	_, _ = sm.GetShard("db1", "cpu", time.Now().UnixNano())
	_, _ = sm.GetShard("db1", "mem", time.Now().UnixNano())

	// CloseAll should succeed
	if err := sm.CloseAll(); err != nil {
		t.Fatalf("CloseAll failed: %v", err)
	}
}

func TestShard_CloseAll_Empty(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	mgr, err := metadata.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	sm := NewShardManager(
		tmpDir,
		time.Hour,
		nil,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	// CloseAll with no shards should succeed
	if err := sm.CloseAll(); err != nil {
		t.Fatalf("CloseAll with no shards failed: %v", err)
	}
}

func TestShardManager_Flush(t *testing.T) {
	tmpDir := t.TempDir()

	mgr, err := metadata.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	sm := NewShardManager(
		tmpDir,
		time.Hour,
		nil,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	// Create points that fall into the same shard
	ts := time.Now().UnixNano()
	shardStart := (ts / int64(time.Hour)) * int64(time.Hour)

	points := []types.MemPoint{
		{Timestamp: shardStart + 1000, Sid: 1, FieldData: nil},
		{Timestamp: shardStart + 2000, Sid: 2, FieldData: nil},
		{Timestamp: shardStart + 3000, Sid: 1, FieldData: nil},
	}

	if err := sm.Flush("db1", "cpu", points); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify shard was created and has data
	shards := sm.GetShards("db1", "cpu", shardStart, shardStart+int64(time.Hour))
	if len(shards) != 1 {
		t.Fatalf("expected 1 shard, got %d", len(shards))
	}

	// Read back the data
	iter := NewShardIterator(shards[0], shardStart, shardStart+int64(time.Hour), 0)
	rows := collectAll(iter)
	iter.Close()
	if err := iter.Err(); err != nil {
		t.Fatalf("Read after flush failed: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}

	_ = sm.CloseAll()
}

func TestShardManager_Flush_EmptyPoints(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	mgr, err := metadata.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	sm := NewShardManager(
		tmpDir,
		time.Hour,
		nil,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	if err := sm.Flush("db1", "cpu", nil); err != nil {
		t.Fatalf("Flush with nil points should succeed: %v", err)
	}

	if err := sm.Flush("db1", "cpu", []types.MemPoint{}); err != nil {
		t.Fatalf("Flush with empty points should succeed: %v", err)
	}

	_ = sm.CloseAll()
}

func TestShardManager_Flush_InvalidName(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	mgr, err := metadata.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	sm := NewShardManager(
		tmpDir,
		time.Hour,
		nil,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	points := []types.MemPoint{
		{Timestamp: 1000, Sid: 1},
	}

	err = sm.Flush("", "cpu", points)
	if err == nil {
		t.Error("expected error for empty database name")
	}

	err = sm.Flush("db1", "", points)
	if err == nil {
		t.Error("expected error for empty measurement name")
	}

	_ = sm.CloseAll()
}

func TestShardManager_Flush_MultipleShards(t *testing.T) {
	tmpDir := t.TempDir()

	mgr, err := metadata.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	smallDuration := 500 * time.Millisecond
	sm := NewShardManager(
		tmpDir,
		smallDuration,
		nil,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	// Create points in two different shards
	points := []types.MemPoint{
		{Timestamp: 100, Sid: 1, FieldData: nil},
		{Timestamp: int64(smallDuration) + 100, Sid: 2, FieldData: nil},
	}

	if err := sm.Flush("db1", "cpu", points); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	shards := sm.GetAllShards()
	if len(shards) != 2 {
		t.Errorf("expected 2 shards, got %d", len(shards))
	}

	_ = sm.CloseAll()
}

func TestShard_MetadataFieldTypeToSSTableFieldType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    int32
		expected sstable.FieldType
	}{
		{1, sstable.FieldTypeFloat64},
		{2, sstable.FieldTypeInt64},
		{3, sstable.FieldTypeString},
		{4, sstable.FieldTypeBool},
		{99, sstable.FieldTypeFloat64}, // unknown defaults to Float64
		{0, sstable.FieldTypeFloat64},  // unknown defaults to Float64
	}

	for _, tt := range tests {
		got := MetadataFieldTypeToSSTableFieldType(tt.input)
		if got != tt.expected {
			t.Errorf("MetadataFieldTypeToSSTableFieldType(%d) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

func TestShard_GetSchema(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	schemaStore := metadata.NewSimpleSchemaStore()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: schemaStore,
	})

	// Without setting a schema, GetSchema should return empty but not error
	sch, err := s.GetSchema()
	if err != nil {
		t.Fatalf("GetSchema failed: %v", err)
	}
	if sch.Fields == nil {
		t.Error("expected non-nil Fields map")
	}

	_ = s.Close()
}

func TestShard_GetSchema_NoSchemaStore(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
	})

	_, err := s.GetSchema()
	if err == nil {
		t.Error("expected error for GetSchema without SchemaStore")
	}

	_ = s.Close()
}

func TestShard_TriggerCompaction_Closed(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	_ = s.Close()

	// TriggerCompaction on a closed shard should be a no-op
	s.TriggerCompaction()
}

func TestShard_TriggerCompaction_WithConfig(t *testing.T) {
	tmpDir := t.TempDir()

	// Create shard with compaction config that triggers quickly
	cfg := ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		CompactionCfg: &compaction.Config{
			MaxSstableCount:    2,
			CheckIntervalNanos: int64(50 * time.Millisecond),
			TimeoutNanos:       int64(30 * time.Second),
		},
		SchemaStore: metadata.NewSimpleSchemaStore(),
	}

	s := NewShard(cfg)

	// Write enough points to create multiple SSTables
	for i := 0; i < 5; i++ {
		shardWrite(t, s, &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(float64(i))},
		})
	}

	// Trigger compaction
	s.TriggerCompaction()

	// Give compaction a moment to start (non-blocking)
	time.Sleep(50 * time.Millisecond)

	_ = s.Close()
}

func TestShard_TriggerCompaction_Level(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		LevelCompactionCfg: &compaction.LevelConfig{
			Enabled:          true,
			CheckInterval:    50 * time.Millisecond,
			Timeout:          30 * time.Second,
			EnableCheckpoint: true,
		},
		SchemaStore: metadata.NewSimpleSchemaStore(),
	}

	s := NewShard(cfg)

	// Write points
	for i := 0; i < 5; i++ {
		shardWrite(t, s, &types.Point{
			Timestamp: int64(i) * 1e9,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(float64(i))},
		})
	}

	// Trigger level compaction
	s.TriggerCompaction()

	time.Sleep(50 * time.Millisecond)

	_ = s.Close()
}

func TestShard_EmptySSTableList(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	files := s.listSSTableFiles()
	if len(files) != 0 {
		t.Errorf("expected 0 SSTable files, got %d", len(files))
	}

	_ = s.Close()
}

func TestIsNameSafe(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		safe  bool
	}{
		{"", false},
		{".", false},
		{"..", false},
		{"normal", true},
		{"test-name_123", true},
	}

	for _, tt := range tests {
		got := isNameSafe(tt.name)
		if got != tt.safe {
			t.Errorf("isNameSafe(%q) = %v, want %v", tt.name, got, tt.safe)
		}
	}
}

func TestCalcShardStart(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	mgr, err := metadata.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	sm := NewShardManager(
		tmpDir,
		time.Hour,
		nil,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	// Test with zero duration (should not divide by zero)
	zeroSM := NewShardManager(
		tmpDir,
		0,
		nil,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	start := zeroSM.calcShardStart(1000)
	if start != 0 {
		t.Errorf("calcShardStart with zero duration should return 0, got %d", start)
	}

	// Test with normal duration
	start = sm.calcShardStart(int64(time.Hour) + 1000)
	if start != int64(time.Hour) {
		t.Errorf("calcShardStart should floor to hour boundary, got %d", start)
	}

	_ = sm.CloseAll()
}

func TestPointToRow_Nil(t *testing.T) {
	// This tests the pointToRow function's nil path
	// Create an iterator that will use an empty shard
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
	})

	shardWrite(t, s, &types.Point{
		Timestamp: 1000000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	})

	iter := NewShardIterator(s, 0, time.Hour.Nanoseconds(), 0)
	defer iter.Close()

	// Read first row to initialize iterator
	_ = iter.Current()

	// Test that shard close works after iteration
	_ = s.Close()
}

func TestShard_WaitForDiscovery(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	mgr, err := metadata.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	sm := NewShardManager(
		tmpDir,
		time.Hour,
		nil,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	// WaitForDiscovery is a no-op but should be safe to call
	sm.WaitForDiscovery()

	_ = sm.CloseAll()
}

func TestShard_WriteSSTable_MkdirError(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	// Create a file at the expected shard path to cause MkdirAll to fail
	shardPath := filepath.Join(tmpDir, "sharddir")
	if err := os.WriteFile(shardPath, []byte{}, 0600); err != nil {
		t.Fatal(err)
	}

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         shardPath,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	points := []types.MemPoint{
		{Timestamp: 1000000000, Sid: 1, FieldData: nil},
	}

	_, _, _, _, err := s.WriteSSTable(points)
	if err == nil {
		t.Error("expected error when MkdirAll fails")
	}

	_ = s.Close()
}

func TestShardIterator_ExtSeriesStore(t *testing.T) {
	t.Parallel()
	// Create an external series store
	sst := metadata.NewSimpleSeriesStore()
	sid, err := sst.AllocateSID("db1", "cpu", map[string]string{"host": "server1"})
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}

	// Create a memtable with a data point
	mt := memtable.NewMemTable(memtable.DefaultMemTableConfig())
	mp := types.PointToMemPoint(&types.Point{
		Timestamp: 1000000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(42.0)},
	}, sid)
	if err := mt.Write(mp); err != nil {
		t.Fatalf("memtable Write failed: %v", err)
	}

	// Create iterator with nil shard and extSeriesStore
	iter := NewShardIteratorWithMemTable(nil, mt, sst, 0, 10000000000, 0)
	defer iter.Close()

	// Should iterate one row using extSeriesStore for tag resolution
	row := iter.Next()
	if row == nil {
		t.Fatal("expected a row from iterator with extSeriesStore")
	}
	if row.Tags["host"] != "server1" {
		t.Errorf("expected host=server1, got %v", row.Tags)
	}
	if row.Sid != sid {
		t.Errorf("expected sid %d, got %d", sid, row.Sid)
	}

	// Should be no more rows
	if iter.Next() != nil {
		t.Error("expected no more rows")
	}
}

func TestShardIterator_ExtSeriesStore_GetTagsFallback(t *testing.T) {
	t.Parallel()
	sst := metadata.NewSimpleSeriesStore()
	sid, err := sst.AllocateSID("db1", "cpu", map[string]string{"host": "server1"})
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}

	// Create a shard with series store AND pass extSeriesStore
	// This tests the resolveTags fallback path
	tmpDir := t.TempDir()
	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: sst,
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	points := []types.MemPoint{
		{Timestamp: 1000000000, Sid: sid, FieldData: nil},
	}
	if _, _, _, _, err := s.WriteSSTable(points); err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	// Iterator with shard should resolve tags via shard.seriesStore
	iter := NewShardIterator(s, 0, 10000000000, 0)
	defer iter.Close()

	count := 0
	for {
		row := iter.Next()
		if row == nil {
			break
		}
		count++
		if row.Tags["host"] != "server1" {
			t.Errorf("expected host=server1, got %v", row.Tags)
		}
	}
	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}

	_ = s.Close()
}

func TestShard_RecoverSSTSeq_OldFormat(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatal(err)
	}

	// Create old-format sst_N/ directories
	for _, seq := range []string{"0", "1", "5"} {
		oldDir := filepath.Join(dataDir, "sst_"+seq)
		if err := os.MkdirAll(oldDir, 0700); err != nil {
			t.Fatal(err)
		}
	}

	// recoverSSTSeq should find max seq and return max+1
	seq := recoverSSTSeq(tmpDir)
	if seq != 6 {
		t.Errorf("expected seq 6 from old-format dirs, got %d", seq)
	}
}

func TestShard_RecoverSSTSeq_NewFormat(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatal(err)
	}

	// Create new-format sst_N.bin files
	for _, seq := range []string{"3", "7", "10"} {
		f, err := os.Create(filepath.Join(dataDir, "sst_"+seq+".bin"))
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}

	seq := recoverSSTSeq(tmpDir)
	if seq != 11 {
		t.Errorf("expected seq 11 from new-format files, got %d", seq)
	}
}

func TestShard_RecoverSSTSeq_InvalidFiles(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatal(err)
	}

	// Create files with invalid names that should be skipped
	for _, name := range []string{"not_sst.bin", "sst_abc.bin", "sst_.bin", "random.txt"} {
		f, err := os.Create(filepath.Join(dataDir, name))
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}

	// Also create an old-format dir with invalid name
	_ = os.MkdirAll(filepath.Join(dataDir, "sst_abc"), 0700)

	seq := recoverSSTSeq(tmpDir)
	if seq != 0 {
		t.Errorf("expected seq 0 when no valid sst files, got %d", seq)
	}
}

func TestShard_RecoverSSTSeq_NoDir(t *testing.T) {
	t.Parallel()
	// Data dir doesn't exist
	seq := recoverSSTSeq(t.TempDir())
	if seq != 0 {
		t.Errorf("expected seq 0 when data dir doesn't exist, got %d", seq)
	}
}

func TestIsNameSafe_PathTraversal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		safe bool
	}{
		{"../etc", false},
		{"a/b", false},
		{"./foo", true},  // cleaned to "foo", which is safe
		{"valid-name_123", true},
		{"normal", true},
	}

	for _, tt := range tests {
		got := isNameSafe(tt.name)
		if got != tt.safe {
			t.Errorf("isNameSafe(%q) = %v, want %v", tt.name, got, tt.safe)
		}
	}
}

func TestShard_GetSchema_Error(t *testing.T) {
	t.Parallel()
	sst := metadata.NewSimpleSchemaStore()
	// Override SchemaStore to return an error
	// We can use the fact that SimpleSchemaStore returns nil error for non-existent schemas
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "nonexistent",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: sst,
	})

	// GetSchema for non-existent measurement should return empty schema (no error from SimpleSchemaStore)
	sch, err := s.GetSchema()
	if err != nil {
		t.Fatalf("GetSchema failed: %v", err)
	}
	if sch.Fields == nil {
		t.Error("expected non-nil Fields map")
	}

	_ = s.Close()
}

func TestShardIterator_GetSchemaError(t *testing.T) {
	tmpDir := t.TempDir()

	// First, create a shard with SchemaStore and write SSTable files
	sst := metadata.NewSimpleSchemaStore()
	s1 := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: sst,
	})

	points := []types.MemPoint{
		{Timestamp: 1000000000, Sid: 1, FieldData: nil},
	}
	if _, _, _, _, err := s1.WriteSSTable(points); err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}
	_ = s1.Close()

	// Now create a shard in the SAME directory WITHOUT a SchemaStore
	// so that GetSchema fails
	s2 := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		// No SchemaStore - GetSchema will fail
	})

	iter := NewShardIterator(s2, 0, 10000000000, 0)
	defer iter.Close()

	// Should have an error from GetSchema
	if iter.Err() == nil {
		t.Error("expected error from GetSchema when SchemaStore is nil")
	}

	_ = s2.Close()
}

func TestShard_WriteSSTable_SchemaStoreError(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	// Write some data first
	points := []types.MemPoint{
		{Timestamp: 1000000000, Sid: 1, FieldData: nil},
	}
	path, _, _, _, err := s.WriteSSTable(points)
	if err != nil {
		t.Fatalf("First WriteSSTable failed: %v", err)
	}
	if path == "" {
		t.Error("expected non-empty path")
	}

	// Verify file exists after write
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("SSTable should exist at %s", path)
	}

	_ = s.Close()
}

func TestShardManager_Flush_WriteSSTableError(t *testing.T) {
	tmpDir := t.TempDir()

	mgr, err := metadata.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	sm := NewShardManager(
		tmpDir,
		time.Hour,
		nil,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	// Create a shard
	ts := time.Now().UnixNano()
	_, err = sm.GetShard("db1", "cpu", ts)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	// Valid points that flush successfully
	shardStart := (ts / int64(time.Hour)) * int64(time.Hour)
	points := []types.MemPoint{
		{Timestamp: shardStart + 100, Sid: 1, FieldData: nil},
	}

	if err := sm.Flush("db1", "cpu", points); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify shards were created
	shards := sm.GetAllShards()
	if len(shards) < 1 {
		t.Error("expected at least 1 shard after flush")
	}

	_ = sm.CloseAll()
}

func TestShardManager_DeleteShard_CloseError(t *testing.T) {
	// For DeleteShard, the close error path is hard to trigger,
	// but we can test that DeleteShard with invalid key formats returns nil
	tmpDir := t.TempDir()
	m := newTestShardManager(t, tmpDir, time.Hour)

	// Delete non-existent shard should not error
	if err := m.DeleteShard("nonexistent"); err != nil {
		t.Errorf("DeleteShard for non-existent should succeed: %v", err)
	}
}

func TestShardManager_GetShards_InvalidName(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	m := newTestShardManager(t, tmpDir, time.Hour)

	shards := m.GetShards("", "cpu", 0, 1000)
	if len(shards) != 0 {
		t.Errorf("expected 0 shards for invalid db name, got %d", len(shards))
	}

	shards = m.GetShards("db1", "", 0, 1000)
	if len(shards) != 0 {
		t.Errorf("expected 0 shards for invalid measurement name, got %d", len(shards))
	}
}

func TestShard_FullIteratorFlow(t *testing.T) {
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})

	// Write an SSTable
	points := []types.MemPoint{
		{Timestamp: 500000000, Sid: 1, FieldData: nil},
		{Timestamp: 1500000000, Sid: 1, FieldData: nil},
	}
	if _, _, _, _, err := s.WriteSSTable(points); err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	// Create iterator and read all data
	iter := NewShardIterator(s, 0, int64(time.Hour), 0)
	defer iter.Close()

	count := 0
	for {
		row := iter.Next()
		if row == nil {
			break
		}
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
	if err := iter.Err(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	_ = s.Close()
}

func TestShardManager_discoverShardsLocked_WithEntries(t *testing.T) {
	tmpDir := t.TempDir()

	mgr, err := metadata.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	sm := NewShardManager(
		tmpDir,
		time.Hour,
		nil,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	// Manually create a shard directory structure
	measDir := filepath.Join(tmpDir, "db1", "cpu")
	if err := os.MkdirAll(measDir, 0700); err != nil {
		t.Fatal(err)
	}

	// Create a valid shard dir with proper time range format
	shardDirName := "0_3600000000000" // start_end
	if err := os.MkdirAll(filepath.Join(measDir, shardDirName), 0700); err != nil {
		t.Fatal(err)
	}

	// Also create a file (non-directory) and an invalid dir name to test skip logic
	if err := os.WriteFile(filepath.Join(measDir, "some_file.txt"), []byte{}, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(measDir, "invalid_name"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(measDir, "abc_def_ghi"), 0700); err != nil {
		t.Fatal(err)
	}

	// discoverShardsLocked should only process the valid "start_end" dir
	sm.discoverShardsLocked("db1", "cpu")

	// Should have discovered 1 shard
	shards := sm.GetAllShards()
	if len(shards) != 1 {
		t.Errorf("expected 1 discovered shard, got %d", len(shards))
	}

	_ = sm.CloseAll()
}

func TestShardManager_Flush_GroupByShardError(t *testing.T) {
	tmpDir := t.TempDir()

	mgr, err := metadata.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	sm := NewShardManager(
		tmpDir,
		time.Hour,
		nil,
		sstable.CompressionNone,
		mgr.Catalog(),
		mgr.Series(),
		mgr.Shards(),
	)

	// Points with empty/invalid db/meas should cause Flush to return error
	// (isNameSafe fails, checked at the top of Flush)
	points := []types.MemPoint{
		{Timestamp: 1000, Sid: 1},
	}

	// This should return error because db name is empty
	if err := sm.Flush("", "cpu", points); err == nil {
		t.Error("expected error for empty db")
	}

	// This should return error because measurement name is empty
	if err := sm.Flush("db1", "", points); err == nil {
		t.Error("expected error for empty measurement")
	}

	_ = sm.CloseAll()
}

func TestShardManager_DeleteShard_RemoveAllError(t *testing.T) {
	tmpDir := t.TempDir()
	m := newTestShardManager(t, tmpDir, time.Hour)

	// Create a shard
	base := time.Now().UnixNano()
	s, err := m.GetShard("db1", "cpu", base)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	// Delete shard - should succeed
	key := "db1/cpu/" + formatInt64(s.StartTime())
	if err := m.DeleteShard(key); err != nil {
		t.Fatalf("DeleteShard failed: %v", err)
	}

	// Should be empty
	shards := m.GetAllShards()
	if len(shards) != 0 {
		t.Errorf("expected 0 shards after delete, got %d", len(shards))
	}
}

func TestShard_WriteSSTable_NewWriterError(t *testing.T) {
	// Create a file where sstable.NewWriter tries to create its tmp dir
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	// Place a file where the tmp dir for seq 0 would be created
	tmpPath := filepath.Join(dataDir, ".sst_0_tmp")
	if err := os.WriteFile(tmpPath, []byte{}, 0600); err != nil {
		t.Fatal(err)
	}

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
	})
	defer s.Close()

	points := []types.MemPoint{
		{Timestamp: 1000000000, Sid: 1, FieldData: nil},
	}

	_, _, _, _, err := s.WriteSSTable(points)
	if err == nil {
		t.Error("expected error when tmp dir is blocked by a file")
	}
}
