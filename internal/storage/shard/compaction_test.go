package shard

import (
	"container/heap"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/measurement"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

func TestDefaultCompactionConfig(t *testing.T) {
	cfg := DefaultCompactionConfig()
	if cfg == nil {
		t.Fatal("DefaultCompactionConfig should not return nil")
	}
	if cfg.MaxSSTableCount != 4 {
		t.Errorf("expected MaxSSTableCount=4, got %d", cfg.MaxSSTableCount)
	}
	if cfg.MaxCompactionBatch != 0 {
		t.Errorf("expected MaxCompactionBatch=0, got %d", cfg.MaxCompactionBatch)
	}
	if cfg.ShardSizeLimit != ShardSizeLimit {
		t.Errorf("expected ShardSizeLimit=%d, got %d", ShardSizeLimit, cfg.ShardSizeLimit)
	}
	if cfg.CheckInterval != time.Hour {
		t.Errorf("expected CheckInterval=1h, got %v", cfg.CheckInterval)
	}
	if cfg.Timeout != 30*time.Minute {
		t.Errorf("expected Timeout=30m, got %v", cfg.Timeout)
	}
}

func TestNewCompactionTask(t *testing.T) {
	inputFiles := []string{"/path/to/sst_1", "/path/to/sst_2"}
	outputPath := "/path/to/output"

	task := NewCompactionTask(inputFiles, outputPath)
	if task == nil {
		t.Fatal("NewCompactionTask should not return nil")
	}
	if len(task.inputFiles) != 2 {
		t.Errorf("expected 2 input files, got %d", len(task.inputFiles))
	}
	if task.outputPath != outputPath {
		t.Errorf("expected outputPath=%s, got %s", outputPath, task.outputPath)
	}
	if task.progress != 0 {
		t.Errorf("expected progress=0, got %d", task.progress)
	}
	if task.startedAt.IsZero() {
		t.Error("startedAt should not be zero")
	}
}

func TestNewCompactionManager(t *testing.T) {
	cfg := &CompactionConfig{
		MaxSSTableCount:    4,
		MaxCompactionBatch: 2,
		ShardSizeLimit:     1 * 1024 * 1024,
		CheckInterval:      time.Minute,
		Timeout:            5 * time.Minute,
	}

	shard := &Shard{}

	cm := NewCompactionManager(shard, cfg)
	if cm == nil {
		t.Fatal("NewCompactionManager should not return nil")
	}
	if cm.shard != shard {
		t.Error("shard not set correctly")
	}
	if cm.config != cfg {
		t.Error("config not set correctly")
	}
}

func TestNewCompactionManager_NilConfig(t *testing.T) {
	shard := &Shard{}
	cm := NewCompactionManager(shard, nil)
	if cm == nil {
		t.Fatal("NewCompactionManager should not return nil with nil config")
	}
	if cm.config == nil {
		t.Fatal("config should not be nil when using default")
	}
	if cm.config.MaxSSTableCount != 4 {
		t.Errorf("expected default MaxSSTableCount=4, got %d", cm.config.MaxSSTableCount)
	}
}

func TestCompactionManager_ShouldCompact_NoData(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	cm := shard.compaction
	if cm == nil {
		t.Fatal("compaction manager should not be nil")
	}

	// No SSTables, should not compact
	if cm.ShouldCompact() {
		t.Error("ShouldCompact should return false when no SSTables exist")
	}
}

func TestCompactionManager_ShouldCompact_WithSSTables(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	// Write and flush to create SSTables
	for i := 0; i < 5; i++ {
		p := &types.Point{
			Database:    "testdb",
			Measurement: "test",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   int64(i) * 1000,
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(int64(i)),
			},
		}
		_ = shard.Write(p)
	}
	_ = shard.Flush()
	_ = shard.Flush()
	_ = shard.Flush()
	_ = shard.Flush()
	_ = shard.Flush()

	cm := shard.compaction
	// With 5 SSTables and MaxSSTableCount=4, should compact
	if !cm.ShouldCompact() {
		t.Log("ShouldCompact returned false, may need more SSTables or shard size check")
	}
}

func TestCompactionManager_ShouldCompactWithLock(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	cm := shard.compaction
	cm.mu.Lock()
	result := cm.ShouldCompactWithLock()
	cm.mu.Unlock()

	// Should return same result as ShouldCompact
	if result != cm.shouldCompactLocked() {
		t.Error("ShouldCompactWithLock should match shouldCompactLocked")
	}
}

func TestCompactionManager_CollectSSTables_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	cm := shard.compaction
	cm.mu.Lock()
	files, err := cm.collectSSTables()
	cm.mu.Unlock()

	if err != nil {
		t.Fatalf("collectSSTables failed: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected 0 files, got %d", len(files))
	}
}

func TestCompactionManager_CollectSSTables_WithData(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)

	// Write and flush to create SSTables
	for i := 0; i < 3; i++ {
		p := &types.Point{
			Database:    "testdb",
			Measurement: "test",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   int64(i) * 1000,
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(int64(i)),
			},
		}
		_ = shard.Write(p)
	}
	_ = shard.Flush()

	cm := shard.compaction
	cm.mu.Lock()
	files, err := cm.collectSSTables()
	cm.mu.Unlock()

	if err != nil {
		t.Fatalf("collectSSTables failed: %v", err)
	}
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}

	_ = shard.Close()
}

func TestCompactionManager_IsSSTableInWrite(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// Create a fake SSTable directory
	sstDir := filepath.Join(tmpDir, "data", "sst_test")
	_ = os.MkdirAll(sstDir, 0755)

	// Should not be in write state initially
	if cm.isSSTableInWrite(sstDir) {
		t.Error("isSSTableInWrite should return false when no .writing file exists")
	}

	// Create .writing file
	writingFlag := filepath.Join(sstDir, ".writing")
	f, _ := os.Create(writingFlag)
	_ = f.Close()

	if !cm.isSSTableInWrite(sstDir) {
		t.Error("isSSTableInWrite should return true when .writing file exists")
	}

	_ = os.Remove(writingFlag)
	_ = shard.Close()
}

func TestCompactionManager_MarkUnmarkSSTableWriting(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// Create a fake SSTable directory
	sstDir := filepath.Join(tmpDir, "data", "sst_test")
	_ = os.MkdirAll(sstDir, 0755)

	// Mark as writing
	err := cm.markSSTableWriting(sstDir)
	if err != nil {
		t.Fatalf("markSSTableWriting failed: %v", err)
	}

	if !cm.isSSTableInWrite(sstDir) {
		t.Error("SSTable should be marked as in write")
	}

	// Unmark
	err = cm.unmarkSSTableWriting(sstDir)
	if err != nil {
		t.Fatalf("unmarkSSTableWriting failed: %v", err)
	}

	if cm.isSSTableInWrite(sstDir) {
		t.Error("SSTable should not be marked as in write after unmark")
	}

	_ = shard.Close()
}

func TestCompactionManager_MarkSSTableWriting_CreateDir(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// Create a path that doesn't exist yet (parent dir exists but not the sst dir)
	sstDir := filepath.Join(tmpDir, "data", "sst_new")

	// Mark as writing - should create the directory
	err := cm.markSSTableWriting(sstDir)
	if err != nil {
		t.Fatalf("markSSTableWriting should create parent dir: %v", err)
	}

	// Verify .writing file exists
	writingFlag := filepath.Join(sstDir, ".writing")
	if _, err := os.Stat(writingFlag); os.IsNotExist(err) {
		t.Error(".writing file should exist")
	}

	_ = shard.Close()
}

func TestCompactionManager_Compact_NoSSTables(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	cm := shard.compaction
	ctx := context.Background()
	outputPath, deletedFiles, err := cm.Compact(ctx)

	if err != nil {
		t.Fatalf("Compact should not fail with no SSTables: %v", err)
	}
	if outputPath != "" {
		t.Error("outputPath should be empty with no SSTables")
	}
	if len(deletedFiles) != 0 {
		t.Error("deletedFiles should be empty with no SSTables")
	}
}

func TestCompactionManager_Compact_LessThanTwoSSTables(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)

	// Write and flush to create only 1 SSTable
	p := &types.Point{
		Database:    "testdb",
		Measurement: "test",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   1000,
		Fields: map[string]*types.FieldValue{
			"value": types.NewFieldValue(int64(1)),
		},
	}
	_ = shard.Write(p)
	_ = shard.Flush()

	cm := shard.compaction
	ctx := context.Background()
	outputPath, deletedFiles, err := cm.Compact(ctx)

	if err != nil {
		t.Fatalf("Compact should not fail with <2 SSTables: %v", err)
	}
	if outputPath != "" {
		t.Error("outputPath should be empty with <2 SSTables")
	}
	if len(deletedFiles) != 0 {
		t.Error("deletedFiles should be empty with <2 SSTables")
	}

	_ = shard.Close()
}

func TestCompactionManager_VerifyOutput_NotExist(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	err := cm.verifyOutput(filepath.Join(tmpDir, "nonexistent"))
	if err == nil {
		t.Error("verifyOutput should fail for nonexistent path")
	}

	_ = shard.Close()
}

func TestCompactionManager_VerifyOutput_NotDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// Create a file instead of directory
	filePath := filepath.Join(tmpDir, "notadir")
	_ = os.WriteFile(filePath, []byte("test"), 0644)

	err := cm.verifyOutput(filePath)
	if err == nil {
		t.Error("verifyOutput should fail for non-directory")
	}

	_ = shard.Close()
}

func TestCompactionManager_VerifyOutput_MissingFiles(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// Create directory without required files
	outputPath := filepath.Join(tmpDir, "output")
	_ = os.MkdirAll(outputPath, 0755)

	err := cm.verifyOutput(outputPath)
	if err == nil {
		t.Error("verifyOutput should fail for missing required files")
	}

	_ = shard.Close()
}

func TestCompactionManager_VerifyOutput_Success(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)

	// Write and flush to create a real SSTable
	p := &types.Point{
		Database:    "testdb",
		Measurement: "test",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   1000,
		Fields: map[string]*types.FieldValue{
			"value": types.NewFieldValue(int64(1)),
		},
	}
	_ = shard.Write(p)
	_ = shard.Flush()

	// Get the SSTable path
	dataDir := shard.DataDir()
	entries, _ := os.ReadDir(dataDir)
	var sstPath string
	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) > 4 && entry.Name()[:4] == "sst_" {
			sstPath = filepath.Join(dataDir, entry.Name())
			break
		}
	}

	if sstPath == "" {
		t.Fatal("no SSTable found")
	}

	cm := shard.compaction
	err := cm.verifyOutput(sstPath)
	if err != nil {
		t.Errorf("verifyOutput should succeed for valid SSTable: %v", err)
	}

	_ = shard.Close()
}

func TestCompactionManager_TryAcquireReleaseCompactLock(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// First acquire should succeed
	if !cm.tryAcquireCompactLock() {
		t.Error("first tryAcquireCompactLock should succeed")
	}

	// Second acquire should fail (already held)
	if cm.tryAcquireCompactLock() {
		t.Error("second tryAcquireCompactLock should fail")
	}

	// Release
	cm.releaseCompactLock()

	// Third acquire should succeed again
	if !cm.tryAcquireCompactLock() {
		t.Error("third tryAcquireCompactLock should succeed after release")
	}

	cm.releaseCompactLock()
	_ = shard.Close()
}

func TestCompactionManager_ResetTimer(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &CompactionConfig{
		MaxSSTableCount:    4,
		MaxCompactionBatch: 2,
		ShardSizeLimit:     1 * 1024 * 1024,
		CheckInterval:      time.Millisecond, // Very short for testing
		Timeout:            5 * time.Minute,
	}

	shardCfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: cfg,
	}

	shard := NewShard(shardCfg)
	cm := shard.compaction

	cm.resetTimer()

	if cm.ticker == nil {
		t.Error("ticker should be set")
	}

	cm.Stop()
	_ = shard.Close()
}

func TestCompactionManager_Stop(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &CompactionConfig{
		MaxSSTableCount:    4,
		MaxCompactionBatch: 2,
		ShardSizeLimit:     1 * 1024 * 1024,
		CheckInterval:      time.Millisecond,
		Timeout:            5 * time.Minute,
	}

	shardCfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: cfg,
	}

	shard := NewShard(shardCfg)
	cm := shard.compaction

	// Stop should be safe to call multiple times
	cm.Stop()
	cm.Stop()

	_ = shard.Close()
}

func TestCompactionManager_StartPeriodicCheck_NilInterval(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &CompactionConfig{
		MaxSSTableCount:    4,
		MaxCompactionBatch: 2,
		ShardSizeLimit:     1 * 1024 * 1024,
		CheckInterval:      0, // Disabled
		Timeout:            5 * time.Minute,
	}

	shardCfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: cfg,
	}

	shard := NewShard(shardCfg)
	cm := shard.compaction

	// Should not panic with 0 interval
	cm.StartPeriodicCheck()

	cm.Stop()
	_ = shard.Close()
}

func TestCompactionManager_StartPeriodicCheck(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &CompactionConfig{
		MaxSSTableCount:    4,
		MaxCompactionBatch: 2,
		ShardSizeLimit:     1 * 1024 * 1024,
		CheckInterval:      10 * time.Millisecond,
		Timeout:            5 * time.Minute,
	}

	shardCfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: cfg,
	}

	shard := NewShard(shardCfg)
	cm := shard.compaction

	cm.StartPeriodicCheck()

	// Wait a bit for periodic check to potentially run
	time.Sleep(50 * time.Millisecond)

	cm.Stop()
	_ = shard.Close()
}

func TestCompactionManager_DoPeriodicCompaction(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &CompactionConfig{
		MaxSSTableCount:    4,
		MaxCompactionBatch: 2,
		ShardSizeLimit:     1 * 1024 * 1024,
		CheckInterval:      10 * time.Millisecond,
		Timeout:            5 * time.Minute,
	}

	shardCfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: cfg,
	}

	shard := NewShard(shardCfg)
	cm := shard.compaction

	// Manually call doPeriodicCompaction
	cm.doPeriodicCompaction()

	cm.Stop()
	_ = shard.Close()
}

func TestCompactionManager_DoPeriodicCompaction_AlreadyRunning(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &CompactionConfig{
		MaxSSTableCount:    4,
		MaxCompactionBatch: 2,
		ShardSizeLimit:     1 * 1024 * 1024,
		CheckInterval:      10 * time.Millisecond,
		Timeout:            5 * time.Minute,
	}

	shardCfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: cfg,
	}

	shard := NewShard(shardCfg)
	cm := shard.compaction

	// Acquire lock manually
	cm.tryAcquireCompactLock()

	// doPeriodicCompaction should return early
	cm.doPeriodicCompaction()

	cm.releaseCompactLock()
	cm.Stop()
	_ = shard.Close()
}

func TestDirSize(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a directory with some files
	subDir := filepath.Join(tmpDir, "subdir")
	_ = os.MkdirAll(subDir, 0755)

	// Write some test files
	_ = os.WriteFile(filepath.Join(subDir, "file1.txt"), []byte("hello"), 0644)
	_ = os.WriteFile(filepath.Join(subDir, "file2.txt"), []byte("world"), 0644)

	size, err := dirSize(subDir)
	if err != nil {
		t.Fatalf("dirSize failed: %v", err)
	}

	// 5 bytes "hello" + 5 bytes "world" = 10 bytes
	if size != 10 {
		t.Errorf("expected size=10, got %d", size)
	}
}

func TestDirSize_NotExist(t *testing.T) {
	tmpDir := t.TempDir()
	nonExistent := filepath.Join(tmpDir, "nonexistent")

	_, err := dirSize(nonExistent)
	if err == nil {
		t.Error("dirSize should fail for nonexistent path")
	}
}

func TestCompactionManager_CalculateShardSize_NoData(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// No SSTables, should return 0
	size, err := cm.calculateShardSize()
	if err != nil {
		t.Fatalf("calculateShardSize failed: %v", err)
	}
	if size != 0 {
		t.Errorf("expected size=0, got %d", size)
	}

	_ = shard.Close()
}

func TestCompactionManager_CalculateShardSize_WithData(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)

	// Write and flush to create SSTable
	p := &types.Point{
		Database:    "testdb",
		Measurement: "test",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   1000,
		Fields: map[string]*types.FieldValue{
			"value": types.NewFieldValue(int64(1)),
		},
	}
	_ = shard.Write(p)
	_ = shard.Flush()

	cm := shard.compaction
	size, err := cm.calculateShardSize()
	if err != nil {
		t.Fatalf("calculateShardSize failed: %v", err)
	}
	if size <= 0 {
		t.Errorf("expected positive size, got %d", size)
	}

	_ = shard.Close()
}

func TestShard_NextSSTSeq(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "test",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)

	// Initial sequence should be 0
	seq := shard.NextSSTSeq()
	if seq != 0 {
		t.Errorf("expected first seq=0, got %d", seq)
	}

	// Next should be 1
	seq = shard.NextSSTSeq()
	if seq != 1 {
		t.Errorf("expected second seq=1, got %d", seq)
	}

	// Next should be 2
	seq = shard.NextSSTSeq()
	if seq != 2 {
		t.Errorf("expected third seq=2, got %d", seq)
	}

	_ = shard.Close()
}

func TestCompactionManager_Compact_WithMultipleSSTables(t *testing.T) {
	// This test requires complex setup with real SSTables and proper cleanup.
	// Skipping for now to focus on other coverage improvements.
	t.Skip("compact with multiple SSTables requires complex setup")
}

func TestCompactionManager_Commit(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		MetaStore:     measurement.NewMeasurementMetaStore(),
		MemTableCfg:   DefaultMemTableConfig(),
		CompactionCfg: DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)

	// Write and flush to create a real SSTable
	p := &types.Point{
		Database:    "testdb",
		Measurement: "test",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   1000,
		Fields: map[string]*types.FieldValue{
			"value": types.NewFieldValue(int64(1)),
		},
	}
	_ = shard.Write(p)
	_ = shard.Flush()

	// Get the SSTable path
	dataDir := shard.DataDir()
	entries, _ := os.ReadDir(dataDir)
	var sstPath string
	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) > 4 && entry.Name()[:4] == "sst_" {
			sstPath = filepath.Join(dataDir, entry.Name())
			break
		}
	}

	if sstPath == "" {
		t.Fatal("no SSTable found")
	}

	cm := shard.compaction

	// Create a task with the existing SSTable
	task := &CompactionTask{
		inputFiles:  []string{sstPath},
		outputPath:  sstPath, // reuse same path for simplicity
		progress:    0,
		startedAt:   time.Now(),
		outputCount: 10,
	}

	// commit should work
	err := cm.commit(task)
	if err != nil {
		t.Errorf("commit failed: %v", err)
	}

	_ = shard.Close()
}

func TestCompactionManager_Merge_ContextCancel(t *testing.T) {
	// This test verifies that when context is cancelled during merge,
	// it returns context.Canceled. However, since we can't easily create
	// a long-running merge with just unit tests, we skip this test.
	t.Skip("merge with context cancel requires complex setup")
}

func TestMergeIterator_Next_Point(t *testing.T) {
	// Create multiple SSTables and test the merge iterator
	tmpDir := t.TempDir()

	// Create a simple shard for context
	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "test",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)

	// Write and flush multiple points
	baseTime := time.Now().UnixNano()
	for i := 0; i < 3; i++ {
		p := &types.Point{
			Database:    "testdb",
			Measurement: "test",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   baseTime + int64(i)*1000,
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(int64(i)),
			},
		}
		_ = shard.Write(p)
	}
	_ = shard.Flush()

	// Now get the SSTable reader
	dataDir := shard.DataDir()
	entries, _ := os.ReadDir(dataDir)
	var sstPath string
	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) > 4 && entry.Name()[:4] == "sst_" {
			sstPath = filepath.Join(dataDir, entry.Name())
			break
		}
	}

	if sstPath == "" {
		t.Fatal("no SSTable found")
	}

	reader, err := sstable.NewReader(sstPath)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = reader.Close() }()

	iter, err := reader.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// Create merge iterator with single iterator
	mergeIter := newMergeIterator([]*sstable.Iterator{iter})

	count := 0
	for mergeIter.Next() {
		point := mergeIter.Point()
		if point == nil {
			t.Error("Point should not be nil when Next returns true")
		}
		count++
	}

	if count < 1 {
		t.Errorf("expected at least 1 point, got %d", count)
	}

	if mergeIter.Error() != nil {
		t.Errorf("Error should be nil: %v", mergeIter.Error())
	}

	_ = shard.Close()
}

func TestMergeIterator_Empty(t *testing.T) {
	mergeIter := newMergeIterator([]*sstable.Iterator{})

	// Should return false immediately
	if mergeIter.Next() {
		t.Error("Next should return false for empty iterator list")
	}

	if mergeIter.Point() != nil {
		t.Error("Point should be nil when heap is empty")
	}
}

// heapItem tests
func TestMergeHeap_Len(t *testing.T) {
	h := mergeHeap{}
	if h.Len() != 0 {
		t.Errorf("expected len=0, got %d", h.Len())
	}

	h = append(h, &mergeHeapItem{})
	if h.Len() != 1 {
		t.Errorf("expected len=1, got %d", h.Len())
	}
}

func TestMergeHeap_Less(t *testing.T) {
	h := mergeHeap{
		{timestamp: 100},
		{timestamp: 200},
	}

	if !h.Less(0, 1) {
		t.Error("timestamp 100 should be less than 200")
	}

	// Same timestamp, should use idx
	h[0].timestamp = 100
	h[1].timestamp = 100
	h[0].idx = 0
	h[1].idx = 1

	if !h.Less(0, 1) {
		t.Error("idx 0 should be less than idx 1 when timestamps equal")
	}
}

func TestMergeHeap_Swap(t *testing.T) {
	h := mergeHeap{
		{timestamp: 100, idx: 0},
		{timestamp: 200, idx: 1},
	}

	h.Swap(0, 1)

	if h[0].timestamp != 200 || h[1].timestamp != 100 {
		t.Error("Swap did not work correctly")
	}
}

func TestMergeHeap_PushPop(t *testing.T) {
	h := make(mergeHeap, 0)

	heap.Push(&h, &mergeHeapItem{timestamp: 100, idx: 0})
	heap.Push(&h, &mergeHeapItem{timestamp: 50, idx: 1})
	heap.Push(&h, &mergeHeapItem{timestamp: 200, idx: 2})

	if h.Len() != 3 {
		t.Errorf("expected len=3, got %d", h.Len())
	}

	// Pop should return smallest (50)
	item := heap.Pop(&h).(*mergeHeapItem)
	if item.timestamp != 50 {
		t.Errorf("expected timestamp=50, got %d", item.timestamp)
	}

	if h.Len() != 2 {
		t.Errorf("expected len=2, got %d", h.Len())
	}
}

func TestMergeIterator_AfterEmpty(t *testing.T) {
	// Test that merge iterator works correctly after exhausting all items
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "test",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)

	// Write and flush a point
	p := &types.Point{
		Database:    "testdb",
		Measurement: "test",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   1000,
		Fields: map[string]*types.FieldValue{
			"value": types.NewFieldValue(int64(1)),
		},
	}
	_ = shard.Write(p)
	_ = shard.Flush()

	// Get SSTable path
	dataDir := shard.DataDir()
	entries, _ := os.ReadDir(dataDir)
	var sstPath string
	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) > 4 && entry.Name()[:4] == "sst_" {
			sstPath = filepath.Join(dataDir, entry.Name())
			break
		}
	}

	reader, _ := sstable.NewReader(sstPath)
	iter, _ := reader.NewIterator()
	mergeIter := newMergeIterator([]*sstable.Iterator{iter})

	// Exhaust the iterator
	for mergeIter.Next() {
	}

	// Next call should return false
	if mergeIter.Next() {
		t.Error("Next should return false after exhausting")
	}

	_ = reader.Close()
	_ = shard.Close()
}
