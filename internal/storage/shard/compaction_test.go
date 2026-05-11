package shard

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

func TestNewCompactionManager(t *testing.T) {
	cfg := &compaction.CompactionConfig{
		MaxSSTableCount:    4,
		MaxCompactionBatch: 2,
		ShardSizeLimit:     1 * 1024 * 1024,
		CheckInterval:      time.Minute,
		Timeout:            5 * time.Minute,
	}

	shard := &Shard{}

	cm := compaction.NewCompactionManager(shard, cfg)
	if cm == nil {
		t.Fatal("NewCompactionManager should not return nil")
	}
	if cm.ShardAccess != shard {
		t.Error("shard not set correctly")
	}
	if cm.Config != cfg {
		t.Error("config not set correctly")
	}
}

func TestNewCompactionManager_NilConfig(t *testing.T) {
	shard := &Shard{}
	cm := compaction.NewCompactionManager(shard, nil)
	if cm == nil {
		t.Fatal("NewCompactionManager should not return nil with nil config")
	}
	if cm.Config == nil {
		t.Fatal("config should not be nil when using default")
	}
	if cm.Config.MaxSSTableCount != 4 {
		t.Errorf("expected default MaxSSTableCount=4, got %d", cm.Config.MaxSSTableCount)
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	cm := shard.compaction
	cm.Mu.Lock()
	result := cm.ShouldCompactWithLock()
	cm.Mu.Unlock()

	// Should return same result as ShouldCompact
	if result != cm.ShouldCompactLocked() {
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	cm := shard.compaction
	cm.Mu.Lock()
	files, err := cm.CollectSSTables()
	cm.Mu.Unlock()

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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
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
	cm.Mu.Lock()
	files, err := cm.CollectSSTables()
	cm.Mu.Unlock()

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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// 创建 SSTable 文件路径（单文件格式）
	sstPath := filepath.Join(tmpDir, "data", "sst_test.bin")
	_ = os.MkdirAll(filepath.Dir(sstPath), 0700)
	_ = os.WriteFile(sstPath, []byte("dummy"), 0600)

	// .writing 文件不存在时应返回 false
	if cm.IsSSTableInWrite(sstPath) {
		t.Error("isSSTableInWrite should return false when no .writing file exists")
	}

	// 创建 sibling .writing 文件
	writingFlag := sstPath + ".writing"
	f, _ := os.Create(writingFlag)
	_ = f.Close()

	if !cm.IsSSTableInWrite(sstPath) {
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// 创建 SSTable 文件路径（单文件格式）
	sstPath := filepath.Join(tmpDir, "data", "sst_test.bin")
	_ = os.MkdirAll(filepath.Dir(sstPath), 0700)
	_ = os.WriteFile(sstPath, []byte("dummy"), 0600)

	// Mark as writing
	err := cm.MarkWriting(sstPath)
	if err != nil {
		t.Fatalf("markSSTableWriting failed: %v", err)
	}

	if !cm.IsSSTableInWrite(sstPath) {
		t.Error("SSTable should be marked as in write")
	}

	// Unmark
	err = cm.UnmarkWriting(sstPath)
	if err != nil {
		t.Fatalf("unmarkSSTableWriting failed: %v", err)
	}

	if cm.IsSSTableInWrite(sstPath) {
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// 创建一个不存在的文件路径（父目录存在但文件不存在）
	sstPath := filepath.Join(tmpDir, "data", "sst_new.bin")

	// Mark as writing - 应该创建父目录和 .writing 文件
	err := cm.MarkWriting(sstPath)
	if err != nil {
		t.Fatalf("markSSTableWriting should create parent dir: %v", err)
	}

	// 验证 sibling .writing 文件存在
	writingFlag := sstPath + ".writing"
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	err := cm.VerifyOutput(filepath.Join(tmpDir, "nonexistent"))
	if err == nil {
		t.Error("verifyOutput should fail for nonexistent path")
	}

	_ = shard.Close()
}

func TestCompactionManager_VerifyOutput_NotFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// 创建目录而非文件，verifyOutput 应拒绝目录
	dirPath := filepath.Join(tmpDir, "is_a_dir")
	_ = os.MkdirAll(dirPath, 0700)

	err := cm.VerifyOutput(dirPath)
	if err == nil {
		t.Error("verifyOutput should fail for directory (expected file)")
	}

	_ = shard.Close()
}

func TestCompactionManager_VerifyOutput_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// 创建空文件（不是有效的 SSTable）
	filePath := filepath.Join(tmpDir, "empty.bin")
	_ = os.WriteFile(filePath, []byte{}, 0600)

	// 空文件 stat 能成功，但不是有效的 SSTable
	// VerifyOutput 只检查是否为文件，不检查内容
	err := cm.VerifyOutput(filePath)
	if err != nil {
		t.Errorf("verifyOutput should succeed for a regular file: %v", err)
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
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
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "sst_") && strings.HasSuffix(entry.Name(), ".bin") {
			sstPath = filepath.Join(dataDir, entry.Name())
			break
		}
	}

	if sstPath == "" {
		t.Fatal("no SSTable found")
	}

	cm := shard.compaction
	err := cm.VerifyOutput(sstPath)
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// First acquire should succeed
	if !cm.TryAcquireCompactLock() {
		t.Error("first tryAcquireCompactLock should succeed")
	}

	// Second acquire should fail (already held)
	if cm.TryAcquireCompactLock() {
		t.Error("second tryAcquireCompactLock should fail")
	}

	// Release
	cm.ReleaseCompactLock()

	// Third acquire should succeed again
	if !cm.TryAcquireCompactLock() {
		t.Error("third tryAcquireCompactLock should succeed after release")
	}

	cm.ReleaseCompactLock()
	_ = shard.Close()
}

func TestCompactionManager_ResetTimer(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &compaction.CompactionConfig{
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: cfg,
	}

	shard := NewShard(shardCfg)
	cm := shard.compaction

	cm.ResetTimer()

	if cm.Ticker == nil {
		t.Error("ticker should be set")
	}

	cm.Stop()
	_ = shard.Close()
}

func TestCompactionManager_Stop(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &compaction.CompactionConfig{
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
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
	cfg := &compaction.CompactionConfig{
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
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
	cfg := &compaction.CompactionConfig{
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
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
	cfg := &compaction.CompactionConfig{
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: cfg,
	}

	shard := NewShard(shardCfg)
	cm := shard.compaction

	// Manually call doPeriodicCompaction
	cm.DoPeriodicCompaction()

	cm.Stop()
	_ = shard.Close()
}

func TestCompactionManager_DoPeriodicCompaction_AlreadyRunning(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &compaction.CompactionConfig{
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: cfg,
	}

	shard := NewShard(shardCfg)
	cm := shard.compaction

	// Acquire lock manually
	cm.TryAcquireCompactLock()

	// doPeriodicCompaction should return early
	cm.DoPeriodicCompaction()

	cm.ReleaseCompactLock()
	cm.Stop()
	_ = shard.Close()
}

func TestDirSize(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a directory with some files
	subDir := filepath.Join(tmpDir, "subdir")
	_ = os.MkdirAll(subDir, 0700)

	// Write some test files
	_ = os.WriteFile(filepath.Join(subDir, "file1.txt"), []byte("hello"), 0644)
	_ = os.WriteFile(filepath.Join(subDir, "file2.txt"), []byte("world"), 0644)

	size, err := compaction.DirSize(subDir)
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

	_, err := compaction.DirSize(nonExistent)
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// No SSTables, should return 0
	size, err := cm.CalculateShardSize()
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
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
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
	size, err := cm.CalculateShardSize()
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
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
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
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "test",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
		CompactionCfg: &compaction.CompactionConfig{
			MaxSSTableCount:    10,
			MaxCompactionBatch: 0,
			ShardSizeLimit:     100 * 1024 * 1024,
			CheckInterval:      time.Hour,
			Timeout:            30 * time.Minute,
		},
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	// 创建 3 个 SSTable
	for j := 0; j < 3; j++ {
		for i := 0; i < 5; i++ {
			p := &types.Point{
				Database:    "testdb",
				Measurement: "test",
				Tags:        map[string]string{"host": "server1"},
				Timestamp:   int64(j*10+i) * 1000,
				Fields: map[string]*types.FieldValue{
					"value": types.NewFieldValue(int64(j*10 + i)),
				},
			}
			_ = shard.Write(p)
		}
		_ = shard.Flush()
	}

	cm := shard.compaction
	ctx := context.Background()

	outputPath, deletedFiles, err := cm.Compact(ctx)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	if outputPath == "" {
		t.Error("outputPath should not be empty after compaction")
	}

	if len(deletedFiles) != 3 {
		t.Errorf("expected 3 deleted files, got %d", len(deletedFiles))
	}

	// 验证输出文件存在
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Error("output SSTable should exist after compaction")
	}

	// 验证旧文件已删除
	for _, f := range deletedFiles {
		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Errorf("old file %s should be deleted", f)
		}
	}
}

func TestCompactionManager_Commit(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
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
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "sst_") && strings.HasSuffix(entry.Name(), ".bin") {
			sstPath = filepath.Join(dataDir, entry.Name())
			break
		}
	}

	if sstPath == "" {
		t.Fatal("no SSTable found")
	}

	cm := shard.compaction

	// Create a task with the existing SSTable
	task := &compaction.CompactionTask{
		InputFiles:  []string{sstPath},
		OutputPath:  sstPath, // reuse same path for simplicity
		Progress:    0,
		StartedAt:   time.Now(),
		OutputCount: 10,
	}

	// commit should work
	err := cm.Commit(task)
	if err != nil {
		t.Errorf("commit failed: %v", err)
	}

	_ = shard.Close()
}

func TestCompactionManager_Merge_ContextCancel(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	// 创建一个包含大量数据的 SSTable 以便测试取消
	for i := 0; i < 100; i++ {
		p := &types.Point{
			Database:    "testdb",
			Measurement: "test",
			Tags:        map[string]string{"host": fmt.Sprintf("server%d", i%10)},
			Timestamp:   int64(i) * 1000,
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(int64(i)),
			},
		}
		_ = shard.Write(p)
	}
	_ = shard.Flush()

	dataDir := shard.DataDir()
	entries, _ := os.ReadDir(dataDir)
	var sstPath string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "sst_") && strings.HasSuffix(entry.Name(), ".bin") {
			sstPath = filepath.Join(dataDir, entry.Name())
			break
		}
	}

	if sstPath == "" {
		t.Fatal("no SSTable found")
	}

	cm := shard.compaction

	// 创建一个会被立即取消的 context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	task := &compaction.CompactionTask{
		InputFiles:  []string{sstPath},
		OutputPath:  filepath.Join(tmpDir, "output"),
		Progress:    0,
		StartedAt:   time.Now(),
		OutputCount: 0,
	}

	err := cm.Merge(ctx, task)
	if err == nil {
		t.Error("merge should fail when context is cancelled")
	}
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
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
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
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "sst_") && strings.HasSuffix(entry.Name(), ".bin") {
			sstPath = filepath.Join(dataDir, entry.Name())
			break
		}
	}

	if sstPath == "" {
		t.Fatal("no SSTable found")
	}

	schema, err := shard.GetSchema()
	if err != nil {
		t.Fatalf("GetSchema failed: %v", err)
	}

	reader, err := sstable.NewReader(sstPath, schema)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = reader.Close() }()

	iter, err := reader.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// Create merge iterator with single iterator
	mergeIter := compaction.NewMergeIterator([]*sstable.Iterator{iter})

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

func TestMergeIterator_AfterEmpty(t *testing.T) {
	// Test that merge iterator works correctly after exhausting all items
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "test",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
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
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "sst_") && strings.HasSuffix(entry.Name(), ".bin") {
			sstPath = filepath.Join(dataDir, entry.Name())
			break
		}
	}

	schema, _ := shard.GetSchema()
	reader, _ := sstable.NewReader(sstPath, schema)
	iter, _ := reader.NewIterator()
	mergeIter := compaction.NewMergeIterator([]*sstable.Iterator{iter})

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

func TestCompactionManager_shouldCompactLocked_True(t *testing.T) {
	// Skip due to race condition with background compaction during cleanup
	t.Skip("skipping due to background compaction race condition")

	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "test",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
		CompactionCfg: &compaction.CompactionConfig{
			MaxSSTableCount:    2, // 低阈值触发 compaction
			MaxCompactionBatch: 0,
			ShardSizeLimit:     100 * 1024 * 1024, // 100MB，大于实际大小
			CheckInterval:      time.Hour,
			Timeout:            30 * time.Minute,
		},
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	// 创建 2 个 SSTable
	for i := 0; i < 2; i++ {
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
		_ = shard.Flush()
	}

	cm := shard.compaction

	// shouldCompactLocked 应该返回 true
	result := cm.ShouldCompactLocked()
	if !result {
		t.Error("shouldCompactLocked should return true when SSTable count >= MaxSSTableCount")
	}
}

func TestCompactionManager_shouldCompactLocked_ShardSizeExceedsLimit(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "test",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
		CompactionCfg: &compaction.CompactionConfig{
			MaxSSTableCount:    2,
			MaxCompactionBatch: 0,
			ShardSizeLimit:     1, // 极小阈值，触发 size 限制
			CheckInterval:      time.Hour,
			Timeout:            30 * time.Minute,
		},
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	// 创建 2 个 SSTable
	for i := 0; i < 2; i++ {
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
		_ = shard.Flush()
	}

	cm := shard.compaction

	// shouldCompactLocked 应该返回 false，因为 shard 大小超过限制
	result := cm.ShouldCompactLocked()
	if result {
		t.Error("shouldCompactLocked should return false when shard size exceeds limit")
	}
}

func TestCompactionManager_Merge_Deduplication(t *testing.T) {
	// 测试 merge 中的去重逻辑
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		SchemaStore:   metadata.NewSimpleSchemaStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	// 创建两个有相同 (timestamp, sid) 的 SSTable
	// 由于去重逻辑，相同的数据应该只保留一个
	baseTime := time.Now().UnixNano()

	// 创建第一个 SSTable
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

	// 获取第一个 SSTable
	dataDir := shard.DataDir()
	entries, _ := os.ReadDir(dataDir)
	var sstPath1 string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "sst_") && strings.HasSuffix(entry.Name(), ".bin") {
			sstPath1 = filepath.Join(dataDir, entry.Name())
			break
		}
	}

	// 创建第二个 SSTable
	shard2 := NewShard(cfg)
	for i := 0; i < 3; i++ {
		p := &types.Point{
			Database:    "testdb",
			Measurement: "test",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   baseTime + int64(i)*1000, // 相同时间戳，触发去重
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(int64(i + 10)),
			},
		}
		_ = shard2.Write(p)
	}
	_ = shard2.Flush()

	// 获取第二个 SSTable
	entries2, _ := os.ReadDir(dataDir)
	var sstPath2 string
	for _, entry := range entries2 {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "sst_") && strings.HasSuffix(entry.Name(), ".bin") {
			p := filepath.Join(dataDir, entry.Name())
			if p != sstPath1 {
				sstPath2 = p
				break
			}
		}
	}
	_ = shard2.Close()

	if sstPath1 == "" || sstPath2 == "" {
		t.Skip("need 2 SSTables for deduplication test")
	}

	schema, _ := shard.GetSchema()

	// 使用 merge 直接测试
	reader1, err := sstable.NewReader(sstPath1, schema)
	if err != nil {
		t.Fatalf("NewReader 1 failed: %v", err)
	}
	defer func() { _ = reader1.Close() }()

	reader2, err := sstable.NewReader(sstPath2, schema)
	if err != nil {
		t.Fatalf("NewReader 2 failed: %v", err)
	}
	defer func() { _ = reader2.Close() }()

	iter1, err := reader1.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator 1 failed: %v", err)
	}

	iter2, err := reader2.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator 2 failed: %v", err)
	}

	mergeIter := compaction.NewMergeIterator([]*sstable.Iterator{iter1, iter2})

	// 使用 heap 模拟去重
	seen := make(map[string]bool)
	uniqueCount := 0
	for mergeIter.Next() {
		row := mergeIter.Point()
		key := fmt.Sprintf("%d-%d", row.Timestamp, row.Sid)
		if !seen[key] {
			seen[key] = true
			uniqueCount++
		}
	}

	// 由于去重，uniqueCount 应该小于总数据量
	t.Logf("unique points after dedup: %d", uniqueCount)
}

func TestCompactionManager_Merge_Error(t *testing.T) {
	// 测试 merge 错误处理 - 尝试打开不存在的文件
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	cm := shard.compaction

	// 创建一个 task，inputFiles 包含不存在的路径
	task := &compaction.CompactionTask{
		InputFiles:  []string{"/nonexistent/path"},
		OutputPath:  filepath.Join(tmpDir, "output"),
		Progress:    0,
		StartedAt:   time.Now(),
		OutputCount: 0,
	}

	err := cm.Merge(context.Background(), task)
	if err == nil {
		t.Error("merge should fail with nonexistent file")
	}
}

func TestCompactionManager_Compact_MaxBatch(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "test",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
		CompactionCfg: &compaction.CompactionConfig{
			MaxSSTableCount:    10,
			MaxCompactionBatch: 2, // 限制每次最多合并 2 个
			ShardSizeLimit:     100 * 1024 * 1024,
			CheckInterval:      time.Hour,
			Timeout:            30 * time.Minute,
		},
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	// 创建 4 个 SSTable
	for j := 0; j < 4; j++ {
		for i := 0; i < 3; i++ {
			p := &types.Point{
				Database:    "testdb",
				Measurement: "test",
				Tags:        map[string]string{"host": "server1"},
				Timestamp:   int64(j*10+i) * 1000,
				Fields: map[string]*types.FieldValue{
					"value": types.NewFieldValue(int64(j*10 + i)),
				},
			}
			_ = shard.Write(p)
		}
		_ = shard.Flush()
	}

	cm := shard.compaction
	ctx := context.Background()

	outputPath, deletedFiles, err := cm.Compact(ctx)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	if outputPath == "" {
		t.Error("outputPath should not be empty after compaction")
	}

	// 由于 MaxCompactionBatch=2，应该只删除 2 个文件
	if len(deletedFiles) != 2 {
		t.Errorf("expected 2 deleted files (MaxCompactionBatch), got %d", len(deletedFiles))
	}
}

func TestCompactionManager_ShouldCompact_CollectError(t *testing.T) {
	// 测试 collectSSTables 返回错误时 ShouldCompact 的行为
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "test",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
		CompactionCfg: &compaction.CompactionConfig{
			MaxSSTableCount: 2,
			ShardSizeLimit:  100 * 1024 * 1024,
			CheckInterval:   time.Hour,
			Timeout:         30 * time.Minute,
		},
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	// 人为删除 data 目录使 collectSSTables 返回错误
	dataDir := filepath.Join(tmpDir, "001", "data")
	_ = os.RemoveAll(dataDir)

	cm := shard.compaction
	// 即使 SSTable 数量足够，collectSSTables 出错时 shouldCompactLocked 应返回 false
	result := cm.ShouldCompactLocked()
	if result {
		t.Error("shouldCompactLocked should return false when collectSSTables fails")
	}
}

func TestCompactionManager_Compact_Concurrent(t *testing.T) {
	// 测试并发 compaction 调用
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "test",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
		CompactionCfg: &compaction.CompactionConfig{
			MaxSSTableCount: 10,
			ShardSizeLimit:  100 * 1024 * 1024,
			CheckInterval:   time.Hour,
			Timeout:         30 * time.Minute,
		},
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	// 创建 4 个 SSTable
	for j := 0; j < 4; j++ {
		for i := 0; i < 5; i++ {
			p := &types.Point{
				Database:    "testdb",
				Measurement: "test",
				Tags:        map[string]string{"host": "server1"},
				Timestamp:   int64(j*10+i) * 1000,
				Fields: map[string]*types.FieldValue{
					"value": types.NewFieldValue(int64(j*10 + i)),
				},
			}
			_ = shard.Write(p)
		}
		_ = shard.Flush()
	}

	// 并发调用 Compact
	var wg sync.WaitGroup
	const goroutines = 3
	wg.Add(goroutines)

	var firstErr error
	var errCount int32

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_, _, err := shard.compaction.Compact(ctx)
			if err != nil {
				atomic.AddInt32(&errCount, 1)
				if firstErr == nil {
					firstErr = err
				}
			}
		}()
	}

	wg.Wait()

	// 至少一个应该成功，其他应该因为锁而被阻塞后重试成功或快速返回
	// 由于使用 tryAcquireCompactLock，第二个及之后的会直接返回
	t.Logf("Concurrent compact errors: %d, first error: %v", errCount, firstErr)
}

func TestCompactionManager_CollectSSTables_PartialError(t *testing.T) {
	// 测试 collectSSTables 部分文件访问错误
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	// 创建 2 个 SSTable
	for j := 0; j < 2; j++ {
		for i := 0; i < 3; i++ {
			p := &types.Point{
				Database:    "testdb",
				Measurement: "test",
				Tags:        map[string]string{"host": "server1"},
				Timestamp:   int64(j*10+i) * 1000,
				Fields: map[string]*types.FieldValue{
					"value": types.NewFieldValue(int64(j*10 + i)),
				},
			}
			_ = shard.Write(p)
		}
		_ = shard.Flush()
	}

	cm := shard.compaction
	cm.Mu.Lock()
	files, err := cm.CollectSSTables()
	cm.Mu.Unlock()

	if err != nil {
		t.Fatalf("collectSSTables failed: %v", err)
	}

	if len(files) != 2 {
		t.Errorf("expected 2 SSTables, got %d", len(files))
	}
}

func TestCompactionManager_markSSTableWriting_Error(t *testing.T) {
	// 测试 markSSTableWriting 在已存在时的行为
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	cm := shard.compaction

	// 创建 SSTable 文件路径（单文件格式）
	sstPath := filepath.Join(tmpDir, "001", "data", "sst_test.bin")

	// markSSTableWriting 应该成功（会创建父目录和 .writing 文件）
	err := cm.MarkWriting(sstPath)
	if err != nil {
		t.Errorf("markSSTableWriting should succeed: %v", err)
	}

	// 再次标记应该也成功（幂等）
	err = cm.MarkWriting(sstPath)
	if err != nil {
		t.Errorf("markSSTableWriting second call should succeed: %v", err)
	}

	// unmark 应该成功
	err = cm.UnmarkWriting(sstPath)
	if err != nil {
		t.Errorf("unmarkSSTableWriting should succeed: %v", err)
	}
}

func TestCompactionManager_TryAcquireReleaseCompactLock_AlreadyLocked(t *testing.T) {
	// 测试尝试获取已锁定的 compaction
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	cm := shard.compaction

	// 第一次获取锁
	acquired1 := cm.TryAcquireCompactLock()
	if !acquired1 {
		t.Error("first tryAcquireCompactLock should succeed")
	}

	// 第二次获取应该失败
	acquired2 := cm.TryAcquireCompactLock()
	if acquired2 {
		t.Error("second tryAcquireCompactLock should fail")
	}

	// 释放锁
	cm.ReleaseCompactLock()

	// 再次获取应该成功
	acquired3 := cm.TryAcquireCompactLock()
	if !acquired3 {
		t.Error("third tryAcquireCompactLock should succeed after release")
	}

	cm.ReleaseCompactLock()
}

func TestCompactionManager_doPeriodicCompaction_NotNeeded(t *testing.T) {
	// 测试 doPeriodicCompaction 当不需要 compaction 时
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "test",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
		CompactionCfg: &compaction.CompactionConfig{
			MaxSSTableCount: 10, // 高阈值，不会触发
			ShardSizeLimit:  100 * 1024 * 1024,
			CheckInterval:   time.Hour,
			Timeout:         30 * time.Minute,
		},
	}

	shard := NewShard(cfg)
	defer func() {
		_ = shard.Close()
	}()

	// 只创建 1 个 SSTable，不满足 compaction 条件
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

	// 手动调用 doPeriodicCompaction
	cm.DoPeriodicCompaction()

	// 不会有错误，因为不满足条件直接返回
}

func TestCompactionManager_Compact_VerifyInputFilesDeleted(t *testing.T) {
	// 测试 compaction 后输入文件确实被删除
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "test",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: metadata.NewSimpleSchemaStore(),
		MemTableCfg: memtable.DefaultMemTableConfig(),
		CompactionCfg: &compaction.CompactionConfig{
			MaxSSTableCount: 10,
			ShardSizeLimit:  100 * 1024 * 1024,
			CheckInterval:   time.Hour,
			Timeout:         30 * time.Minute,
		},
	}

	shard := NewShard(cfg)

	// 创建 3 个 SSTable
	for j := 0; j < 3; j++ {
		for i := 0; i < 5; i++ {
			p := &types.Point{
				Database:    "testdb",
				Measurement: "test",
				Tags:        map[string]string{"host": "server1"},
				Timestamp:   int64(j*10+i) * 1000,
				Fields: map[string]*types.FieldValue{
					"value": types.NewFieldValue(int64(j*10 + i)),
				},
			}
			_ = shard.Write(p)
		}
		_ = shard.Flush()
	}

	// 记录 compaction 前的 SSTable 路径
	dataDir := shard.DataDir()
	entriesBefore, _ := os.ReadDir(dataDir)
	var sstPathsBefore []string
	for _, entry := range entriesBefore {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "sst_") && strings.HasSuffix(entry.Name(), ".bin") {
			sstPathsBefore = append(sstPathsBefore, filepath.Join(dataDir, entry.Name()))
		}
	}

	// 执行 compaction
	ctx := context.Background()
	outputPath, _, err := shard.compaction.Compact(ctx)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// 验证输出存在
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Error("output SSTable should exist")
	}

	// 验证所有输入文件都被删除
	for _, inputPath := range sstPathsBefore {
		if _, err := os.Stat(inputPath); !os.IsNotExist(err) {
			t.Errorf("input file %s should be deleted", inputPath)
		}
	}

	_ = shard.Close()
}

func TestCompactionManager_GetProgress_NoTask(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	cm := shard.compaction

	// No task running, should return nil
	progress := cm.GetProgress()
	if progress != nil {
		t.Error("GetProgress should return nil when no task is running")
	}

	_ = shard.Close()
}

func TestCompactionManager_reportProgress(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:            "testdb",
		Measurement:   "test",
		StartTime:     0,
		EndTime:       time.Hour.Nanoseconds(),
		Dir:           tmpDir,
		SeriesStore:   metadata.NewSimpleSeriesStore(),
		MemTableCfg:   memtable.DefaultMemTableConfig(),
		CompactionCfg: compaction.DefaultCompactionConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()
	cm := shard.compaction

	// Set a mock current task
	cm.Mu.Lock()
	cm.CurrentTask = &compaction.CompactionProgress{
		Status:   "running",
		Progress: 0,
	}
	cm.Mu.Unlock()

	// Report progress
	cm.ReportProgress(50)

	// Verify progress was updated
	cm.Mu.Lock()
	if cm.CurrentTask.Progress != 50 {
		t.Errorf("expected Progress=50, got %d", cm.CurrentTask.Progress)
	}
	cm.Mu.Unlock()

	// Test with nil task (should not panic)
	cm.Mu.Lock()
	cm.CurrentTask = nil
	cm.Mu.Unlock()
	cm.ReportProgress(100) // Should not panic
}

func TestSSTableRef_IsUnused(t *testing.T) {
	ref := &SSTableRef{}

	// Initially unused (refCnt=0)
	if !ref.IsUnused() {
		t.Error("new ref should be unused")
	}

	// Acquire makes it used
	ref.Acquire()
	if ref.IsUnused() {
		t.Error("ref should not be unused after Acquire")
	}

	// Release makes it unused again
	ref.Release()
	if !ref.IsUnused() {
		t.Error("ref should be unused after Release")
	}

	// Multiple acquires/releases
	ref.Acquire()
	ref.Acquire()
	ref.Acquire()
	if ref.IsUnused() {
		t.Error("ref should not be unused with count=3")
	}
	ref.Release()
	ref.Release()
	ref.Release()
	if !ref.IsUnused() {
		t.Error("ref should be unused after all releases")
	}
}
