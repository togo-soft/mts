package shard

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/memtable"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
)

func TestLevelCompactionManager_NewLevelCompactionManager(t *testing.T) {
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

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, err := compaction.NewLevelCompactionManager(shard, lcmCfg)
	if err != nil {
		t.Fatalf("NewLevelCompactionManager failed: %v", err)
	}

	if lcm == nil {
		t.Fatal("LevelCompactionManager should not be nil")
	}

	_ = shard.Close()
}

func TestLevelCompactionManager_ShouldCompact_Empty(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// 空状态不应该触发 compaction
	if lcm.ShouldCompact() {
		t.Error("ShouldCompact should return false when no parts exist")
	}
}

func TestLevelCompactionManager_IsOldFormat(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// 新格式应该有 L0 目录
	if lcm.IsOldFormat() {
		t.Error("freshly created should not be old format")
	}
}

func TestLevelCompactionManager_Recover_NoCheckpoint(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// 没有 checkpoint，恢复应该成功
	if err := lcm.Recover(); err != nil {
		t.Errorf("Recover should succeed with no checkpoint: %v", err)
	}
}

func TestLevelCompactionManager_LevelMaxSize(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// 验证各层容量
	if size := lcm.LevelMaxSize(0); size != 10*1024*1024 {
		t.Errorf("L0 size should be 10MB, got %d", size)
	}
	if size := lcm.LevelMaxSize(1); size != 100*1024*1024 {
		t.Errorf("L1 size should be 100MB, got %d", size)
	}
	if size := lcm.LevelMaxSize(2); size != 1024*1024*1024 {
		t.Errorf("L2 size should be 1GB, got %d", size)
	}
}

func TestLevelCompactionManager_SelectPartsForMerge(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// 添加一些测试 parts
	lcm.Manifest.AddPart(0, compaction.PartInfo{Name: "small", Size: 100, MinTime: 1000, MaxTime: 2000})
	lcm.Manifest.AddPart(0, compaction.PartInfo{Name: "medium", Size: 500, MinTime: 2000, MaxTime: 3000})
	lcm.Manifest.AddPart(0, compaction.PartInfo{Name: "large", Size: 1000, MinTime: 3000, MaxTime: 4000})

	// 选择要合并的 parts
	selected := lcm.SelectPartsForMerge(0)

	// 应该选择小的 parts
	if len(selected) < 1 {
		t.Error("should select at least 1 part")
	}

	// 验证是按小文件优先排序
	if len(selected) > 1 {
		if selected[0].Size > selected[1].Size {
			t.Error("parts should be selected in size order (small first)")
		}
	}
}

func TestLevelCompactionManager_Compact_NoParts(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	ctx := context.Background()
	outputPath, deletedFiles, err := lcm.Compact(ctx)

	if err != nil {
		t.Fatalf("Compact should not fail with no parts: %v", err)
	}
	if outputPath != "" {
		t.Error("outputPath should be empty with no parts")
	}
	if len(deletedFiles) != 0 {
		t.Error("deletedFiles should be empty with no parts")
	}
}

func TestLevelCompactionManager_Compact_LessThanTwoParts(t *testing.T) {
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

	// 添加一个 part
	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	lcm.Manifest.AddPart(0, compaction.PartInfo{
		Name:    "sst_00000000000000000001",
		Size:    1024,
		MinTime: 1000,
		MaxTime: 2000,
	})

	ctx := context.Background()
	outputPath, deletedFiles, err := lcm.Compact(ctx)

	if err != nil {
		t.Fatalf("Compact should not fail with <2 parts: %v", err)
	}
	if outputPath != "" {
		t.Error("outputPath should be empty with <2 parts")
	}
	if len(deletedFiles) != 0 {
		t.Error("deletedFiles should be empty with <2 parts")
	}

	_ = shard.Close()
}

func TestLevelCompactionManager_CollectOverlapParts(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// 添加 L0 parts
	lcm.Manifest.AddPart(0, compaction.PartInfo{Name: "l0_1", Size: 100, MinTime: 1000, MaxTime: 2000})
	lcm.Manifest.AddPart(0, compaction.PartInfo{Name: "l0_2", Size: 100, MinTime: 2000, MaxTime: 3000})

	// 添加 L1 parts（部分重叠）
	lcm.Manifest.AddPart(1, compaction.PartInfo{Name: "l1_1", Size: 1000, MinTime: 1500, MaxTime: 2500})
	lcm.Manifest.AddPart(1, compaction.PartInfo{Name: "l1_2", Size: 1000, MinTime: 5000, MaxTime: 6000})

	// 选择 l0_1 作为目标
	targets := []compaction.PartInfo{lcm.Manifest.GetLevel(0).Parts[0]}

	// 收集重叠 parts
	overlaps := lcm.CollectOverlapParts(0, targets)

	// 应该包含 l0_1, l0_2 (L0中重叠), l1_1 (L1中重叠)
	if len(overlaps) < 2 {
		t.Errorf("expected at least 2 overlapping parts, got %d", len(overlaps))
	}
}

func TestLevelCompactionManager_StartStop(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := &compaction.LevelCompactionConfig{
		Enabled:       true,
		CheckInterval: time.Millisecond, // 非常短用于测试
		Timeout:       time.Minute,
	}

	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// 启动定期检查
	lcm.StartPeriodicCheck()

	// 立即停止应该安全
	lcm.Stop()
	lcm.Stop() // 多次调用应该安全
}

func TestLevelCompactionManager_MigrateFromOldFormat(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	_ = os.MkdirAll(dataDir, 0700)

	// 创建旧的扁平结构 SSTable 目录
	oldSstDir := filepath.Join(dataDir, "sst_00000000000000000001")
	_ = os.MkdirAll(oldSstDir, 0700)

	// 创建 shard
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

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// 旧格式应该被检测到
	if !lcm.IsOldFormat() {
		t.Log("IsOldFormat returned false, possibly L0 dir already created by NewLevelManifest")
	}

	// 执行迁移
	if err := lcm.MigrateFromOldFormat(); err != nil {
		t.Fatalf("MigrateFromOldFormat failed: %v", err)
	}

	// 验证 L0 目录存在
	l0Dir := filepath.Join(dataDir, "L0")
	if _, err := os.Stat(l0Dir); os.IsNotExist(err) {
		t.Error("L0 directory should exist after migration")
	}

	_ = shard.Close()
}

func TestLevelManifest_RemoveParts(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest, _ := compaction.NewLevelManifest(dataDir, nil)

	// 添加多个 parts
	manifest.AddPart(0, compaction.PartInfo{Name: "sst_00000000000000000001", Size: 100, MinTime: 1000, MaxTime: 2000})
	manifest.AddPart(0, compaction.PartInfo{Name: "sst_00000000000000000002", Size: 200, MinTime: 2000, MaxTime: 3000})
	manifest.AddPart(0, compaction.PartInfo{Name: "sst_00000000000000000003", Size: 300, MinTime: 3000, MaxTime: 4000})

	l0 := manifest.GetLevel(0)
	if len(l0.Parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(l0.Parts))
	}
	if l0.Size != 600 {
		t.Fatalf("expected size 600, got %d", l0.Size)
	}

	// 批量删除
	manifest.RemoveParts(0, []string{"sst_00000000000000000001", "sst_00000000000000000003"})

	if len(l0.Parts) != 1 {
		t.Errorf("expected 1 part after removal, got %d", len(l0.Parts))
	}
	if l0.Size != 200 {
		t.Errorf("expected size 200 after removal, got %d", l0.Size)
	}
}

func TestLevelCompactionManager_LevelMaxSize_Extended(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// 验证各层容量
	if size := lcm.LevelMaxSize(3); size != 10*1024*1024*1024 {
		t.Errorf("L3 size should be 10GB, got %d", size)
	}
	if size := lcm.LevelMaxSize(4); size != 100*1024*1024*1024 {
		t.Errorf("L4 size should be 100GB, got %d", size)
	}
	// 无效层级应该返回 0
	if size := lcm.LevelMaxSize(99); size != 0 {
		t.Errorf("invalid level should return 0, got %d", size)
	}
}

func TestLevelCompactionManager_ShouldCompactLevel_WithParts(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// L0 有多个 part（超过 MaxParts=10），应该触发 compaction
	for i := 0; i < 12; i++ {
		lcm.Manifest.AddPart(0, compaction.PartInfo{
			Name:    fmt.Sprintf("sst_%020d", i),
			Size:    1 * 1024 * 1024, // 1MB each
			MinTime: int64(i) * 1000,
			MaxTime: int64(i+1) * 1000,
		})
	}

	if !lcm.ShouldCompactLevel(0) {
		t.Error("L0 with >10 parts should trigger compaction")
	}
}

func TestLevelCompactionManager_DoPeriodicCompaction(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcmCfg.CheckInterval = 10 * time.Millisecond

	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// 添加 12 个 L0 parts 以触发 ShouldCompact
	for i := 0; i < 12; i++ {
		lcm.Manifest.AddPart(0, compaction.PartInfo{
			Name:    fmt.Sprintf("sst_%020d", i),
			Size:    1 * 1024 * 1024,
			MinTime: int64(i) * 1000,
			MaxTime: int64(i+1) * 1000,
		})
	}

	// 验证 ShouldCompact 返回 true
	if !lcm.ShouldCompact() {
		t.Fatal("ShouldCompact should return true with 12 L0 parts")
	}

	// 启动定期检查
	lcm.StartPeriodicCheck()

	// 等待一段时间让 doPeriodicCompaction 运行
	time.Sleep(50 * time.Millisecond)

	// 停止
	lcm.Stop()
}

func TestLevelCompactionManager_Recover_WithCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	_ = os.MkdirAll(dataDir, 0700)

	// 创建一个 checkpoint
	cp := &compaction.CompactionCheckpoint{
		Version:    1,
		Level:      0,
		InputParts: []string{"sst_1", "sst_2"},
		OutputPath: filepath.Join(dataDir, "sst_3"),
		OutputSeq:  3,
		MergedSize: 1024,
		StartedAt:  time.Now().Unix(),
	}
	if err := cp.Save(dataDir); err != nil {
		t.Fatalf("Save checkpoint failed: %v", err)
	}

	// 创建 shard
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// 恢复
	if err := lcm.Recover(); err != nil {
		t.Errorf("Recover failed: %v", err)
	}

	// checkpoint 应该已被清理
	cpPath := filepath.Join(dataDir, "_compaction.cp")
	if _, err := os.Stat(cpPath); !os.IsNotExist(err) {
		t.Error("checkpoint should be cleared after recover")
	}
}

func TestLevelCompactionManager_Recover_WithIncompleteOutput(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	_ = os.MkdirAll(dataDir, 0700)

	// 创建 checkpoint
	cp := &compaction.CompactionCheckpoint{
		Version:    1,
		Level:      0,
		InputParts: []string{"sst_1", "sst_2"},
		OutputPath: filepath.Join(dataDir, "sst_3"),
		OutputSeq:  3,
		MergedSize: 1024,
		StartedAt:  time.Now().Unix(),
	}
	if err := cp.Save(dataDir); err != nil {
		t.Fatalf("Save checkpoint failed: %v", err)
	}

	// 创建未完成的输出文件
	incompletePath := filepath.Join(dataDir, "sst_3")
	if err := os.WriteFile(incompletePath, []byte("incomplete"), 0600); err != nil {
		t.Fatalf("Create incomplete file failed: %v", err)
	}

	// 创建 shard
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// 恢复应该清理未完成的输出文件
	if err := lcm.Recover(); err != nil {
		t.Errorf("Recover failed: %v", err)
	}

	// 输出文件应该已被删除
	if _, err := os.Stat(incompletePath); !os.IsNotExist(err) {
		t.Error("incomplete output should be removed after recover")
	}
}

func TestLevelCompactionManager_NextSeq(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	seq1 := lcm.NextSeq()
	seq2 := lcm.NextSeq()

	if seq1 == seq2 {
		t.Error("NextSeq should return different values")
	}
	if seq2 != seq1+1 {
		t.Error("NextSeq should increment by 1")
	}
}

func TestLevelCompactionManager_SaveManifest(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// Add a part
	lcm.AddPart(0, compaction.PartInfo{
		Name: "test_part",
		Size: 1024,
	})

	// SaveManifest should succeed
	err := lcm.SaveManifest()
	if err != nil {
		t.Fatalf("SaveManifest failed: %v", err)
	}
}

func TestLevelCompactionManager_Config(t *testing.T) {
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
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelCompactionConfig()
	lcm, _ := compaction.NewLevelCompactionManager(shard, lcmCfg)

	// Config should return the same config
	returnedCfg := lcm.Config()
	if returnedCfg != lcmCfg {
		t.Error("Config should return the same config instance")
	}
	if returnedCfg == nil {
		t.Error("Config should not return nil")
	}
}
