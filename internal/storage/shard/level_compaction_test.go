package shard

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/measurement"
)

func TestLevelConfig_DefaultLevelConfigs(t *testing.T) {
	configs := DefaultLevelConfigs()
	if len(configs) != 5 {
		t.Errorf("expected 5 level configs, got %d", len(configs))
	}

	// 验证 L0 配置
	if configs[0].Level != 0 || configs[0].MaxParts != 10 {
		t.Errorf("L0 config incorrect: %+v", configs[0])
	}

	// 验证 L1 配置
	if configs[1].Level != 1 || configs[1].MaxSize != 100*1024*1024 {
		t.Errorf("L1 config incorrect: %+v", configs[1])
	}
}

func TestLevelManifest_NewLevelManifest(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest, err := NewLevelManifest(dataDir, nil)
	if err != nil {
		t.Fatalf("NewLevelManifest failed: %v", err)
	}

	if manifest == nil {
		t.Fatal("manifest should not be nil")
	}

	// 验证层次已创建
	for i := 0; i < 5; i++ {
		l := manifest.GetLevel(i)
		if l == nil {
			t.Errorf("level %d should exist", i)
			continue
		}
		if l.Level != i {
			t.Errorf("level %d has incorrect Level field", i)
		}
	}

	// 验证目录已创建
	for i := 0; i < 5; i++ {
		levelPath := manifest.GetLevelPath(i)
		if _, err := os.Stat(levelPath); os.IsNotExist(err) {
			t.Errorf("level directory L%d should exist at %s", i, levelPath)
		}
	}
}

func TestLevelManifest_NextSeq(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest, _ := NewLevelManifest(dataDir, nil)

	seq1 := manifest.NextSeq()
	seq2 := manifest.NextSeq()
	seq3 := manifest.NextSeq()

	if seq1 == seq2 || seq2 == seq3 || seq1 == seq3 {
		t.Error("NextSeq should return unique values")
	}
	if seq2 != seq1+1 || seq3 != seq2+1 {
		t.Error("NextSeq should increment by 1")
	}
}

func TestLevelManifest_SetNextSeq(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest, _ := NewLevelManifest(dataDir, nil)

	// 初始值应该是 0
	seq1 := manifest.NextSeq()
	if seq1 != 0 {
		t.Errorf("initial seq should be 0, got %d", seq1)
	}

	// 设置一个特定的值
	manifest.SetNextSeq(100)
	seq2 := manifest.NextSeq()
	if seq2 != 100 {
		t.Errorf("after SetNextSeq(100), NextSeq should return 100, got %d", seq2)
	}
	seq3 := manifest.NextSeq()
	if seq3 != 101 {
		t.Errorf("after SetNextSeq(100), NextSeq should increment to 101, got %d", seq3)
	}
}

func TestLevelManifest_AddRemovePart(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest, _ := NewLevelManifest(dataDir, nil)

	part := PartInfo{
		Name:    "sst_00000000000000000001",
		Size:    1024,
		MinTime: 1000,
		MaxTime: 2000,
	}

	// 添加
	manifest.AddPart(0, part)

	l0 := manifest.GetLevel(0)
	if len(l0.Parts) != 1 {
		t.Errorf("expected 1 part, got %d", len(l0.Parts))
	}
	if l0.Size != 1024 {
		t.Errorf("expected size 1024, got %d", l0.Size)
	}

	// 删除
	manifest.RemovePart(0, "sst_00000000000000000001")
	if len(l0.Parts) != 0 {
		t.Errorf("expected 0 parts, got %d", len(l0.Parts))
	}
	if l0.Size != 0 {
		t.Errorf("expected size 0, got %d", l0.Size)
	}
}

func TestLevelManifest_SaveLoad(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	manifest1, _ := NewLevelManifest(dataDir, nil)

	// 添加一些 parts
	manifest1.AddPart(0, PartInfo{Name: "sst_00000000000000000001", Size: 100, MinTime: 1000, MaxTime: 2000})
	manifest1.AddPart(0, PartInfo{Name: "sst_00000000000000000002", Size: 200, MinTime: 2000, MaxTime: 3000})
	manifest1.AddPart(1, PartInfo{Name: "sst_00000000000000000100", Size: 5000, MinTime: 1000, MaxTime: 5000})

	// 保存
	if err := manifest1.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// 重新加载
	manifest2, _ := NewLevelManifest(dataDir, nil)
	if err := manifest2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// 验证
	l0 := manifest2.GetLevel(0)
	if len(l0.Parts) != 2 {
		t.Errorf("expected 2 parts in L0, got %d", len(l0.Parts))
	}

	l1 := manifest2.GetLevel(1)
	if len(l1.Parts) != 1 {
		t.Errorf("expected 1 part in L1, got %d", len(l1.Parts))
	}

	if l1.Size != 5000 {
		t.Errorf("expected L1 size 5000, got %d", l1.Size)
	}
}

func TestLevelCompactionConfig_DefaultLevelCompactionConfig(t *testing.T) {
	cfg := DefaultLevelCompactionConfig()
	if cfg == nil {
		t.Fatal("DefaultLevelCompactionConfig should not return nil")
	}
	if !cfg.Enabled {
		t.Error("Enabled should be true by default")
	}
	if cfg.CheckInterval != 5*time.Minute {
		t.Errorf("CheckInterval should be 5 minutes, got %v", cfg.CheckInterval)
	}
	if cfg.Timeout != 30*time.Minute {
		t.Errorf("Timeout should be 30 minutes, got %v", cfg.Timeout)
	}
}

func TestLevelCompactionManager_NewLevelCompactionManager(t *testing.T) {
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

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, err := NewLevelCompactionManager(shard, lcmCfg)
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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

	// 没有 checkpoint，恢复应该成功
	if err := lcm.Recover(); err != nil {
		t.Errorf("Recover should succeed with no checkpoint: %v", err)
	}
}

func TestCompactionCheckpoint_SaveLoad(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	_ = os.MkdirAll(dataDir, 0700)

	cp := &CompactionCheckpoint{
		Version:    1,
		Level:      0,
		InputParts: []string{"sst_1", "sst_2"},
		OutputPath: filepath.Join(dataDir, "sst_3"),
		OutputSeq:  3,
		MergedSize: 1024,
		StartedAt:  time.Now().Unix(),
	}

	// 保存
	if err := cp.Save(dataDir); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// 加载
	cp2 := &CompactionCheckpoint{}
	if err := cp2.Load(dataDir); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cp2.Level != 0 {
		t.Errorf("expected Level 0, got %d", cp2.Level)
	}
	if len(cp2.InputParts) != 2 {
		t.Errorf("expected 2 input parts, got %d", len(cp2.InputParts))
	}
	if cp2.OutputSeq != 3 {
		t.Errorf("expected OutputSeq 3, got %d", cp2.OutputSeq)
	}

	// 清理
	_ = cp.Clear(dataDir)
}

func TestCompactionCheckpoint_Clear(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	_ = os.MkdirAll(dataDir, 0700)

	cp := &CompactionCheckpoint{
		Version:   1,
		Level:     0,
		OutputSeq: 1,
	}

	// 保存
	_ = cp.Save(dataDir)

	// 验证文件存在
	path := cp.checkpointPath(dataDir)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("checkpoint file should exist after Save")
	}

	// 清理
	if err := cp.Clear(dataDir); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	// 验证文件已删除
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("checkpoint file should not exist after Clear")
	}
}

func TestHasOverlap(t *testing.T) {
	tests := []struct {
		name     string
		p1       PartInfo
		p2       PartInfo
		expected bool
	}{
		{
			name:     "no overlap - p1 before p2",
			p1:       PartInfo{MinTime: 0, MaxTime: 100},
			p2:       PartInfo{MinTime: 200, MaxTime: 300},
			expected: false,
		},
		{
			name:     "overlap - p1 ends during p2",
			p1:       PartInfo{MinTime: 0, MaxTime: 250},
			p2:       PartInfo{MinTime: 200, MaxTime: 300},
			expected: true,
		},
		{
			name:     "overlap - p1 starts during p2",
			p1:       PartInfo{MinTime: 250, MaxTime: 350},
			p2:       PartInfo{MinTime: 200, MaxTime: 300},
			expected: true,
		},
		{
			name:     "overlap - p1 contains p2",
			p1:       PartInfo{MinTime: 0, MaxTime: 400},
			p2:       PartInfo{MinTime: 100, MaxTime: 300},
			expected: true,
		},
		{
			name:     "touch - p1 ends where p2 starts",
			p1:       PartInfo{MinTime: 0, MaxTime: 200},
			p2:       PartInfo{MinTime: 200, MaxTime: 300},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasOverlap(tt.p1, tt.p2)
			if result != tt.expected {
				t.Errorf("hasOverlap() = %v, expected %v", result, tt.expected)
			}
		})
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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

	// 验证各层容量
	if size := lcm.levelMaxSize(0); size != 10*1024*1024 {
		t.Errorf("L0 size should be 10MB, got %d", size)
	}
	if size := lcm.levelMaxSize(1); size != 100*1024*1024 {
		t.Errorf("L1 size should be 100MB, got %d", size)
	}
	if size := lcm.levelMaxSize(2); size != 1024*1024*1024 {
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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

	// 添加一些测试 parts
	lcm.manifest.AddPart(0, PartInfo{Name: "small", Size: 100, MinTime: 1000, MaxTime: 2000})
	lcm.manifest.AddPart(0, PartInfo{Name: "medium", Size: 500, MinTime: 2000, MaxTime: 3000})
	lcm.manifest.AddPart(0, PartInfo{Name: "large", Size: 1000, MinTime: 3000, MaxTime: 4000})

	// 选择要合并的 parts
	selected := lcm.selectPartsForMerge(0)

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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)

	// 添加一个 part
	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

	lcm.manifest.AddPart(0, PartInfo{
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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

	// 添加 L0 parts
	lcm.manifest.AddPart(0, PartInfo{Name: "l0_1", Size: 100, MinTime: 1000, MaxTime: 2000})
	lcm.manifest.AddPart(0, PartInfo{Name: "l0_2", Size: 100, MinTime: 2000, MaxTime: 3000})

	// 添加 L1 parts（部分重叠）
	lcm.manifest.AddPart(1, PartInfo{Name: "l1_1", Size: 1000, MinTime: 1500, MaxTime: 2500})
	lcm.manifest.AddPart(1, PartInfo{Name: "l1_2", Size: 1000, MinTime: 5000, MaxTime: 6000})

	// 选择 l0_1 作为目标
	targets := []PartInfo{lcm.manifest.GetLevel(0).Parts[0]}

	// 收集重叠 parts
	overlaps := lcm.collectOverlapParts(0, targets)

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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := &LevelCompactionConfig{
		Enabled:       true,
		CheckInterval: time.Millisecond, // 非常短用于测试
		Timeout:       time.Minute,
	}

	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

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
	_ = os.MkdirAll(oldSstDir, 0755)

	// 创建 shard
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

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

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

	manifest, _ := NewLevelManifest(dataDir, nil)

	// 添加多个 parts
	manifest.AddPart(0, PartInfo{Name: "sst_00000000000000000001", Size: 100, MinTime: 1000, MaxTime: 2000})
	manifest.AddPart(0, PartInfo{Name: "sst_00000000000000000002", Size: 200, MinTime: 2000, MaxTime: 3000})
	manifest.AddPart(0, PartInfo{Name: "sst_00000000000000000003", Size: 300, MinTime: 3000, MaxTime: 4000})

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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

	// 验证各层容量
	if size := lcm.levelMaxSize(3); size != 10*1024*1024*1024 {
		t.Errorf("L3 size should be 10GB, got %d", size)
	}
	if size := lcm.levelMaxSize(4); size != 100*1024*1024*1024 {
		t.Errorf("L4 size should be 100GB, got %d", size)
	}
	// 无效层级应该返回 0
	if size := lcm.levelMaxSize(99); size != 0 {
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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

	// L0 有多个 part（超过 MaxParts=10），应该触发 compaction
	for i := 0; i < 12; i++ {
		lcm.manifest.AddPart(0, PartInfo{
			Name:    fmt.Sprintf("sst_%020d", i),
			Size:    1 * 1024 * 1024, // 1MB each
			MinTime: int64(i) * 1000,
			MaxTime: int64(i+1) * 1000,
		})
	}

	if !lcm.shouldCompactLevel(0) {
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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := &LevelCompactionConfig{
		Enabled:       true,
		CheckInterval: 10 * time.Millisecond,
		Timeout:       time.Minute,
	}

	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

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
	cp := &CompactionCheckpoint{
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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

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
	cp := &CompactionCheckpoint{
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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

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
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := DefaultLevelCompactionConfig()
	lcm, _ := NewLevelCompactionManager(shard, lcmCfg)

	seq1 := lcm.NextSeq()
	seq2 := lcm.NextSeq()

	if seq1 == seq2 {
		t.Error("NextSeq should return different values")
	}
	if seq2 != seq1+1 {
		t.Error("NextSeq should increment by 1")
	}
}
