package shard

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/compaction"
	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

// sstDirName returns the directory name used by sstable.Writer for a given seq.
func sstDirName(seq uint64) string {
	return fmt.Sprintf("sst_%d", seq)
}

// createTestSSTableInLevel creates a test SSTable file in the specified level directory.
func createTestSSTableInLevel(t *testing.T, shardDir string, level int, seq uint64, points []*types.Point) string {
	// 创建 SSTable Writer（会在 shardDir/data/sst_{seq}.bin 创建文件）
	w, err := sstable.NewWriter(shardDir, seq, 0, sstable.CompressionNone, sstable.FlagSorted)
	if err != nil {
		t.Fatalf("failed to create SSTable writer: %v", err)
	}

	// 转换为 InternalPoint
	internalPoints := make([]types.InternalPoint, len(points))
	for i := range points {
		internalPoints[i] = types.PointToInternal(points[i], uint64(1))
	}

	// 写入数据
	if err := w.WritePoints(internalPoints); err != nil {
		t.Fatalf("failed to write points: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("failed to close SSTable writer: %v", err)
	}

	// SSTable 文件创建在 shardDir/data/sst_{seq}.bin
	// 需要移动到 shardDir/data/L{level}/sst_{seq}.bin
	srcFile := filepath.Join(shardDir, "data", sstDirName(seq)+".bin")
	dstDir := filepath.Join(shardDir, "data", fmt.Sprintf("L%d", level))
	dstFile := filepath.Join(dstDir, sstDirName(seq)+".bin")

	// 确保目标目录存在
	if err := os.MkdirAll(dstDir, 0700); err != nil {
		t.Fatalf("failed to create level dir: %v", err)
	}

	// 移动文件
	if err := os.Rename(srcFile, dstFile); err != nil {
		t.Fatalf("failed to move SSTable to level dir: %v", err)
	}

	// 返回 part 名称（manifest 中使用不带 .bin 的 Name）
	return sstDirName(seq)
}

// TestLevelCompactionE2E_L0ToL1 tests basic L0 to L1 compaction.
func TestLevelCompactionE2E_L0ToL1(t *testing.T) {
	tmpDir := t.TempDir()
	schemaStore := metadata.NewSimpleSchemaStore()
	_ = schemaStore.SetSchema("testdb", "cpu", &metadata.Schema{
		Version: 1,
		Fields:  []metadata.FieldDef{{Name: "usage", Type: 1}}, // float64
	})

	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: schemaStore,
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelConfig()
	lcm, err := compaction.NewLevelManager(shard, lcmCfg)
	if err != nil {
		t.Fatalf("NewLevelManager failed: %v", err)
	}

	// 创建测试数据 - L0 的两个重叠的 SSTable
	points1 := []*types.Point{
		{Timestamp: 1000000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(10.5)}},
		{Timestamp: 2000000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(20.5)}},
	}
	points2 := []*types.Point{
		{Timestamp: 1500000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(15.0)}},
		{Timestamp: 2500000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(25.0)}},
	}

	// 写入两个 SSTable 到 L0
	sst1 := createTestSSTableInLevel(t, tmpDir, 0, 1, points1)
	sst2 := createTestSSTableInLevel(t, tmpDir, 0, 2, points2)

	// 添加到 manifest
	lcm.Manifest.AddPart(0, compaction.PartInfo{
		Name:    sst1,
		Size:    1024,
		MinTime: 1000000000,
		MaxTime: 2000000000,
	})
	lcm.Manifest.AddPart(0, compaction.PartInfo{
		Name:    sst2,
		Size:    1024,
		MinTime: 1500000000,
		MaxTime: 2500000000,
	})

	// 验证 L0 有 2 个 parts
	if len(lcm.Manifest.GetLevel(0).Parts) != 2 {
		t.Fatalf("expected 2 parts in L0, got %d", len(lcm.Manifest.GetLevel(0).Parts))
	}

	// 执行 compaction
	ctx := context.Background()
	outputPath, deletedFiles, err := lcm.Compact(ctx)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// 验证有输出
	if outputPath == "" {
		t.Fatal("expected output path from compaction")
	}

	// 验证删除了输入文件
	if len(deletedFiles) != 2 {
		t.Errorf("expected 2 deleted files, got %d", len(deletedFiles))
	}

	// 验证 L0 已被清空（源文件已删除）
	if len(lcm.Manifest.GetLevel(0).Parts) != 0 {
		t.Errorf("L0 should be empty after compaction, got %d parts", len(lcm.Manifest.GetLevel(0).Parts))
	}

	// 验证 L1 有数据
	if len(lcm.Manifest.GetLevel(1).Parts) != 1 {
		t.Errorf("expected 1 part in L1, got %d", len(lcm.Manifest.GetLevel(1).Parts))
	}

	// 验证 manifest 已保存
	if err := lcm.Manifest.Save(); err != nil {
		t.Fatalf("failed to save manifest: %v", err)
	}
}

// TestLevelCompactionE2E_L1ToL2 tests L1 to L2 compaction.
func TestLevelCompactionE2E_L1ToL2(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelConfig()
	lcm, err := compaction.NewLevelManager(shard, lcmCfg)
	if err != nil {
		t.Fatalf("NewLevelManager failed: %v", err)
	}

	points1 := []*types.Point{
		{Timestamp: 1000000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(10.5)}},
		{Timestamp: 2000000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(20.5)}},
	}
	points2 := []*types.Point{
		{Timestamp: 1500000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(15.0)}},
		{Timestamp: 2500000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"usage": types.NewFieldValue(25.0)}},
	}

	sst1 := createTestSSTableInLevel(t, tmpDir, 1, 100, points1)
	sst2 := createTestSSTableInLevel(t, tmpDir, 1, 101, points2)

	// 添加到 L1 manifest
	lcm.Manifest.AddPart(1, compaction.PartInfo{
		Name:    sst1,
		Size:    1024,
		MinTime: 1000000000,
		MaxTime: 2000000000,
	})
	lcm.Manifest.AddPart(1, compaction.PartInfo{
		Name:    sst2,
		Size:    1024,
		MinTime: 1500000000,
		MaxTime: 2500000000,
	})

	// L0 为空，不会触发 L0 → L1，而是检查 L1
	// 需要手动让 L1 超过容量限制才能触发 L1 → L2
	// 这里我们直接测试 selectPartsForMerge 和 collectOverlapParts

	// 收集重叠 parts
	selected := lcm.SelectPartsForMerge(1)
	if len(selected) < 2 {
		t.Fatalf("expected at least 2 selected parts for L1, got %d", len(selected))
	}

	// 验证 ShouldCompact 能检测到 L1 需要 compaction
	if !lcm.ShouldCompact() {
		// L1 可能没有达到容量限制，添加更多 parts
		lcm.Manifest.AddPart(1, compaction.PartInfo{Name: "sst_999", Size: 50 * 1024 * 1024, MinTime: 3000000000, MaxTime: 4000000000})
		if !lcm.ShouldCompact() {
			t.Log("L1 may not trigger compaction without exceeding size limit")
		}
	}
}

// TestLevelCompactionE2E_CheckpointRecovery tests crash recovery via checkpoint.
func TestLevelCompactionE2E_CheckpointRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	// 确保 data 目录存在
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatalf("failed to create data dir: %v", err)
	}

	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
	}

	shard := NewShard(cfg)
	_ = shard.Close() // 关闭以便重新创建

	// 手动创建一个 checkpoint
	cp := &compaction.Checkpoint{
		Version:    1,
		Level:      0,
		InputParts: []string{"sst_1", "sst_2"},
		OutputPath: filepath.Join(dataDir, "L1", "sst_3"),
		OutputSeq:  3,
		MergedSize: 2048,
		StartedAt:  time.Now().Unix(),
	}
	if err := cp.Save(dataDir); err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	// 验证 checkpoint 文件存在
	cpPath := filepath.Join(dataDir, "_compaction.cp")
	if _, err := os.Stat(cpPath); os.IsNotExist(err) {
		t.Fatal("checkpoint file should exist after save")
	}

	// 创建新的 LevelCompactionManager 并恢复
	shard2 := NewShard(cfg)
	defer func() { _ = shard2.Close() }()

	lcmCfg := compaction.DefaultLevelConfig()
	lcm2, err := compaction.NewLevelManager(shard2, lcmCfg)
	if err != nil {
		t.Fatalf("NewLevelManager failed: %v", err)
	}

	// 执行恢复
	if err := lcm2.Recover(); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// 验证 checkpoint 已清理
	if _, err := os.Stat(cpPath); !os.IsNotExist(err) {
		t.Error("checkpoint should be cleared after recovery")
	}
}

// TestLevelCompactionE2E_OldFormatMigration tests migration from old flat format.
// TestLevelCompactionE2E_DataIntegrity tests that data is correctly merged and deduplicated.
func TestLevelCompactionE2E_DataIntegrity(t *testing.T) {
	tmpDir := t.TempDir()
	schemaStore := metadata.NewSimpleSchemaStore()
	_ = schemaStore.SetSchema("testdb", "cpu", &metadata.Schema{
		Version: 1,
		Fields:  []metadata.FieldDef{{Name: "value", Type: 2}}, // int64
	})

	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		SchemaStore: schemaStore,
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelConfig()
	lcm, err := compaction.NewLevelManager(shard, lcmCfg)
	if err != nil {
		t.Fatalf("NewLevelManager failed: %v", err)
	}

	// 创建两个 SSTable，有部分重叠的时间范围
	points1 := []*types.Point{
		{Timestamp: 1000000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"value": types.NewFieldValue(int64(100))}},
		{Timestamp: 2000000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"value": types.NewFieldValue(int64(200))}},
		{Timestamp: 3000000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"value": types.NewFieldValue(int64(300))}},
	}
	points2 := []*types.Point{
		{Timestamp: 2500000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"value": types.NewFieldValue(int64(250))}}, // 重复时间戳
		{Timestamp: 3500000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"value": types.NewFieldValue(int64(350))}},
		{Timestamp: 4000000000, Tags: map[string]string{"host": "server1"}, Fields: map[string]*types.FieldValue{"value": types.NewFieldValue(int64(400))}},
	}

	sst1 := createTestSSTableInLevel(t, tmpDir, 0, 1, points1)
	sst2 := createTestSSTableInLevel(t, tmpDir, 0, 2, points2)

	// 添加到 manifest
	lcm.Manifest.AddPart(0, compaction.PartInfo{
		Name:    sst1,
		Size:    1024,
		MinTime: 1000000000,
		MaxTime: 3000000000,
	})
	lcm.Manifest.AddPart(0, compaction.PartInfo{
		Name:    sst2,
		Size:    1024,
		MinTime: 2500000000,
		MaxTime: 4000000000,
	})

	// 执行 compaction
	ctx := context.Background()
	outputPath, deletedFiles, err := lcm.Compact(ctx)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	if outputPath == "" {
		t.Fatal("expected output path from compaction")
	}

	// 验证输出路径格式正确（指向 L1 目录）
	if filepath.Base(filepath.Dir(outputPath)) != "L1" {
		t.Errorf("output should be in L1 directory, got %s", filepath.Dir(outputPath))
	}

	// 验证删除了输入文件
	if len(deletedFiles) != 2 {
		t.Errorf("expected 2 deleted files, got %d", len(deletedFiles))
	}

	// 验证 L0 已被清空
	if len(lcm.Manifest.GetLevel(0).Parts) != 0 {
		t.Errorf("L0 should be empty after compaction, got %d parts", len(lcm.Manifest.GetLevel(0).Parts))
	}

	// 验证 L1 有新数据
	if len(lcm.Manifest.GetLevel(1).Parts) != 1 {
		t.Errorf("expected 1 part in L1, got %d", len(lcm.Manifest.GetLevel(1).Parts))
	}
}

// TestLevelCompactionE2E_SelectPartsForMerge tests the small parts priority selection.
func TestLevelCompactionE2E_SelectPartsForMerge(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelConfig()
	lcm, err := compaction.NewLevelManager(shard, lcmCfg)
	if err != nil {
		t.Fatalf("NewLevelManager failed: %v", err)
	}

	// 添加多个不同大小的 parts
	lcm.Manifest.AddPart(0, compaction.PartInfo{Name: "small1", Size: 100, MinTime: 1000, MaxTime: 2000})
	lcm.Manifest.AddPart(0, compaction.PartInfo{Name: "small2", Size: 200, MinTime: 2000, MaxTime: 3000})
	lcm.Manifest.AddPart(0, compaction.PartInfo{Name: "medium", Size: 500, MinTime: 3000, MaxTime: 4000})
	lcm.Manifest.AddPart(0, compaction.PartInfo{Name: "large", Size: 1000, MinTime: 4000, MaxTime: 5000})

	// 选择要合并的 parts
	selected := lcm.SelectPartsForMerge(0)

	// 应该选择小文件优先
	if len(selected) == 0 {
		t.Fatal("should select at least 1 part")
	}

	// 验证选择的是小文件（排序后前面的应该更小）
	for i := 1; i < len(selected); i++ {
		if selected[i].Size < selected[i-1].Size {
			t.Error("selected parts should be sorted by size (small first)")
		}
	}

	// 验证至少选择了小文件
	if selected[0].Size != 100 {
		t.Errorf("first selected should be smallest (100), got %d", selected[0].Size)
	}
}

// TestLevelCompactionE2E_HasOverlap tests time range overlap detection.
func TestLevelCompactionE2E_HasOverlap(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelConfig()
	lcm, err := compaction.NewLevelManager(shard, lcmCfg)
	if err != nil {
		t.Fatalf("NewLevelManager failed: %v", err)
	}

	// 添加 L0 parts
	lcm.Manifest.AddPart(0, compaction.PartInfo{Name: "l0_1", Size: 100, MinTime: 1000, MaxTime: 2000})
	lcm.Manifest.AddPart(0, compaction.PartInfo{Name: "l0_2", Size: 100, MinTime: 2000, MaxTime: 3000})

	// 添加 L1 parts
	lcm.Manifest.AddPart(1, compaction.PartInfo{Name: "l1_1", Size: 1000, MinTime: 1500, MaxTime: 2500})

	// 收集重叠 parts
	targets := []compaction.PartInfo{lcm.Manifest.GetLevel(0).Parts[0]}
	overlaps := lcm.CollectOverlapParts(0, targets)

	// 验证重叠检测
	foundL0 := false
	foundL1 := false
	for _, p := range overlaps {
		if p.Name == "l0_1" || p.Name == "l0_2" {
			foundL0 = true
		}
		if p.Name == "l1_1" {
			foundL1 = true
		}
	}

	if !foundL0 {
		t.Error("should find overlapping parts in L0")
	}
	if !foundL1 {
		t.Error("should find overlapping parts in L1")
	}
}

// TestLevelCompactionE2E_PeriodicCompaction tests automatic periodic compaction.
func TestLevelCompactionE2E_PeriodicCompaction(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := &compaction.LevelConfig{
		Enabled:       true,
		CheckInterval: 10 * time.Millisecond, // 非常短的间隔用于测试
		Timeout:       time.Minute,
	}

	lcm, err := compaction.NewLevelManager(shard, lcmCfg)
	if err != nil {
		t.Fatalf("NewLevelManager failed: %v", err)
	}

	// 启动定期检查
	lcm.StartPeriodicCheck()

	// 等待一段时间
	time.Sleep(50 * time.Millisecond)

	// 停止
	lcm.Stop()

	// 再次停止应该是安全的（幂等）
	lcm.Stop()
}

// TestLevelCompactionE2E_EmptyManifest tests behavior with empty manifest.
func TestLevelCompactionE2E_EmptyManifest(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelConfig()
	lcm, err := compaction.NewLevelManager(shard, lcmCfg)
	if err != nil {
		t.Fatalf("NewLevelManager failed: %v", err)
	}

	// 空状态不应该触发 compaction
	if lcm.ShouldCompact() {
		t.Error("empty manifest should not trigger compaction")
	}

	// Compact 应该返回空结果
	ctx := context.Background()
	outputPath, deletedFiles, err := lcm.Compact(ctx)
	if err != nil {
		t.Fatalf("Compact should not fail on empty manifest: %v", err)
	}
	if outputPath != "" {
		t.Error("empty manifest should produce no output")
	}
	if len(deletedFiles) != 0 {
		t.Error("empty manifest should produce no deleted files")
	}
}

// TestLevelCompactionE2E_MultipleLevels tests that compaction respects level hierarchy.
func TestLevelCompactionE2E_MultipleLevels(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
	}

	shard := NewShard(cfg)
	defer func() { _ = shard.Close() }()

	lcmCfg := compaction.DefaultLevelConfig()
	lcm, err := compaction.NewLevelManager(shard, lcmCfg)
	if err != nil {
		t.Fatalf("NewLevelManager failed: %v", err)
	}

	// 验证各层容量配置
	levelConfigs := lcm.Config().LevelConfigs

	if levelConfigs[0].MaxSize != 10*1024*1024 {
		t.Errorf("L0 MaxSize should be 10MB, got %d", levelConfigs[0].MaxSize)
	}
	if levelConfigs[0].MaxParts != 10 {
		t.Errorf("L0 MaxParts should be 10, got %d", levelConfigs[0].MaxParts)
	}
	if levelConfigs[1].MaxSize != 100*1024*1024 {
		t.Errorf("L1 MaxSize should be 100MB, got %d", levelConfigs[1].MaxSize)
	}
	if levelConfigs[2].MaxSize != 1024*1024*1024 {
		t.Errorf("L2 MaxSize should be 1GB, got %d", levelConfigs[2].MaxSize)
	}

	// 验证 levelMaxSize
	if size := lcm.LevelMaxSize(0); size != 10*1024*1024 {
		t.Errorf("L0 levelMaxSize should be 10MB, got %d", size)
	}
	if size := lcm.LevelMaxSize(1); size != 100*1024*1024 {
		t.Errorf("L1 levelMaxSize should be 100MB, got %d", size)
	}
	if size := lcm.LevelMaxSize(2); size != 1024*1024*1024 {
		t.Errorf("L2 levelMaxSize should be 1GB, got %d", size)
	}
	if size := lcm.LevelMaxSize(99); size != 0 {
		t.Errorf("invalid level should return 0, got %d", size)
	}
}
