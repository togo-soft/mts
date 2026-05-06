// internal/storage/shard/manager_test.go
package shard

import (
	"os"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/measurement"
	"codeberg.org/micro-ts/mts/types"
)

func TestShardManager_GetShard(t *testing.T) {
	m := NewShardManager(t.TempDir(), time.Hour, DefaultMemTableConfig(), nil)

	start := time.Now().UnixNano()

	s, err := m.GetShard("db1", "cpu", start)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	if !s.ContainsTime(start) {
		t.Errorf("shard should contain time %d", start)
	}
}

func TestShardManager_GetShard_TimeWindow(t *testing.T) {
	m := NewShardManager(t.TempDir(), time.Hour, DefaultMemTableConfig(), nil)

	// 1小时时间窗口
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()

	// 30分钟应该落在第一个shard
	s1, _ := m.GetShard("db1", "cpu", base)
	s2, _ := m.GetShard("db1", "cpu", base+30*60*1e9)

	if s1 != s2 {
		t.Errorf("30分钟应该在同一个shard")
	}

	// 90分钟应该落在下一个shard
	s3, _ := m.GetShard("db1", "cpu", base+90*60*1e9)
	if s1 == s3 {
		t.Errorf("90分钟应该在不同shard")
	}
}

func TestShardManager_PersistAllMetaStores(t *testing.T) {
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil)

	// 创建一个 shard，这会自动创建 MetaStore
	_, err := m.GetShard("db1", "cpu", time.Now().UnixNano())
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	// 调用 PersistAllMetaStores - 应该成功执行
	m.PersistAllMetaStores()
}

func TestShardManager_GetAllShards(t *testing.T) {
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil)

	// 初始应该为空
	shards := m.GetAllShards()
	if len(shards) != 0 {
		t.Errorf("expected 0 shards, got %d", len(shards))
	}

	// 创建一个 shard
	_, _ = m.GetShard("db1", "cpu", time.Now().UnixNano())

	// 应该有1个 shard
	shards = m.GetAllShards()
	if len(shards) != 1 {
		t.Errorf("expected 1 shard, got %d", len(shards))
	}
}

func TestShardManager_DeleteShard(t *testing.T) {
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil)

	// 创建一个 shard
	base := time.Now().UnixNano()
	s, _ := m.GetShard("db1", "cpu", base)

	// 验证 shard 已创建
	shards := m.GetAllShards()
	if len(shards) != 1 {
		t.Fatalf("expected 1 shard, got %d", len(shards))
	}

	// 删除 shard
	key := "db1/cpu/" + formatInt64(s.StartTime())
	err := m.DeleteShard(key)
	if err != nil {
		t.Fatalf("DeleteShard failed: %v", err)
	}

	// 应该为空
	shards = m.GetAllShards()
	if len(shards) != 0 {
		t.Errorf("expected 0 shards after delete, got %d", len(shards))
	}
}

func TestShardManager_DeleteShard_NotFound(t *testing.T) {
	m := NewShardManager(t.TempDir(), time.Hour, DefaultMemTableConfig(), nil)

	// 删除不存在的 shard 应该成功
	err := m.DeleteShard("nonexistent/key")
	if err != nil {
		t.Errorf("DeleteShard nonexistent should succeed, got: %v", err)
	}
}

func TestShardManager_DeleteShard_WithData(t *testing.T) {
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil)

	// 创建一个 shard 并写入数据
	base := time.Now().UnixNano()
	s, _ := m.GetShard("db1", "cpu", base)

	// 写入数据触发数据目录创建
	ts := s.StartTime() + 1000
	p := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   ts,
		Fields: map[string]*types.FieldValue{
			"usage": types.NewFieldValue(float64(50.0)),
		},
	}
	_ = s.Write(p)

	// Flush 确保数据写入磁盘
	_ = s.Flush()

	// 验证数据目录存在（s.Dir() 包含完整路径）
	dataDir := s.Dir()
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		t.Fatalf("data dir should exist: %s", dataDir)
	}

	// 删除 shard
	key := "db1/cpu/" + formatInt64(s.StartTime())
	err := m.DeleteShard(key)
	if err != nil {
		t.Fatalf("DeleteShard failed: %v", err)
	}

	// 验证数据目录已删除
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Errorf("data dir should be deleted: %s", dataDir)
	}
}

func TestShardManager_GetAllShards_MultipleShards(t *testing.T) {
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil)

	// 创建多个 shard
	_, _ = m.GetShard("db1", "cpu", time.Now().UnixNano())
	_, _ = m.GetShard("db1", "mem", time.Now().UnixNano())
	_, _ = m.GetShard("db2", "cpu", time.Now().UnixNano())

	// 应该有3个 shard
	shards := m.GetAllShards()
	if len(shards) != 3 {
		t.Errorf("expected 3 shards, got %d", len(shards))
	}
}

func TestRetentionService_NewRetentionService(t *testing.T) {
	m := NewShardManager(t.TempDir(), time.Hour, DefaultMemTableConfig(), nil)
	retention := NewRetentionService(m, time.Hour, time.Minute)
	if retention == nil {
		t.Error("NewRetentionService should not return nil")
	}
}

func TestRetentionService_StartStop(t *testing.T) {
	m := NewShardManager(t.TempDir(), time.Hour, DefaultMemTableConfig(), nil)
	retention := NewRetentionService(m, time.Hour, time.Minute)

	// Start
	retention.Start()

	// Stop - 多次调用安全
	retention.Stop()
	retention.Stop()
}

func TestRetentionService_Cleanup(t *testing.T) {
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, 500*time.Millisecond, DefaultMemTableConfig(), nil)

	// 创建 shard
	oldTime := time.Now().Add(-2 * time.Second).UnixNano()
	_, _ = m.GetShard("db1", "cpu", oldTime)

	retention := NewRetentionService(m, time.Second, time.Hour)

	// 执行清理
	retention.cleanup()

	// shard 应该被删除
	shards := m.GetAllShards()
	if len(shards) != 0 {
		t.Errorf("expected 0 shards after retention cleanup, got %d", len(shards))
	}
}

func TestRetentionService_Cleanup_NoExpiredShards(t *testing.T) {
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil)

	// 创建 shard
	_, _ = m.GetShard("db1", "cpu", time.Now().UnixNano())

	retention := NewRetentionService(m, time.Hour, time.Hour)

	// 执行清理 - 不应该有删除
	retention.cleanup()

	// shard 应该保留
	shards := m.GetAllShards()
	if len(shards) != 1 {
		t.Errorf("expected 1 shard, got %d", len(shards))
	}
}

func TestShard_DB_Measurement_Dir(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := ShardConfig{
		DB:          "testdb",
		Measurement: "testmeas",
		StartTime:   1000,
		EndTime:     2000,
		Dir:         tmpDir,
		MetaStore:   measurement.NewMeasurementMetaStore(),
		MemTableCfg: DefaultMemTableConfig(),
	}

	s := NewShard(cfg)

	if s.DB() != "testdb" {
		t.Errorf("expected DB=testdb, got %s", s.DB())
	}
	if s.Measurement() != "testmeas" {
		t.Errorf("expected Measurement=testmeas, got %s", s.Measurement())
	}
	if s.Dir() != tmpDir {
		t.Errorf("expected Dir=%s, got %s", tmpDir, s.Dir())
	}
}

func TestMeasurementMetaStore_SetPersistPath(t *testing.T) {
	m := measurement.NewMeasurementMetaStore()
	path := "/tmp/test/meta.json"
	m.SetPersistPath(path)

	// 调用 Persist 不应该出错（路径不存在会创建）
	err := m.Persist()
	if err != nil {
		t.Errorf("Persist failed: %v", err)
	}
}

func TestMeasurementMetaStore_Persist_NoPath(t *testing.T) {
	m := measurement.NewMeasurementMetaStore()
	// persistPath 为空，Persist 应该直接返回
	err := m.Persist()
	if err != nil {
		t.Errorf("Persist with no path should succeed, got: %v", err)
	}
}

func TestDatabaseMetaStore_MeasurementExists(t *testing.T) {
	dbMeta := measurement.NewDatabaseMetaStore()

	// 初始不存在
	if dbMeta.MeasurementExists("cpu") {
		t.Error("cpu should not exist initially")
	}

	// 创建
	dbMeta.GetOrCreate("cpu")

	// 现在存在
	if !dbMeta.MeasurementExists("cpu") {
		t.Error("cpu should exist after GetOrCreate")
	}
}
