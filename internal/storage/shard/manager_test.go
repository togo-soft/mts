// internal/storage/shard/manager_test.go
package shard

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage/metadata"
	"codeberg.org/micro-ts/mts/types"
)

func TestShardManager_GetShard(t *testing.T) {
	m := NewShardManager(t.TempDir(), time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, t.TempDir()))

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
	m := NewShardManager(t.TempDir(), time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, t.TempDir()))

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

func TestShardManager_PersistAll(t *testing.T) {
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

	// 创建一个 shard，这会自动创建 MetaStore
	_, err := m.GetShard("db1", "cpu", time.Now().UnixNano())
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	// 调用 PersistAll - 应该成功执行
	if err := m.PersistAll(); err != nil {
		t.Errorf("PersistAll failed: %v", err)
	}
}

func TestShardManager_GetAllShards(t *testing.T) {
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

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
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

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
	m := NewShardManager(t.TempDir(), time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, t.TempDir()))

	// 删除不存在的 shard 应该成功
	err := m.DeleteShard("nonexistent/key")
	if err != nil {
		t.Errorf("DeleteShard nonexistent should succeed, got: %v", err)
	}
}

func TestShardManager_DeleteShard_WithData(t *testing.T) {
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

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
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

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
	m := NewShardManager(t.TempDir(), time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, t.TempDir()))
	retention := NewRetentionService(m, time.Hour, time.Minute)
	if retention == nil {
		t.Error("NewRetentionService should not return nil")
	}
}

func TestRetentionService_StartStop(t *testing.T) {
	m := NewShardManager(t.TempDir(), time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, t.TempDir()))
	retention := NewRetentionService(m, time.Hour, time.Minute)

	// Start
	retention.Start()

	// Stop - 多次调用安全
	retention.Stop()
	retention.Stop()
}

func TestRetentionService_Cleanup(t *testing.T) {
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, 500*time.Millisecond, DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

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
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

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
		SeriesStore: metadata.NewSimpleSeriesStore(),
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

func TestSimpleSeriesStore_BasicAllocate(t *testing.T) {
	m := metadata.NewSimpleSeriesStore()
	tags := map[string]string{"host": "server1", "region": "us"}

	sid, err := m.AllocateSID(tags)
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid != 0 {
		t.Errorf("expected SID 0, got %d", sid)
	}

	// 再次分配相同 tags 应返回相同 SID
	sid2, err := m.AllocateSID(tags)
	if err != nil {
		t.Fatal("second AllocateSID failed:", err)
	}
	if sid2 != sid {
		t.Errorf("expected same SID %d, got %d", sid, sid2)
	}
}

func TestSimpleSeriesStore_GetTags(t *testing.T) {
	m := metadata.NewSimpleSeriesStore()
	tags := map[string]string{"host": "server1", "region": "us"}

	sid, _ := m.AllocateSID(tags)
	got, ok := m.GetTagsBySID(sid)
	if !ok {
		t.Fatal("GetTagsBySID returned false")
	}
	if got["host"] != "server1" {
		t.Errorf("expected host=server1, got %s", got["host"])
	}
	if got["region"] != "us" {
		t.Errorf("expected region=us, got %s", got["region"])
	}
}

func TestSimpleSeriesStore_GetTagsNotFound(t *testing.T) {
	m := metadata.NewSimpleSeriesStore()

	_, ok := m.GetTagsBySID(999)
	if ok {
		t.Error("expected false for nonexistent SID")
	}
}

func TestShardManager_GetShard_DiscoverExisting(t *testing.T) {
	// 测试发现已存在的 shard 目录
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

	// 手动创建一个 shard 目录结构
	shardDir := filepath.Join(tmpDir, "db1", "cpu", "0_3600000000000")
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		t.Fatalf("failed to create shard dir: %v", err)
	}

	// GetShard 应该能发现已存在的 shard
	s, err := m.GetShard("db1", "cpu", 1000000000) // 1000ms 在 [0, 3600s) 范围内
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	if s.StartTime() != 0 {
		t.Errorf("expected start time 0, got %d", s.StartTime())
	}
	if s.EndTime() != 3600000000000 {
		t.Errorf("expected end time 3600000000000, got %d", s.EndTime())
	}
}

func TestShardManager_GetShard_DiscoverMultipleExisting(t *testing.T) {
	// 测试发现多个已存在的 shard
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

	// 手动创建多个 shard 目录结构
	baseTime := int64(3600000000000) // 1 hour in ns
	for i := 0; i < 3; i++ {
		start := baseTime * int64(i)
		end := start + baseTime
		shardDir := filepath.Join(tmpDir, "db1", "cpu", fmt.Sprintf("%d_%d", start, end))
		if err := os.MkdirAll(shardDir, 0755); err != nil {
			t.Fatalf("failed to create shard dir: %v", err)
		}
	}

	// GetShard 应该发现 3 个 shard
	s1, _ := m.GetShard("db1", "cpu", 1000000000)
	s2, _ := m.GetShard("db1", "cpu", baseTime+1000000000)
	s3, _ := m.GetShard("db1", "cpu", baseTime*2+1000000000)

	if s1.StartTime() != 0 || s2.StartTime() != baseTime || s3.StartTime() != baseTime*2 {
		t.Errorf("shard start times incorrect")
	}
}

func TestShardManager_GetShard_InvalidDirectory(t *testing.T) {
	// 测试处理无效目录名
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

	// 手动创建包含无效目录名的目录结构
	measDir := filepath.Join(tmpDir, "db1", "cpu")
	if err := os.MkdirAll(measDir, 0755); err != nil {
		t.Fatalf("failed to create measurement dir: %v", err)
	}

	// 创建无效的 shard 目录
	invalidDir := filepath.Join(measDir, "invalid_name")
	if err := os.MkdirAll(invalidDir, 0755); err != nil {
		t.Fatalf("failed to create invalid dir: %v", err)
	}

	// 应该能正常处理（跳过无效目录）
	_, err := m.GetShard("db1", "cpu", 1000000000)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
}

func TestShardManager_DeleteShard_CleanupMetaStore(t *testing.T) {
	// 测试删除 shard 后 MetaStore 是否被清理
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

	// 创建一个 shard
	base := time.Now().UnixNano()
	s, _ := m.GetShard("db1", "cpu", base)

	// 写入数据
	p := &types.Point{
		Database:    "db1",
		Measurement: "cpu",
		Tags:        map[string]string{"host": "server1"},
		Timestamp:   s.StartTime() + 1000,
		Fields:      map[string]*types.FieldValue{"value": types.NewFieldValue(int64(1))},
	}
	_ = s.Write(p)
	_ = s.Flush()

	// 获取所有 shards
	shards := m.GetAllShards()
	if len(shards) != 1 {
		t.Errorf("expected 1 shard, got %d", len(shards))
	}

	// 删除 shard
	key := "db1/cpu/" + formatInt64(s.StartTime())
	err := m.DeleteShard(key)
	if err != nil {
		t.Fatalf("DeleteShard failed: %v", err)
	}

	// 获取所有 shards 应该为空
	shards = m.GetAllShards()
	if len(shards) != 0 {
		t.Errorf("expected 0 shards after delete, got %d", len(shards))
	}
}

func TestShardManager_GetShard_SameMeasurementDifferentDB(t *testing.T) {
	// 测试同一 measurement 不同 database
	tmpDir := t.TempDir()
	m := NewShardManager(tmpDir, time.Hour, DefaultMemTableConfig(), nil, newTestMgr(t, tmpDir))

	s1, _ := m.GetShard("db1", "cpu", time.Now().UnixNano())
	s2, _ := m.GetShard("db2", "cpu", time.Now().UnixNano())

	if s1 == s2 {
		t.Error("different DBs should have different shards")
	}

	if s1.Measurement() != "cpu" || s2.Measurement() != "cpu" {
		t.Error("measurement should be cpu for both")
	}
}

func TestShardManager_flushLocked_NotCalled(t *testing.T) {
	// flushLocked 不是公共方法，但可以通过错误场景测试
	// 由于 flushLocked 是私有的，这里测试 Shard 在异常情况下的行为
	tmpDir := t.TempDir()

	s := NewShard(ShardConfig{
		DB:          "db1",
		Measurement: "cpu",
		StartTime:   0,
		EndTime:     time.Hour.Nanoseconds(),
		Dir:         tmpDir,
		SeriesStore: metadata.NewSimpleSeriesStore(),
		MemTableCfg: DefaultMemTableConfig(),
	})

	// 正常关闭
	_ = s.Close()
}

func newTestMgr(t *testing.T, dir string) *metadata.Manager {
	t.Helper()
	mgr, err := metadata.NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	return mgr
}
