// internal/storage/shard/manager_test.go
package shard

import (
	"testing"
	"time"
)

func TestShardManager_GetShard(t *testing.T) {
	m := NewShardManager(t.TempDir(), time.Hour, DefaultMemTableConfig())

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
	m := NewShardManager(t.TempDir(), time.Hour, DefaultMemTableConfig())

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
