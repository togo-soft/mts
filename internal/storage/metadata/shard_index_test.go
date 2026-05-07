package metadata

import (
	"testing"
)

func TestShardIndex_RegisterShard(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	idx := m.Shards()

	info := ShardInfo{
		ID:        "0_86400",
		StartTime: 0,
		EndTime:   86400000000000,
		DataDir:   "db/cpu/shards/0_86400",
	}
	if err := idx.RegisterShard("db", "cpu", info); err != nil {
		t.Fatal("RegisterShard failed:", err)
	}
}

func TestShardIndex_RegisterShard_Duplicate(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	idx := m.Shards()

	info := ShardInfo{ID: "0_86400", StartTime: 0, EndTime: 86400000000000}
	_ = idx.RegisterShard("db", "cpu", info)
	if err := idx.RegisterShard("db", "cpu", info); err == nil {
		t.Error("expected error for duplicate shard ID")
	}
}

func TestShardIndex_UnregisterShard(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	idx := m.Shards()

	info := ShardInfo{ID: "0_86400", StartTime: 0, EndTime: 86400000000000}
	_ = idx.RegisterShard("db", "cpu", info)

	if err := idx.UnregisterShard("db", "cpu", "0_86400"); err != nil {
		t.Fatal("UnregisterShard failed:", err)
	}

	shards := idx.ListShards("db", "cpu")
	if len(shards) != 0 {
		t.Errorf("expected empty list after unregister, got %d shards", len(shards))
	}
}

func TestShardIndex_UnregisterShard_NotFound(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	idx := m.Shards()

	if err := idx.UnregisterShard("db", "cpu", "nonexistent"); err == nil {
		t.Error("expected error for nonexistent shard")
	}
}

func TestShardIndex_QueryShards_Exact(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	idx := m.Shards()

	info := ShardInfo{ID: "0_100", StartTime: 0, EndTime: 100}
	_ = idx.RegisterShard("db", "cpu", info)

	results := idx.QueryShards("db", "cpu", 0, 100)
	if len(results) != 1 {
		t.Fatalf("expected 1 shard, got %d", len(results))
	}
	if results[0].ID != "0_100" {
		t.Errorf("expected ID 0_100, got %s", results[0].ID)
	}
}

func TestShardIndex_QueryShards_PartialOverlap(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	idx := m.Shards()

	info := ShardInfo{ID: "0_100", StartTime: 0, EndTime: 100}
	_ = idx.RegisterShard("db", "cpu", info)

	results := idx.QueryShards("db", "cpu", 50, 150)
	if len(results) != 1 {
		t.Errorf("expected 1 shard for partial overlap, got %d", len(results))
	}
}

func TestShardIndex_QueryShards_NoMatch(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	idx := m.Shards()

	info := ShardInfo{ID: "0_100", StartTime: 0, EndTime: 100}
	_ = idx.RegisterShard("db", "cpu", info)

	results := idx.QueryShards("db", "cpu", 200, 300)
	if len(results) != 0 {
		t.Errorf("expected empty list, got %d shards", len(results))
	}
}

func TestShardIndex_QueryShards_AdjacentBoundary(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	idx := m.Shards()

	info := ShardInfo{ID: "100_200", StartTime: 100, EndTime: 200}
	_ = idx.RegisterShard("db", "cpu", info)

	// [0, 100) 与 [100, 200) 不重叠
	results := idx.QueryShards("db", "cpu", 0, 100)
	if len(results) != 0 {
		t.Errorf("expected empty for adjacent non-overlapping, got %d", len(results))
	}

	// [100, 100) 空区间
	results = idx.QueryShards("db", "cpu", 100, 100)
	if len(results) != 0 {
		t.Errorf("expected empty for zero-length query, got %d", len(results))
	}
}

func TestShardIndex_QueryShards_Multiple(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	idx := m.Shards()

	_ = idx.RegisterShard("db", "cpu", ShardInfo{ID: "0_100", StartTime: 0, EndTime: 100})
	_ = idx.RegisterShard("db", "cpu", ShardInfo{ID: "100_200", StartTime: 100, EndTime: 200})
	_ = idx.RegisterShard("db", "cpu", ShardInfo{ID: "200_300", StartTime: 200, EndTime: 300})

	results := idx.QueryShards("db", "cpu", 50, 150)
	if len(results) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(results))
	}
	if results[0].ID != "0_100" || results[1].ID != "100_200" {
		t.Errorf("expected [0_100, 100_200], got %v", results)
	}
}

func TestShardIndex_ListShards(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	idx := m.Shards()

	// 乱序注册
	_ = idx.RegisterShard("db", "cpu", ShardInfo{ID: "200_300", StartTime: 200, EndTime: 300})
	_ = idx.RegisterShard("db", "cpu", ShardInfo{ID: "0_100", StartTime: 0, EndTime: 100})
	_ = idx.RegisterShard("db", "cpu", ShardInfo{ID: "100_200", StartTime: 100, EndTime: 200})

	shards := idx.ListShards("db", "cpu")
	if len(shards) != 3 {
		t.Fatalf("expected 3 shards, got %d", len(shards))
	}
	// 按 startTime 排序
	if shards[0].StartTime != 0 || shards[1].StartTime != 100 || shards[2].StartTime != 200 {
		t.Errorf("shards not sorted by startTime: %v", shards)
	}
}

func TestShardIndex_ListShards_Empty(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	idx := m.Shards()

	shards := idx.ListShards("db", "cpu")
	if len(shards) != 0 {
		t.Errorf("expected empty list, got %d shards", len(shards))
	}
}

func TestShardIndex_UpdateShardStats(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	idx := m.Shards()

	info := ShardInfo{ID: "0_100", StartTime: 0, EndTime: 100, SSTableCount: 0, TotalSize: 0}
	_ = idx.RegisterShard("db", "cpu", info)

	if err := idx.UpdateShardStats("db", "cpu", "0_100", 5, 1024); err != nil {
		t.Fatal("UpdateShardStats failed:", err)
	}

	shards := idx.ListShards("db", "cpu")
	if shards[0].SSTableCount != 5 {
		t.Errorf("expected SSTableCount 5, got %d", shards[0].SSTableCount)
	}
	if shards[0].TotalSize != 1024 {
		t.Errorf("expected TotalSize 1024, got %d", shards[0].TotalSize)
	}
}

func TestShardIndex_UpdateShardStats_NotFound(t *testing.T) {
	m, _ := NewManager(t.TempDir())
	defer func() { _ = m.Close() }()
	idx := m.Shards()

	if err := idx.UpdateShardStats("db", "cpu", "nonexistent", 1, 100); err == nil {
		t.Error("expected error for nonexistent shard")
	}
}
