package metadata

import (
	"testing"
)

func TestShardIndex_RegisterShard(t *testing.T) {
	m, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}
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

func TestShardIndex_ListShards_Empty(t *testing.T) {
	m, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}
	idx := m.Shards()

	shards := idx.ListShards("db", "cpu")
	if len(shards) != 0 {
		t.Errorf("expected empty list, got %d shards", len(shards))
	}
}
