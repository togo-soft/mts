package metadata

import (
	"os"
	"testing"
)

func TestShardIndex_RegisterShard_Success(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	info := ShardInfo{
		ID:        "0_3600",
		StartTime: 0,
		EndTime:   3600000000000,
		DataDir:   "testdb/cpu/0_3600",
	}
	if err := idx.RegisterShard("testdb", "cpu", info); err != nil {
		t.Fatal("RegisterShard failed:", err)
	}
}

func TestShardIndex_RegisterShard_Duplicate(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	info := ShardInfo{ID: "0_3600", StartTime: 0, EndTime: 3600000000000}
	_ = idx.RegisterShard("testdb", "cpu", info)
	err := idx.RegisterShard("testdb", "cpu", info)
	if err == nil {
		t.Error("expected error for duplicate shard")
	}
}

func TestShardIndex_UnregisterShard_Success(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	info := ShardInfo{ID: "0_3600", StartTime: 0, EndTime: 3600000000000}
	_ = idx.RegisterShard("testdb", "cpu", info)

	if err := idx.UnregisterShard("testdb", "cpu", "0_3600"); err != nil {
		t.Fatal("UnregisterShard failed:", err)
	}

	shards := idx.ListShards("testdb", "cpu")
	if len(shards) != 0 {
		t.Errorf("expected 0 shards after unregister, got %d", len(shards))
	}
}

func TestShardIndex_UnregisterShard_NotFound(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	err := idx.UnregisterShard("testdb", "cpu", "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent shard")
	}
}

func TestShardIndex_UnregisterShard_DatabaseNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	err := idx.UnregisterShard("nonexistent", "cpu", "x")
	if err == nil {
		t.Error("expected error for nonexistent database")
	}
}

func TestShardIndex_UnregisterShard_MeasurementNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	info := ShardInfo{ID: "0_3600", StartTime: 0, EndTime: 3600000000000}
	_ = idx.RegisterShard("testdb", "cpu", info)

	err := idx.UnregisterShard("testdb", "nonexistent", "0_3600")
	if err == nil {
		t.Error("expected error for nonexistent measurement")
	}
}

func TestShardIndex_ListShards_Success(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{
		ID: "0_3600", StartTime: 0, EndTime: 3600000000000,
	})
	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{
		ID: "3600_7200", StartTime: 3600000000000, EndTime: 7200000000000,
	})

	shards := idx.ListShards("testdb", "cpu")
	if len(shards) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(shards))
	}
	if shards[0].ID != "0_3600" {
		t.Errorf("expected first shard 0_3600, got %s", shards[0].ID)
	}
	if shards[1].ID != "3600_7200" {
		t.Errorf("expected second shard 3600_7200, got %s", shards[1].ID)
	}
}

func TestShardIndex_ListShards_Empty(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	shards := idx.ListShards("testdb", "cpu")
	if len(shards) != 0 {
		t.Errorf("expected empty list, got %d shards", len(shards))
	}
}

func TestShardIndex_ListShards_NonexistentDB(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	shards := idx.ListShards("nonexistent", "cpu")
	if len(shards) != 0 {
		t.Errorf("expected empty list, got %d shards", len(shards))
	}
}

func TestShardIndex_ListShards_SortedByStartTime(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	// register out of order
	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{
		ID: "7200_10800", StartTime: 7200000000000, EndTime: 10800000000000,
	})
	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{
		ID: "0_3600", StartTime: 0, EndTime: 3600000000000,
	})
	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{
		ID: "3600_7200", StartTime: 3600000000000, EndTime: 7200000000000,
	})

	shards := idx.ListShards("testdb", "cpu")
	if len(shards) != 3 {
		t.Fatalf("expected 3 shards, got %d", len(shards))
	}
	for i := 0; i < len(shards)-1; i++ {
		if shards[i].StartTime > shards[i+1].StartTime {
			t.Errorf("shards not sorted: index %d start=%d, index %d start=%d",
				i, shards[i].StartTime, i+1, shards[i+1].StartTime)
		}
	}
}

func TestShardIndex_QueryShards_ExactMatch(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{
		ID: "0_3600", StartTime: 0, EndTime: 3600000000000,
	})
	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{
		ID: "3600_7200", StartTime: 3600000000000, EndTime: 7200000000000,
	})
	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{
		ID: "7200_10800", StartTime: 7200000000000, EndTime: 10800000000000,
	})

	// query range that overlaps all three
	result := idx.QueryShards("testdb", "cpu", 0, 10800000000000)
	if len(result) != 3 {
		t.Errorf("expected 3 shards in query, got %d", len(result))
	}
}

func TestShardIndex_QueryShards_PartialOverlap(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{
		ID: "0_3600", StartTime: 0, EndTime: 3600000000000,
	})
	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{
		ID: "3600_7200", StartTime: 3600000000000, EndTime: 7200000000000,
	})
	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{
		ID: "7200_10800", StartTime: 7200000000000, EndTime: 10800000000000,
	})

	// query only the middle shard
	result := idx.QueryShards("testdb", "cpu", 4000000000000, 5000000000000)
	if len(result) != 1 {
		t.Errorf("expected 1 shard, got %d", len(result))
	}
	if result[0].ID != "3600_7200" {
		t.Errorf("expected 3600_7200, got %s", result[0].ID)
	}
}

func TestShardIndex_QueryShards_NoMatch(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{
		ID: "0_3600", StartTime: 0, EndTime: 3600000000000,
	})

	result := idx.QueryShards("testdb", "cpu", 10000000000000, 20000000000000)
	if len(result) != 0 {
		t.Errorf("expected 0 shards, got %d", len(result))
	}
}

func TestShardIndex_QueryShards_Nonexistent(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	result := idx.QueryShards("nonexistent", "cpu", 0, 100)
	if len(result) != 0 {
		t.Errorf("expected 0 shards, got %d", len(result))
	}
}

func TestShardIndex_UpdateShardStats_Success(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	info := ShardInfo{ID: "0_3600", StartTime: 0, EndTime: 3600000000000}
	_ = idx.RegisterShard("testdb", "cpu", info)

	if err := idx.UpdateShardStats("testdb", "cpu", "0_3600", 5, 10240); err != nil {
		t.Fatal("UpdateShardStats failed:", err)
	}

	shards := idx.ListShards("testdb", "cpu")
	if len(shards) != 1 {
		t.Fatal("expected 1 shard")
	}
	if shards[0].SSTableCount != 5 {
		t.Errorf("expected SSTableCount=5, got %d", shards[0].SSTableCount)
	}
	if shards[0].TotalSize != 10240 {
		t.Errorf("expected TotalSize=10240, got %d", shards[0].TotalSize)
	}
}

func TestShardIndex_UpdateShardStats_ShardNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	err := idx.UpdateShardStats("testdb", "cpu", "nonexistent", 0, 0)
	if err == nil {
		t.Error("expected error for nonexistent shard")
	}
}

func TestShardIndex_UpdateShardStats_DatabaseNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	err := idx.UpdateShardStats("nonexistent", "cpu", "x", 0, 0)
	if err == nil {
		t.Error("expected error for nonexistent database")
	}
}

func TestShardIndex_UpdateShardStats_MeasurementNotFound(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	info := ShardInfo{ID: "0_3600", StartTime: 0, EndTime: 3600000000000}
	_ = idx.RegisterShard("testdb", "cpu", info)

	err := idx.UpdateShardStats("testdb", "nonexistent", "0_3600", 0, 0)
	if err == nil {
		t.Error("expected error for nonexistent measurement")
	}
}

func TestShardIndex_DuplicateRegister(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	info := ShardInfo{ID: "test_shard", StartTime: 100, EndTime: 200}
	_ = idx.RegisterShard("testdb", "cpu", info)

	err := idx.RegisterShard("testdb", "cpu", info)
	if err == nil {
		t.Error("expected error for duplicate shard registration")
	}
}

func TestShardIndex_RegisterShard_ShardInfoFields(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	info := ShardInfo{
		ID:           "0_3600",
		StartTime:    0,
		EndTime:      3600000000000,
		DataDir:      "testdb/cpu/0_3600",
		SSTableCount: 3,
		TotalSize:    51200,
	}
	if err := idx.RegisterShard("testdb", "cpu", info); err != nil {
		t.Fatal("RegisterShard failed:", err)
	}

	shards := idx.ListShards("testdb", "cpu")
	if shards[0].ID != info.ID || shards[0].StartTime != info.StartTime ||
		shards[0].EndTime != info.EndTime || shards[0].SSTableCount != info.SSTableCount {
		t.Error("shard info fields mismatch")
	}
}

func TestMetadataDirNotCreated(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}
	defer func() { _ = m.Close() }()

	// verify _metadata directory is NOT created
	metaPath := dir + "/_metadata"
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Error("_metadata directory should not be created by new Manager")
	}

	// verify metadata.db exists instead
	dbPath := dir + "/metadata.db"
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("metadata.db should exist")
	}
}
