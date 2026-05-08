package metadata

import (
	"encoding/json"
	"fmt"
	"sort"

	bolt "go.etcd.io/bbolt"
)

// ===================================
// shardIndex — bbolt 版 ShardIndex
// ===================================

type shardIndex struct {
	db *bolt.DB
}

func newShardIndex(db *bolt.DB) *shardIndex {
	return &shardIndex{db: db}
}

func (idx *shardIndex) RegisterShard(database, measurement string, info ShardInfo) error {
	return idx.db.Update(func(tx *bolt.Tx) error {
		measBucket, err := ensureMeasBuckets(tx, database, measurement)
		if err != nil {
			return err
		}
		shardsBucket, err := ensureSubBucket(measBucket, "shards")
		if err != nil {
			return err
		}
		if shardsBucket.Get([]byte(info.ID)) != nil {
			return fmt.Errorf("shard %q already registered", info.ID)
		}
		data, err := json.Marshal(info)
		if err != nil {
			return fmt.Errorf("marshal shard info: %w", err)
		}
		return shardsBucket.Put([]byte(info.ID), data)
	})
}

func (idx *shardIndex) UnregisterShard(database, measurement, shardID string) error {
	return idx.db.Update(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return fmt.Errorf("measurement %q not found", measurement)
		}
		shardsBucket := measBucket.Bucket([]byte("shards"))
		if shardsBucket == nil {
			return fmt.Errorf("shard %q not found", shardID)
		}
		if shardsBucket.Get([]byte(shardID)) == nil {
			return fmt.Errorf("shard %q not found", shardID)
		}
		return shardsBucket.Delete([]byte(shardID))
	})
}

func (idx *shardIndex) QueryShards(database, measurement string, startTime, endTime int64) []ShardInfo {
	var result []ShardInfo
	_ = idx.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return nil
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return nil
		}
		shardsBucket := measBucket.Bucket([]byte("shards"))
		if shardsBucket == nil {
			return nil
		}
		return shardsBucket.ForEach(func(_, v []byte) error {
			var info ShardInfo
			if err := json.Unmarshal(v, &info); err != nil {
				return nil
			}
			if info.StartTime < endTime && info.EndTime > startTime {
				result = append(result, info)
			}
			return nil
		})
	})
	sort.Slice(result, func(i, j int) bool {
		return result[i].StartTime < result[j].StartTime
	})
	return result
}

func (idx *shardIndex) ListShards(database, measurement string) []ShardInfo {
	var result []ShardInfo
	_ = idx.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return nil
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return nil
		}
		shardsBucket := measBucket.Bucket([]byte("shards"))
		if shardsBucket == nil {
			return nil
		}
		return shardsBucket.ForEach(func(_, v []byte) error {
			var info ShardInfo
			if err := json.Unmarshal(v, &info); err != nil {
				return nil
			}
			result = append(result, info)
			return nil
		})
	})
	sort.Slice(result, func(i, j int) bool {
		return result[i].StartTime < result[j].StartTime
	})
	return result
}

func (idx *shardIndex) UpdateShardStats(database, measurement, shardID string, sstableCount int, totalSize int64) error {
	return idx.db.Update(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return fmt.Errorf("measurement %q not found", measurement)
		}
		shardsBucket := measBucket.Bucket([]byte("shards"))
		if shardsBucket == nil {
			return fmt.Errorf("shard %q not found", shardID)
		}
		raw := shardsBucket.Get([]byte(shardID))
		if raw == nil {
			return fmt.Errorf("shard %q not found", shardID)
		}
		var info ShardInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			return fmt.Errorf("unmarshal shard: %w", err)
		}
		info.SSTableCount = sstableCount
		info.TotalSize = totalSize
		data, err := json.Marshal(info)
		if err != nil {
			return fmt.Errorf("marshal shard: %w", err)
		}
		return shardsBucket.Put([]byte(shardID), data)
	})
}
