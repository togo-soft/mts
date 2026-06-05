package metadata

import (
	"fmt"
	"log/slog"

	bolt "go.etcd.io/bbolt"
)

// getTagsFromSeriesBucket 从 series bucket 读取指定 SID 的 tags。
func getTagsFromSeriesBucket(seriesBucket *bolt.Bucket, sid uint64) (map[string]string, error) {
	sidKey := encodeSIDKey(sid)
	data := seriesBucket.Get(sidKey)
	if data == nil {
		return nil, fmt.Errorf("sid %d not found", sid)
	}
	return unmarshalTags(data)
}

func (s *seriesStore) GetTags(database, measurement string, sid uint64) (map[string]string, bool) {
	key := s.cacheKey(database, measurement, sid)
	if cached, ok := s.cache.load(key); ok {
		return cached, true
	}

	var tags map[string]string
	if err := s.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return nil
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return nil
		}
		seriesBucket := measBucket.Bucket([]byte("series"))
		if seriesBucket == nil {
			return nil
		}
		t, err := getTagsFromSeriesBucket(seriesBucket, sid)
		if err != nil {
			return nil
		}
		tags = t
		return nil
	}); err != nil {
		slog.Warn("db.View failed", "error", err)
	}

	if tags != nil {
		s.cache.store(key, tags)
		return tags, true
	}
	return nil, false
}

func (s *seriesStore) GetSIDsByTag(database, measurement, tagKey, tagValue string) []uint64 {
	var sids []uint64
	if err := s.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return nil
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return nil
		}
		tagIdxBucket := measBucket.Bucket([]byte("tag_index"))
		if tagIdxBucket == nil {
			return nil
		}
		idxBucket := tagIdxBucket.Bucket([]byte(tagKey + "\x00" + tagValue))
		if idxBucket == nil {
			return nil
		}
		_ = idxBucket.ForEach(func(k, _ []byte) error {
			sids = append(sids, decodeSIDKey(k))
			return nil
		})
		return nil
	}); err != nil {
		slog.Warn("db.View failed", "error", err)
	}
	return sids
}

func (s *seriesStore) SeriesCount(database, measurement string) int {
	count := 0
	if err := s.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return nil
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return nil
		}
		seriesBucket := measBucket.Bucket([]byte("series"))
		if seriesBucket == nil {
			return nil
		}
		c := seriesBucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if string(k) == "_next_sid" {
				continue
			}
			count++
		}
		return nil
	}); err != nil {
		slog.Warn("db.View failed", "error", err)
	}
	return count
}
