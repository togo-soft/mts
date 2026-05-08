package metadata

import (
	"fmt"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// ===================================
// seriesStore — bbolt 版 SeriesStore
// ===================================

type seriesStore struct {
	db    *bolt.DB
	cache sync.Map // key: "db/meas/{sid}" → map[string]string (只缓存高频读 tags)
}

func newSeriesStore(db *bolt.DB) *seriesStore {
	return &seriesStore{db: db}
}

// ensureMeasBuckets 确保 db/meas 及其子 bucket 存在（写事务中调用）。
func ensureMeasBuckets(tx *bolt.Tx, dbName, measName string) (*bolt.Bucket, error) {
	dbBucket, err := tx.CreateBucketIfNotExists([]byte(dbName))
	if err != nil {
		return nil, fmt.Errorf("create db bucket %q: %w", dbName, err)
	}
	measBucket, err := dbBucket.CreateBucketIfNotExists([]byte(measName))
	if err != nil {
		return nil, fmt.Errorf("create meas bucket %q/%q: %w", dbName, measName, err)
	}
	return measBucket, nil
}

// ensureSubBucket 确保子 bucket 存在。
func ensureSubBucket(parent *bolt.Bucket, name string) (*bolt.Bucket, error) {
	b, err := parent.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, fmt.Errorf("create sub bucket %q: %w", name, err)
	}
	return b, nil
}

func (s *seriesStore) cacheKey(db, meas string, sid uint64) string {
	buf := make([]byte, 0, len(db)+1+len(meas)+1+8)
	buf = append(buf, db...)
	buf = append(buf, '/')
	buf = append(buf, meas...)
	buf = append(buf, '/')
	buf = appendSIDToBuf(buf, sid)
	return string(buf)
}

func appendSIDToBuf(buf []byte, v uint64) []byte {
	off := len(buf)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	buf[off] = byte(v >> 56)
	buf[off+1] = byte(v >> 48)
	buf[off+2] = byte(v >> 40)
	buf[off+3] = byte(v >> 32)
	buf[off+4] = byte(v >> 24)
	buf[off+5] = byte(v >> 16)
	buf[off+6] = byte(v >> 8)
	buf[off+7] = byte(v)
	return buf
}

func (s *seriesStore) AllocateSID(database, measurement string, tags map[string]string) (uint64, error) {
	h := tagsHash(tags)
	hashKey := encodeSIDKey(h)

	var sid uint64

	err := s.db.Update(func(tx *bolt.Tx) error {
		measBucket, err := ensureMeasBuckets(tx, database, measurement)
		if err != nil {
			return err
		}

		seriesBucket, err := ensureSubBucket(measBucket, "series")
		if err != nil {
			return err
		}

		hashIdxBucket, err := ensureSubBucket(measBucket, "hash_idx")
		if err != nil {
			return err
		}

		// 通过 hash_idx 快速查找已存在的 SID
		if existingSIDRaw := hashIdxBucket.Get(hashKey); existingSIDRaw != nil {
			existingSID := decodeSIDKey(existingSIDRaw)
			existingTags, err := getTagsFromSeriesBucket(seriesBucket, existingSID)
			if err == nil && tagsEqual(existingTags, tags) {
				sid = existingSID
				return nil // 已存在，直接返回
			}
		}

		// 分配新 SID
		currentNext := uint64(0)
		if raw := seriesBucket.Get([]byte("_next_sid")); raw != nil {
			currentNext = decodeUint64(raw)
		}

		newSID := currentNext
		currentNext++
		if err := seriesBucket.Put([]byte("_next_sid"), encodeUint64(currentNext)); err != nil {
			return fmt.Errorf("update next_sid: %w", err)
		}

		sidKey := encodeSIDKey(newSID)
		tagsJSON, err := marshalTags(tags)
		if err != nil {
			return fmt.Errorf("marshal tags: %w", err)
		}
		if err := seriesBucket.Put(sidKey, tagsJSON); err != nil {
			return fmt.Errorf("put series: %w", err)
		}

		// 写入 hash_idx
		if err := hashIdxBucket.Put(hashKey, sidKey); err != nil {
			return fmt.Errorf("put hash_idx: %w", err)
		}

		// 写入 tag_index
		tagIdxBucket, err := ensureSubBucket(measBucket, "tag_index")
		if err != nil {
			return err
		}
		for k, v := range tags {
			idxKey := []byte(k + "\x00" + v)
			idxBucket, err := ensureSubBucket(tagIdxBucket, string(idxKey))
			if err != nil {
				return fmt.Errorf("create tag_idx bucket: %w", err)
			}
			if err := idxBucket.Put(sidKey, []byte{}); err != nil {
				return fmt.Errorf("put tag_idx: %w", err)
			}
		}

		sid = newSID
		return nil
	})

	if err != nil {
		return 0, err
	}

	// 更新内存缓存
	s.cache.Store(s.cacheKey(database, measurement, sid), copyTags(tags))
	return sid, nil
}

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
	if cached, ok := s.cache.Load(key); ok {
		return copyTags(cached.(map[string]string)), true
	}

	var tags map[string]string
	_ = s.db.View(func(tx *bolt.Tx) error {
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
	})

	if tags != nil {
		s.cache.Store(key, copyTags(tags))
		return tags, true
	}
	return nil, false
}

func (s *seriesStore) GetSIDsByTag(database, measurement, tagKey, tagValue string) []uint64 {
	var sids []uint64
	_ = s.db.View(func(tx *bolt.Tx) error {
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
	})
	return sids
}

func (s *seriesStore) SeriesCount(database, measurement string) int {
	count := 0
	_ = s.db.View(func(tx *bolt.Tx) error {
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
	})
	return count
}

// rebuildCache 从 bbolt 遍历所有 series 重建内存缓存（Load 时调用）。
func (s *seriesStore) rebuildCache() error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(dbName []byte, dbBucket *bolt.Bucket) error {
			if len(dbName) > 0 && dbName[0] == '_' {
				return nil
			}
			cur := dbBucket.Cursor()
			for k, v := cur.First(); k != nil; k, v = cur.Next() {
				if v != nil {
					continue
				}
				measBucket := dbBucket.Bucket(k)
				if measBucket == nil {
					continue
				}
				measName := string(k)
				seriesBucket := measBucket.Bucket([]byte("series"))
				if seriesBucket == nil {
					continue
				}
				sc := seriesBucket.Cursor()
				for sk, sv := sc.First(); sk != nil; sk, sv = sc.Next() {
					if len(sk) == 0 || sk[0] == '_' {
						continue
					}
					sid := decodeSIDKey(sk)
					tags, err := unmarshalTags(sv)
					if err != nil {
						continue
					}
					ck := s.cacheKey(string(dbName), measName, sid)
					s.cache.Store(ck, tags)
				}
			}
			return nil
		})
	})
}

// ===================================
// MeasSeriesStore — 绑定 db/meas 的适配器
// ===================================

// MeasSeriesStore 将 SeriesStore 绑定到特定 database/measurement，
// 实现 shard.SeriesStore 接口（AllocateSID 和 GetTagsBySID 无 db/meas 参数）。
type MeasSeriesStore struct {
	store *seriesStore
	db    string
	meas  string
}

// NewMeasSeriesStore 创建绑定到指定 db/meas 的 MeasSeriesStore。
func NewMeasSeriesStore(store *seriesStore, db, meas string) *MeasSeriesStore {
	return &MeasSeriesStore{store: store, db: db, meas: meas}
}

// AllocateSID 为 tags 分配 SID。
func (a *MeasSeriesStore) AllocateSID(tags map[string]string) (uint64, error) {
	return a.store.AllocateSID(a.db, a.meas, tags)
}

// GetTagsBySID 根据 SID 获取 tags。
func (a *MeasSeriesStore) GetTagsBySID(sid uint64) (map[string]string, bool) {
	return a.store.GetTags(a.db, a.meas, sid)
}
