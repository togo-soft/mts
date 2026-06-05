package metadata

import (
	"fmt"
	"log/slog"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// ===================================
// seriesStore — bbolt 版 SeriesStore
// ===================================

const (
	defaultTagsCacheMaxSize = 100000
)

// tagsCache 有界并发安全的 SID→Tags 缓存，FIFO 淘汰。
type tagsCache struct {
	mu      sync.Mutex
	entries map[string]map[string]string
	order   []string
	maxSize int
}

// newTagsCache 创建有界 tags 缓存。
func newTagsCache(maxSize int) *tagsCache {
	if maxSize <= 0 {
		maxSize = defaultTagsCacheMaxSize
	}
	return &tagsCache{
		entries: make(map[string]map[string]string, maxSize),
		order:   make([]string, 0, maxSize),
		maxSize: maxSize,
	}
}

func (c *tagsCache) load(key string) (map[string]string, bool) {
	c.mu.Lock()
	v, ok := c.entries[key]
	c.mu.Unlock()
	return v, ok
}

func (c *tagsCache) store(key string, tags map[string]string) {
	c.mu.Lock()
	if _, exists := c.entries[key]; exists {
		c.entries[key] = tags
		c.mu.Unlock()
		return
	}
	if len(c.order) >= c.maxSize {
		oldKey := c.order[0]
		c.order[0] = ""
		// 原地左移，cap 不变，避免 append 重新分配
		copy(c.order, c.order[1:])
		c.order = c.order[:c.maxSize-1]
		delete(c.entries, oldKey)
	}
	c.order = append(c.order, key)
	if len(c.order) <= c.maxSize/2 {
		newOrder := make([]string, len(c.order), c.maxSize)
		copy(newOrder, c.order)
		c.order = newOrder
	}
	c.entries[key] = tags
	c.mu.Unlock()
}

type seriesStore struct {
	db           *bolt.DB
	cache        *tagsCache
	hashSidCache *hashSidCache
}

func newSeriesStore(db *bolt.DB) *seriesStore {
	return &seriesStore{
		db:           db,
		cache:        newTagsCache(0),
		hashSidCache: newHashSidCache(0),
	}
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

// hashCacheKey 构造 "db/meas/{hash}" 格式的缓存键。
func (s *seriesStore) hashCacheKey(db, meas string, h uint64) string {
	buf := make([]byte, len(db)+1+len(meas)+1+16)
	n := copy(buf, db)
	buf[n] = '/'
	n++
	n += copy(buf[n:], meas)
	buf[n] = '/'
	n++
	const hex = "0123456789abcdef"
	for i := 0; i < 8; i++ {
		b := byte(h >> (56 - i*8))
		buf[n+i*2] = hex[b>>4]
		buf[n+i*2+1] = hex[b&0xf]
	}
	return string(buf[:n+16])
}

// loadHashSid 从有界 hash 缓存中查找 SID。
func (s *seriesStore) loadHashSid(db, meas string, h uint64) (uint64, bool) {
	return s.hashSidCache.load(s.hashCacheKey(db, meas, h))
}

// storeHashSid 将 SID 存入有界 hash 缓存（FIFO 淘汰）。
func (s *seriesStore) storeHashSid(db, meas string, h, sid uint64) {
	s.hashSidCache.store(s.hashCacheKey(db, meas, h), sid)
}

func (s *seriesStore) AllocateSID(database, measurement string, tags map[string]string) (uint64, error) {
	h := tagsHash(tags)
	hashKey := encodeSIDKey(h)

	if sid, ok := s.loadHashSid(database, measurement, h); ok {
		return sid, nil
	}

	if sid, ok := s.lookupSIDReadOnly(database, measurement, hashKey, tags); ok {
		s.storeHashSid(database, measurement, h, sid)
		return sid, nil
	}

	sid, err := s.allocateSIDWriteTx(database, measurement, tags, hashKey)
	if err != nil {
		return 0, err
	}

	s.storeHashSid(database, measurement, h, sid)
	s.cache.store(s.cacheKey(database, measurement, sid), copyTags(tags))
	return sid, nil
}

// lookupSIDReadOnly 在只读事务中查找已存在的 SID（不触发 fsync）。
func (s *seriesStore) lookupSIDReadOnly(database, measurement string, hashKey []byte, tags map[string]string) (uint64, bool) {
	var sid uint64
	found := false
	if err := s.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return nil
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return nil
		}
		hashIdxBucket := measBucket.Bucket([]byte("hash_idx"))
		if hashIdxBucket == nil {
			return nil
		}
		seriesBucket := measBucket.Bucket([]byte("series"))
		if seriesBucket == nil {
			return nil
		}

		existingSIDRaw := hashIdxBucket.Get(hashKey)
		if existingSIDRaw == nil {
			return nil
		}
		existingSID := decodeSIDKey(existingSIDRaw)
		existingTags, err := getTagsFromSeriesBucket(seriesBucket, existingSID)
		if err == nil && tagsEqual(existingTags, tags) {
			sid = existingSID
			found = true
		}
		return nil
	}); err != nil {
		slog.Warn("db.View failed", "error", err)
	}
	if found {
		return sid, true
	}
	return 0, false
}

// allocateSIDWriteTx 在写事务中为新 tags 分配 SID。
func (s *seriesStore) allocateSIDWriteTx(database, measurement string, tags map[string]string, hashKey []byte) (uint64, error) {
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

		// 二次检查：其他 goroutine 可能在 View 和 Update 之间已插入
		if existingSIDRaw := hashIdxBucket.Get(hashKey); existingSIDRaw != nil {
			existingSID := decodeSIDKey(existingSIDRaw)
			existingTags, err := getTagsFromSeriesBucket(seriesBucket, existingSID)
			if err == nil && tagsEqual(existingTags, tags) {
				sid = existingSID
				return nil
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
	return sid, nil
}


