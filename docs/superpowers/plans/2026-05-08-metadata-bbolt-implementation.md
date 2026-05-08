# MTS 元数据系统 bbolt 重写实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用 `go.etcd.io/bbolt` 单文件替代所有 JSON 文件，重写 metadata 包实现，保留 Catalog/SeriesStore/ShardIndex 三层接口不变

**Architecture:** 引入 bbolt 作为唯一元数据存储引擎。catalogStore、seriesStore（含内存缓存）、shardIndex 三个实现通过 `*bolt.DB` 操作数据。提供 `MeasSeriesStore` 适配器给 Shard 层（绑定 db/meas），提供 `SimpleSeriesStore` 给外部测试（纯内存）。移除 `measurement.MeasurementMetaStore` / `DatabaseMetaStore` 及 persist.go、migrate.go

**Tech Stack:** Go 1.26, go.etcd.io/bbolt v1.4.3, encoding/json, encoding/binary

---

## File Structure Map

| File | Action | Responsibility |
|------|--------|----------------|
| `internal/storage/metadata/catalog.go` | **Keep** | Catalog 接口 + Schema/FieldDef 类型 |
| `internal/storage/metadata/series.go` | **Keep** | SeriesStore 接口 |
| `internal/storage/metadata/shard_index.go` | **Keep** | ShardIndex 接口 + ShardInfo 类型 |
| `internal/storage/metadata/manager.go` | **Rewrite** | Manager 结构体(持有 *bolt.DB), New/Load/Sync/Close |
| `internal/storage/metadata/catalog_impl.go` | **Rewrite** | catalogStore — bbolt 版 Catalog 实现 |
| `internal/storage/metadata/series_impl.go` | **Rewrite** | seriesStore(bbolt+缓存) + MeasSeriesStore(适配器) |
| `internal/storage/metadata/series_simple.go` | **Create** | SimpleSeriesStore 纯内存实现(外部测试用) |
| `internal/storage/metadata/shard_index_impl.go` | **Rewrite** | shardIndex — bbolt 版 ShardIndex 实现 |
| `internal/storage/metadata/persist.go` | **Delete** | 旧 atomicWrite/atomicRead |
| `internal/storage/metadata/migrate.go` | **Delete** | 旧格式迁移 |
| `internal/storage/metadata/*_test.go` | **Rewrite** | 基于临时 bbolt DB 的测试 |
| `internal/storage/measurement/meas_meta.go` | **Delete** | 旧 MeasurementMetaStore |
| `internal/storage/measurement/meas_meta_query.go` | **Delete** | 旧 MeasurementMetaStore 查询方法 |
| `internal/storage/measurement/db_meta.go` | **Delete** | 旧 DatabaseMetaStore |
| `internal/storage/measurement/*_test.go` | **Delete** | 旧 measurement 包测试 |
| `internal/engine/engine.go` | **Modify** | 适配 Manager API 变更(Persist→Sync) |
| `internal/storage/shard/manager.go` | **Modify** | GetOrCreateSeriesStore 返回类型适配 |
| `internal/storage/shard/*_test.go` | **Modify** | measurement.NewMeasurementMetaStore → metadata.NewSimpleSeriesStore |
| `internal/query/*_test.go` | **Modify** | measurement.NewMeasurementMetaStore → metadata.NewSimpleSeriesStore |
| `go.mod` | **Modify** | 添加 go.etcd.io/bbolt |

---

### Task 1: 添加 bbolt 依赖

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: 添加 bbolt 依赖**

```bash
cd /root/projects/mts && go get go.etcd.io/bbolt@v1.4.3
```

- [ ] **Step 2: 验证依赖下载成功**

```bash
cd /root/projects/mts && go mod tidy
```

Expected: 无错误输出，`go.mod` 中出现 `go.etcd.io/bbolt v1.4.3`

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: 添加 go.etcd.io/bbolt v1.4.3 依赖"
```

---

### Task 2: 实现 seriesStore（bbolt + 内存缓存）+ MeasSeriesStore 适配器

**Files:**
- Create: `internal/storage/metadata/series_simple.go` (SimpleSeriesStore + 工具函数)
- Rewrite: `internal/storage/metadata/series_impl.go`

先写 seriesStore，因为它是三个子系统中逻辑最复杂的（缓存、tag 倒排索引），也是 catalogStore 和 shardIndex 的参考模板。

- [ ] **Step 1: 创建 series_simple.go（SimpleSeriesStore + 从 measurement 包迁移的工具函数）**

```go
package metadata

import (
	"encoding/binary"
	"encoding/json"
	"sort"
	"sync"
)

// tagsHash 计算 tags 的哈希值（与顺序无关）。
func tagsHash(tags map[string]string) uint64 {
	if len(tags) == 0 {
		return 0
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var h uint64 = 14695981039346656037
	for _, k := range keys {
		v := tags[k]
		for i := 0; i < len(k); i++ {
			h ^= uint64(k[i])
			h *= 1099511628211
		}
		for i := 0; i < len(v); i++ {
			h ^= uint64(v[i])
			h *= 1099511628211
		}
	}
	return h
}

// tagsEqual 比较两个 tags map 是否相等。
func tagsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// copyTags 复制 tags map。
func copyTags(tags map[string]string) map[string]string {
	if tags == nil {
		return nil
	}
	result := make(map[string]string, len(tags))
	for k, v := range tags {
		result[k] = v
	}
	return result
}

// encodeSIDKey 将 uint64 SID 编码为 BigEndian 8 字节。
func encodeSIDKey(sid uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, sid)
	return buf
}

// encodeUint64 将 uint64 编码为 varint 字节。
func encodeUint64(v uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	return buf[:n]
}

// decodeUint64 从 varint 字节解码 uint64。
func decodeUint64(data []byte) uint64 {
	v, _ := binary.Uvarint(data)
	return v
}

// marshalTags 将 tags 序列化为 JSON。
func marshalTags(tags map[string]string) ([]byte, error) {
	return json.Marshal(tags)
}

// unmarshalTags 从 JSON 反序列化 tags。
func unmarshalTags(data []byte) (map[string]string, error) {
	tags := make(map[string]string)
	if err := json.Unmarshal(data, tags); err != nil {
		return nil, err
	}
	return tags, nil
}

// SimpleSeriesStore 是纯内存的 Series 存储，供外部测试使用。
// 实现 shard.SeriesStore 接口。
type SimpleSeriesStore struct {
	mu       sync.RWMutex
	series   map[uint64]map[string]string
	hashIdx  map[uint64]uint64
	tagIdx   map[string][]uint64
	nextSID  uint64
}

// NewSimpleSeriesStore 创建纯内存 SeriesStore。
func NewSimpleSeriesStore() *SimpleSeriesStore {
	return &SimpleSeriesStore{
		series:  make(map[uint64]map[string]string),
		hashIdx: make(map[uint64]uint64),
		tagIdx:  make(map[string][]uint64),
	}
}

// AllocateSID 分配或查找 SID。
func (s *SimpleSeriesStore) AllocateSID(tags map[string]string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	h := tagsHash(tags)
	if sid, ok := s.hashIdx[h]; ok {
		if tagsEqual(s.series[sid], tags) {
			return sid, nil
		}
	}

	sid := s.nextSID
	s.nextSID++
	s.series[sid] = copyTags(tags)
	s.hashIdx[h] = sid

	for k, v := range tags {
		key := k + "\x00" + v
		s.tagIdx[key] = append(s.tagIdx[key], sid)
	}
	return sid, nil
}

// GetTagsBySID 根据 SID 获取 tags。
func (s *SimpleSeriesStore) GetTagsBySID(sid uint64) (map[string]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tags, ok := s.series[sid]
	if !ok {
		return nil, false
	}
	return copyTags(tags), true
}
```

- [ ] **Step 2: 重写 series_impl.go（seriesStore + MeasSeriesStore）**

```go
package metadata

import (
	"encoding/json"
	"fmt"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// ===================================
// seriesStore — bbolt 版 SeriesStore
// ===================================

type seriesStore struct {
	db       *bolt.DB
	cache    sync.Map // key: "db/meas/{sid}" → map[string]string
	nextSIDs sync.Map // key: "db/meas" → uint64
}

func newSeriesStore(db *bolt.DB) *seriesStore {
	return &seriesStore{db: db}
}

// ensureMeas 确保 db/meas 的 bucket 层级存在（写事务中调用）。
func ensureMeas(tx *bolt.Tx, dbName, measName string) (*bolt.Bucket, error) {
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

// ensureSubBucket 在 meas bucket 下确保子 bucket 存在。
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
	buf = binaryBigEndianAppend(buf, sid)
	return string(buf)
}

func binaryBigEndianAppend(buf []byte, v uint64) []byte {
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
		measBucket, err := ensureMeas(tx, database, measurement)
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
			existingTags, err := s.getTagsBySIDLocked(seriesBucket, existingSID)
			if err == nil && tagsEqual(existingTags, tags) {
				sid = existingSID
				return nil // 已存在，直接返回
			}
			// hash 碰撞：tags 不相等但 hash 相同
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

func decodeSIDKey(data []byte) uint64 {
	if len(data) < 8 {
		return 0
	}
	return binaryBigEndianToUint64(data)
}

func binaryBigEndianToUint64(b []byte) uint64 {
	_ = b[7]
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

func (s *seriesStore) getTagsBySIDLocked(seriesBucket *bolt.Bucket, sid uint64) (map[string]string, error) {
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
		t, err := s.getTagsBySIDLocked(seriesBucket, sid)
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
			return dbBucket.ForEach(func(measName []byte, measBucket *bolt.Bucket) error {
				seriesBucket := measBucket.Bucket([]byte("series"))
				if seriesBucket == nil {
					return nil
				}
				c := seriesBucket.Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					if len(k) == 0 || k[0] == '_' {
						continue
					}
					sid := decodeSIDKey(k)
					tags, err := unmarshalTags(v)
					if err != nil {
						continue
					}
					cacheKey := s.cacheKey(string(dbName), string(measName), sid)
					s.cache.Store(cacheKey, tags)
				}
				return nil
			})
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

// AllocateSID 为 tags 分配 SID。
func (a *MeasSeriesStore) AllocateSID(tags map[string]string) (uint64, error) {
	return a.store.AllocateSID(a.db, a.meas, tags)
}

// GetTagsBySID 根据 SID 获取 tags。
func (a *MeasSeriesStore) GetTagsBySID(sid uint64) (map[string]string, bool) {
	return a.store.GetTags(a.db, a.meas, sid)
}
```

- [ ] **Step 3: Commit**

```bash
git add internal/storage/metadata/series_impl.go internal/storage/metadata/series_simple.go
git commit -m "feat(metadata): 实现 bbolt 版 seriesStore 与 MeasSeriesStore 适配器"
```

---

### Task 3: 实现 catalogStore（bbolt 版 Catalog）

**Files:**
- Rewrite: `internal/storage/metadata/catalog_impl.go`

- [ ] **Step 1: 重写 catalog_impl.go**

```go
package metadata

import (
	"fmt"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ===================================
// catalogStore — bbolt 版 Catalog
// ===================================

type catalogStore struct {
	db *bolt.DB
}

func newCatalogStore(db *bolt.DB) *catalogStore {
	return &catalogStore{db: db}
}

func (c *catalogStore) CreateDatabase(name string) error {
	if name == "" {
		return fmt.Errorf("database name is empty")
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return fmt.Errorf("create database bucket: %w", err)
		}
		return nil
	})
}

func (c *catalogStore) DropDatabase(name string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte(name)); err != nil {
			if err == bolt.ErrBucketNotFound {
				return fmt.Errorf("database %q not found", name)
			}
			return fmt.Errorf("delete database bucket: %w", err)
		}
		return nil
	})
}

func (c *catalogStore) ListDatabases() []string {
	var names []string
	_ = c.db.View(func(tx *bolt.Tx) error {
		_ = tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			if len(name) == 0 || name[0] == '_' {
				return nil
			}
			names = append(names, string(name))
			return nil
		})
		return nil
	})
	sort.Strings(names)
	return names
}

func (c *catalogStore) DatabaseExists(name string) bool {
	exists := false
	_ = c.db.View(func(tx *bolt.Tx) error {
		exists = tx.Bucket([]byte(name)) != nil
		return nil
	})
	return exists
}

func (c *catalogStore) CreateMeasurement(database, name string) error {
	if database == "" {
		return fmt.Errorf("database name is empty")
	}
	if name == "" {
		return fmt.Errorf("measurement name is empty")
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		if dbBucket.Bucket([]byte(name)) != nil {
			return nil // 已存在
		}
		measBucket, err := dbBucket.CreateBucket([]byte(name))
		if err != nil {
			return fmt.Errorf("create measurement bucket: %w", err)
		}
		now := time.Now().UnixNano()
		if err := measBucket.Put([]byte("_created"), encodeUint64(uint64(now))); err != nil {
			return fmt.Errorf("put _created: %w", err)
		}
		return nil
	})
}

func (c *catalogStore) DropMeasurement(database, name string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		if err := dbBucket.DeleteBucket([]byte(name)); err != nil {
			if err == bolt.ErrBucketNotFound {
				return fmt.Errorf("measurement %q not found", name)
			}
			return fmt.Errorf("delete measurement bucket: %w", err)
		}
		return nil
	})
}

func (c *catalogStore) ListMeasurements(database string) ([]string, error) {
	var names []string
	err := c.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		return dbBucket.ForEach(func(name []byte, b *bolt.Bucket) error {
			names = append(names, string(name))
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func (c *catalogStore) MeasurementExists(database, name string) bool {
	exists := false
	_ = c.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return nil
		}
		exists = dbBucket.Bucket([]byte(name)) != nil
		return nil
	})
	return exists
}

func (c *catalogStore) GetRetention(database, measurement string) (time.Duration, error) {
	var d time.Duration
	err := c.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return fmt.Errorf("measurement %q not found", measurement)
		}
		raw := measBucket.Get([]byte("_retention"))
		if raw != nil {
			d = time.Duration(decodeUint64(raw))
		}
		return nil
	})
	return d, err
}

func (c *catalogStore) SetRetention(database, measurement string, d time.Duration) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return fmt.Errorf("measurement %q not found", measurement)
		}
		return measBucket.Put([]byte("_retention"), encodeUint64(uint64(d)))
	})
}

func (c *catalogStore) GetSchema(database, measurement string) (*Schema, error) {
	var s Schema
	err := c.db.View(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return fmt.Errorf("measurement %q not found", measurement)
		}
		raw := measBucket.Get([]byte("_schema"))
		if raw == nil {
			return nil
		}
		return json.Unmarshal(raw, &s)
	})
	if err != nil {
		return nil, err
	}
	if s.Version == 0 {
		return nil, nil
	}
	return &s, nil
}

func (c *catalogStore) SetSchema(database, measurement string, s *Schema) error {
	if s == nil {
		return fmt.Errorf("schema is nil")
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		dbBucket := tx.Bucket([]byte(database))
		if dbBucket == nil {
			return fmt.Errorf("database %q not found", database)
		}
		measBucket := dbBucket.Bucket([]byte(measurement))
		if measBucket == nil {
			return fmt.Errorf("measurement %q not found", measurement)
		}

		// 校验兼容性
		if raw := measBucket.Get([]byte("_schema")); raw != nil {
			var existing Schema
			if err := json.Unmarshal(raw, &existing); err == nil {
				if err := validateSchemaUpdate(&existing, s); err != nil {
					return err
				}
			}
		}

		s.UpdatedAt = time.Now().UnixNano()
		data, err := json.Marshal(s)
		if err != nil {
			return fmt.Errorf("marshal schema: %w", err)
		}
		return measBucket.Put([]byte("_schema"), data)
	})
}
```

保留 validateSchemaUpdate 函数从旧 `catalog_impl.go`。

- [ ] **Step 2: Commit**

```bash
git add internal/storage/metadata/catalog_impl.go
git commit -m "feat(metadata): 实现 bbolt 版 catalogStore"
```

---

### Task 4: 实现 shardIndex（bbolt 版 ShardIndex）

**Files:**
- Rewrite: `internal/storage/metadata/shard_index_impl.go`

- [ ] **Step 1: 重写 shard_index_impl.go**

```go
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
		measBucket, err := ensureMeas(tx, database, measurement)
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
		_ = shardsBucket.ForEach(func(_, v []byte) error {
			var info ShardInfo
			if err := json.Unmarshal(v, &info); err != nil {
				return nil
			}
			if info.StartTime < endTime && info.EndTime > startTime {
				result = append(result, info)
			}
			return nil
		})
		return nil
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
		_ = shardsBucket.ForEach(func(_, v []byte) error {
			var info ShardInfo
			if err := json.Unmarshal(v, &info); err != nil {
				return nil
			}
			result = append(result, info)
			return nil
		})
		return nil
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
```

- [ ] **Step 2: Commit**

```bash
git add internal/storage/metadata/shard_index_impl.go
git commit -m "feat(metadata): 实现 bbolt 版 shardIndex"
```

---

### Task 5: 重写 Manager（持有 bolt.DB）

**Files:**
- Rewrite: `internal/storage/metadata/manager.go`

- [ ] **Step 1: 重写 manager.go**

```go
package metadata

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// Manager 是所有元数据子系统的聚合入口。
type Manager struct {
	catalog    *catalogStore
	series     *seriesStore
	shardIndex *shardIndex
	db         *bolt.DB
	dbPath     string
	closeOnce  sync.Once
}

// NewManager 创建新的 Manager 实例。
func NewManager(dataDir string) (*Manager, error) {
	dbPath := filepath.Join(dataDir, "metadata.db")

	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt db: %w", err)
	}

	m := &Manager{
		db:     db,
		dbPath: dbPath,
	}
	m.catalog = newCatalogStore(db)
	m.series = newSeriesStore(db)
	m.shardIndex = newShardIndex(db)
	return m, nil
}

// Catalog 返回 Catalog 子系统。
func (m *Manager) Catalog() Catalog { return m.catalog }

// Series 返回 SeriesStore 子系统。
func (m *Manager) Series() SeriesStore { return m.series }

// Shards 返回 ShardIndex 子系统。
func (m *Manager) Shards() ShardIndex { return m.shardIndex }

// GetOrCreateSeriesStore 创建绑定 db/meas 的 SeriesStore 适配器，供 ShardManager 使用。
func (m *Manager) GetOrCreateSeriesStore(db, meas string) *MeasSeriesStore {
	return &MeasSeriesStore{store: m.series, db: db, meas: meas}
}

// Load 从 bbolt 重建 series 内存缓存。
func (m *Manager) Load() error {
	return m.series.rebuildCache()
}

// Sync 强制 fsync bbolt 数据库。
func (m *Manager) Sync() error {
	return m.db.Sync()
}

// Close 关闭 bbolt 数据库。
func (m *Manager) Close() error {
	var err error
	m.closeOnce.Do(func() {
		err = m.db.Close()
	})
	return err
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/storage/metadata/manager.go
git commit -m "feat(metadata): 重写 Manager 以持有 bolt.DB"
```

---

### Task 6: 删除旧文件

**Files:**
- Delete: `internal/storage/metadata/persist.go`
- Delete: `internal/storage/metadata/migrate.go`
- Delete: `internal/storage/measurement/meas_meta.go`
- Delete: `internal/storage/measurement/meas_meta_query.go`
- Delete: `internal/storage/measurement/db_meta.go`
- Delete: `internal/storage/measurement/meas_meta_test.go`
- Delete: `internal/storage/measurement/meas_meta_bench_test.go`
- Delete: `internal/storage/measurement/meta_extra_test.go`
- Delete: `internal/storage/measurement/db_meta_test.go`

- [ ] **Step 1: 删除所有旧文件**

```bash
rm internal/storage/metadata/persist.go
rm internal/storage/metadata/migrate.go
rm internal/storage/measurement/meas_meta.go
rm internal/storage/measurement/meas_meta_query.go
rm internal/storage/measurement/db_meta.go
rm internal/storage/measurement/meas_meta_test.go
rm internal/storage/measurement/meas_meta_bench_test.go
rm internal/storage/measurement/meta_extra_test.go
rm internal/storage/measurement/db_meta_test.go
```

- [ ] **Step 2: Commit**

```bash
git add -A internal/storage/metadata/ internal/storage/measurement/
git commit -m "refactor(metadata): 删除旧 JSON 持久化与 migration 代码，移除 measurement 包旧类型"
```

---

### Task 7: 适配 engine.go（Manager API 变更）

**Files:**
- Modify: `internal/engine/engine.go`

- [ ] **Step 1: 修改 engine.go**

`Manager.New()` 不再返回错误（当前没有，bbolt 版本会返回 error）。将 `PersistAll()` 调用改为 `Sync()`。

将 `engine.go:70` 附近：
```go
mgr, err := metadata.NewManager(cfg.DataDir)
```
保持不变（NewManager 现在返回 error）。

将 `engine.go:115` 附近：
```go
if err := e.shardManager.PersistAll(); err != nil {
    return fmt.Errorf("persist metadata: %w", err)
}
```
改为：
```go
if err := e.manager.Sync(); err != nil {
    return fmt.Errorf("sync metadata: %w", err)
}
```

- [ ] **Step 2: 修改 shard/manager.go 的 PersistAll**

`internal/storage/shard/manager.go:203`：
```go
func (m *ShardManager) PersistAll() error {
    return m.manager.Persist()
}
```
改为：
```go
func (m *ShardManager) PersistAll() error {
    return m.manager.Sync()
}
```

- [ ] **Step 3: Commit**

```bash
git add internal/engine/engine.go internal/storage/shard/manager.go
git commit -m "refactor(engine): 适配 Manager API 变更 Persist→Sync"
```

---

### Task 8: 更新所有 shard 和 query 测试文件（替换 measurement.NewMeasurementMetaStore）

**Files:**
- Modify: `internal/storage/shard/shard_test.go`
- Modify: `internal/storage/shard/shard_extra_test.go`
- Modify: `internal/storage/shard/compaction_test.go`
- Modify: `internal/storage/shard/level_compaction_test.go`
- Modify: `internal/storage/shard/level_compaction_e2e_test.go`
- Modify: `internal/storage/shard/manager_test.go`
- Modify: `internal/query/iterator_test.go`

- [ ] **Step 1: 全局替换**

将所有 `measurement.NewMeasurementMetaStore()` 替换为 `metadata.NewSimpleSeriesStore()`。

同时移除这些测试文件中 `"codeberg.org/micro-ts/mts/internal/storage/measurement"` 的 import。

```bash
cd /root/projects/mts
# 先在 shard 测试中替换
find internal/storage/shard -name '*_test.go' -exec sed -i 's|measurement\.NewMeasurementMetaStore()|metadata.NewSimpleSeriesStore()|g' {} +
find internal/query -name '*_test.go' -exec sed -i 's|measurement\.NewMeasurementMetaStore()|metadata.NewSimpleSeriesStore()|g' {} +
```

- [ ] **Step 2: 移除 measurement import**

对于不再引用 `measurement` 包的文件（替换后只剩下 `SimpleSeriesStore` 相关的 imports），移除 measurement 的 import 行。检查 `manager_test.go` 中的 `measurement.NewDatabaseMetaStore()` 也需要删除（不再需要）。

手动清理每个文件中的 import 块，删除 `"codeberg.org/micro-ts/mts/internal/storage/measurement"` 行。

- [ ] **Step 3: Commit**

```bash
git add internal/storage/shard/ internal/query/
git commit -m "refactor(test): 用 metadata.NewSimpleSeriesStore 替换 measurement.NewMeasurementMetaStore"
```

---

### Task 9: 编写 metadata 包测试

**Files:**
- Create/Rewrite: `internal/storage/metadata/catalog_test.go`
- Create/Rewrite: `internal/storage/metadata/series_test.go`
- Create/Rewrite: `internal/storage/metadata/shard_index_test.go`
- Create/Rewrite: `internal/storage/metadata/manager_test.go`

- [ ] **Step 1: 创建测试辅助函数**

在 `internal/storage/metadata/` 目录下创建 `test_helpers_test.go`:

```go
package metadata

import (
	"os"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func openTestDB(t *testing.T) (*bolt.DB, string) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "mts-metadata-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	dbPath := tmpDir + "/metadata.db"
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("open bolt: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
		os.RemoveAll(tmpDir)
	})
	return db, tmpDir
}

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	db, _ := openTestDB(t)
	return &Manager{
		db:         db,
		catalog:    newCatalogStore(db),
		series:     newSeriesStore(db),
		shardIndex: newShardIndex(db),
	}
}
```

- [ ] **Step 2: 编写 catalog_test.go**

测试用例：
- `TestCatalogStore_CreateDatabase` — 创建成功，重复创建不报错，空名称报错
- `TestCatalogStore_DropDatabase` — 删除存在的 DB，删除不存在的 DB 报错
- `TestCatalogStore_ListDatabases` — 空列表、创建多个后的列表、排序验证
- `TestCatalogStore_DatabaseExists` — 存在返回 true，不存在返回 false
- `TestCatalogStore_CreateMeasurement` — 创建成功，DB 不存在报错，空名称报错，重复创建不报错
- `TestCatalogStore_DropMeasurement` — 删除成功，DB 不存在报错，Meas 不存在报错
- `TestCatalogStore_ListMeasurements` — 正常列表，DB 不存在报错
- `TestCatalogStore_MeasurementExists` — 存在/不存在
- `TestCatalogStore_GetSetRetention` — 设置并读取，默认值为 0
- `TestCatalogStore_GetSetSchema` — 设置并读取，nil schema 报错，更新兼容/不兼容
- `TestCatalogStore_Schema_NotFound` — 获取未设置的 schema 返回 nil

```go
package metadata

import (
	"testing"
	"time"
)

func TestCatalogStore_CreateDatabase(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	if err := cs.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	// 重复创建不报错
	if err := cs.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase duplicate failed: %v", err)
	}
	// 空名称
	if err := cs.CreateDatabase(""); err == nil {
		t.Fatal("expected error for empty name")
	}
	if !cs.DatabaseExists("testdb") {
		t.Fatal("expected DatabaseExists true")
	}
}

func TestCatalogStore_DropDatabase(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	if err := cs.DropDatabase("testdb"); err != nil {
		t.Fatalf("DropDatabase failed: %v", err)
	}
	if cs.DatabaseExists("testdb") {
		t.Fatal("expected DatabaseExists false after drop")
	}
	if err := cs.DropDatabase("nonexistent"); err == nil {
		t.Fatal("expected error for nonexistent database")
	}
}

func TestCatalogStore_ListDatabases(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	names := cs.ListDatabases()
	if len(names) != 0 {
		t.Fatalf("expected empty list, got %v", names)
	}
	_ = cs.CreateDatabase("zulu")
	_ = cs.CreateDatabase("alpha")
	names = cs.ListDatabases()
	if len(names) != 2 {
		t.Fatalf("expected 2 databases, got %d", len(names))
	}
	if names[0] != "alpha" || names[1] != "zulu" {
		t.Fatalf("expected sorted [alpha zulu], got %v", names)
	}
}

func TestCatalogStore_DatabaseExists(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	if cs.DatabaseExists("testdb") {
		t.Fatal("expected false for nonexistent")
	}
	_ = cs.CreateDatabase("testdb")
	if !cs.DatabaseExists("testdb") {
		t.Fatal("expected true after create")
	}
}

func TestCatalogStore_CreateMeasurement(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	// DB 不存在
	if err := cs.CreateMeasurement("nonexistent", "meas"); err == nil {
		t.Fatal("expected error for nonexistent DB")
	}
	_ = cs.CreateDatabase("testdb")
	// 正常创建
	if err := cs.CreateMeasurement("testdb", "cpu"); err != nil {
		t.Fatalf("CreateMeasurement failed: %v", err)
	}
	// 重复创建不报错
	if err := cs.CreateMeasurement("testdb", "cpu"); err != nil {
		t.Fatalf("CreateMeasurement duplicate failed: %v", err)
	}
	// 空名称
	if err := cs.CreateMeasurement("testdb", ""); err == nil {
		t.Fatal("expected error for empty measurement name")
	}
	if err := cs.CreateMeasurement("", "cpu"); err == nil {
		t.Fatal("expected error for empty database name")
	}
}

func TestCatalogStore_DropMeasurement(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")
	if err := cs.DropMeasurement("testdb", "cpu"); err != nil {
		t.Fatalf("DropMeasurement failed: %v", err)
	}
	if cs.MeasurementExists("testdb", "cpu") {
		t.Fatal("expected MeasurementExists false after drop")
	}
	// 不存在的 measurement
	if err := cs.DropMeasurement("testdb", "nonexistent"); err == nil {
		t.Fatal("expected error for nonexistent measurement")
	}
}

func TestCatalogStore_ListMeasurements(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "memory")
	_ = cs.CreateMeasurement("testdb", "cpu")
	names, err := cs.ListMeasurements("testdb")
	if err != nil {
		t.Fatalf("ListMeasurements failed: %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("expected 2 measurements, got %d", len(names))
	}
	if names[0] != "cpu" || names[1] != "memory" {
		t.Fatalf("expected sorted [cpu memory], got %v", names)
	}
}

func TestCatalogStore_MeasurementExists(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")
	if !cs.MeasurementExists("testdb", "cpu") {
		t.Fatal("expected true")
	}
	if cs.MeasurementExists("testdb", "nonexistent") {
		t.Fatal("expected false")
	}
	if cs.MeasurementExists("nonexistent", "cpu") {
		t.Fatal("expected false for nonexistent DB")
	}
}

func TestCatalogStore_Retention(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	d, err := cs.GetRetention("testdb", "cpu")
	if err != nil {
		t.Fatalf("GetRetention failed: %v", err)
	}
	if d != 0 {
		t.Fatalf("expected default retention 0, got %v", d)
	}

	dur := 24 * time.Hour
	if err := cs.SetRetention("testdb", "cpu", dur); err != nil {
		t.Fatalf("SetRetention failed: %v", err)
	}
	d, err = cs.GetRetention("testdb", "cpu")
	if err != nil {
		t.Fatalf("GetRetention after set failed: %v", err)
	}
	if d != dur {
		t.Fatalf("expected %v, got %v", dur, d)
	}
}

func TestCatalogStore_Schema(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	// 未设置时返回 nil
	s, err := cs.GetSchema("testdb", "cpu")
	if err != nil {
		t.Fatalf("GetSchema failed: %v", err)
	}
	if s != nil {
		t.Fatal("expected nil schema for unset")
	}

	// 设置 schema
	schema := &Schema{
		Version: 1,
		Fields:  []FieldDef{{Name: "usage", Type: 2}},
		TagKeys: []string{"host"},
	}
	if err := cs.SetSchema("testdb", "cpu", schema); err != nil {
		t.Fatalf("SetSchema failed: %v", err)
	}

	s, err = cs.GetSchema("testdb", "cpu")
	if err != nil {
		t.Fatalf("GetSchema after set failed: %v", err)
	}
	if s == nil || s.Version != 1 || len(s.Fields) != 1 || s.Fields[0].Name != "usage" {
		t.Fatalf("unexpected schema: %+v", s)
	}
	if len(s.TagKeys) != 1 || s.TagKeys[0] != "host" {
		t.Fatalf("unexpected tag keys: %v", s.TagKeys)
	}

	// nil schema 报错
	if err := cs.SetSchema("testdb", "cpu", nil); err == nil {
		t.Fatal("expected error for nil schema")
	}
}

func TestCatalogStore_Schema_IncompatibleUpdate(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	_ = cs.SetSchema("testdb", "cpu", &Schema{
		Version: 1,
		Fields:  []FieldDef{{Name: "usage", Type: 2}},
	})
	err := cs.SetSchema("testdb", "cpu", &Schema{
		Version: 2,
		Fields:  []FieldDef{{Name: "usage", Type: 3}}, // 类型变更
	})
	if err == nil {
		t.Fatal("expected error for incompatible type change")
	}
}
```

- [ ] **Step 3: 编写 series_test.go**

```go
package metadata

import (
	"testing"
)

func TestSeriesStore_AllocateSID(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	ss := newSeriesStore(db)

	tags1 := map[string]string{"host": "server1", "region": "us-east"}
	sid1, err := ss.AllocateSID("testdb", "cpu", tags1)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	if sid1 != 0 {
		t.Fatalf("expected first SID 0, got %d", sid1)
	}

	// 相同 tags 返回相同 SID
	sid, err := ss.AllocateSID("testdb", "cpu", tags1)
	if err != nil {
		t.Fatalf("AllocateSID same tags failed: %v", err)
	}
	if sid != sid1 {
		t.Fatalf("expected same SID %d, got %d", sid1, sid)
	}

	// 不同 tags 返回不同 SID
	tags2 := map[string]string{"host": "server2", "region": "us-west"}
	sid2, err := ss.AllocateSID("testdb", "cpu", tags2)
	if err != nil {
		t.Fatalf("AllocateSID tags2 failed: %v", err)
	}
	if sid2 != 1 {
		t.Fatalf("expected second SID 1, got %d", sid2)
	}
}

func TestSeriesStore_AllocateSID_TagOrderIndependence(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	ss := newSeriesStore(db)

	sid1, _ := ss.AllocateSID("testdb", "cpu", map[string]string{"a": "1", "b": "2"})
	sid2, _ := ss.AllocateSID("testdb", "cpu", map[string]string{"b": "2", "a": "1"})
	if sid1 != sid2 {
		t.Fatalf("expected same SID regardless of tag order: %d vs %d", sid1, sid2)
	}
}

func TestSeriesStore_GetTags(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	ss := newSeriesStore(db)

	tags := map[string]string{"host": "server1"}
	sid, _ := ss.AllocateSID("testdb", "cpu", tags)

	result, ok := ss.GetTags("testdb", "cpu", sid)
	if !ok {
		t.Fatal("expected GetTags ok")
	}
	if result["host"] != "server1" {
		t.Fatalf("expected host=server1, got %v", result)
	}

	// 不存在的 SID
	_, ok = ss.GetTags("testdb", "cpu", 999)
	if ok {
		t.Fatal("expected false for nonexistent SID")
	}

	// 不存在的 measurement
	_, ok = ss.GetTags("testdb", "nonexistent", 0)
	if ok {
		t.Fatal("expected false for nonexistent measurement")
	}
}

func TestSeriesStore_GetTags_CacheHit(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	ss := newSeriesStore(db)

	tags := map[string]string{"host": "server1"}
	sid, _ := ss.AllocateSID("testdb", "cpu", tags)

	// 第一次查询 — 从 bbolt 读并填充缓存
	result, ok := ss.GetTags("testdb", "cpu", sid)
	if !ok || result["host"] != "server1" {
		t.Fatal("first GetTags failed")
	}

	// 验证缓存命中（通过 key 存在性）
	key := ss.cacheKey("testdb", "cpu", sid)
	if _, ok := ss.cache.Load(key); !ok {
		t.Fatal("expected cache populated")
	}
}

func TestSeriesStore_GetSIDsByTag(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	ss := newSeriesStore(db)

	ss.AllocateSID("testdb", "cpu", map[string]string{"host": "server1", "dc": "nyc"})
	ss.AllocateSID("testdb", "cpu", map[string]string{"host": "server2", "dc": "nyc"})
	ss.AllocateSID("testdb", "cpu", map[string]string{"host": "server1", "dc": "sf"})

	sids := ss.GetSIDsByTag("testdb", "cpu", "host", "server1")
	if len(sids) != 2 {
		t.Fatalf("expected 2 SIDs for host=server1, got %d", len(sids))
	}

	sids = ss.GetSIDsByTag("testdb", "cpu", "dc", "nyc")
	if len(sids) != 2 {
		t.Fatalf("expected 2 SIDs for dc=nyc, got %d", len(sids))
	}

	sids = ss.GetSIDsByTag("testdb", "cpu", "nonexistent", "value")
	if len(sids) != 0 {
		t.Fatalf("expected 0 SIDs for nonexistent tag, got %d", len(sids))
	}
}

func TestSeriesStore_SeriesCount(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	ss := newSeriesStore(db)

	if count := ss.SeriesCount("testdb", "cpu"); count != 0 {
		t.Fatalf("expected 0, got %d", count)
	}

	ss.AllocateSID("testdb", "cpu", map[string]string{"host": "server1"})
	ss.AllocateSID("testdb", "cpu", map[string]string{"host": "server2"})
	ss.AllocateSID("testdb", "cpu", map[string]string{"host": "server1"}) // 重复

	if count := ss.SeriesCount("testdb", "cpu"); count != 2 {
		t.Fatalf("expected 2 unique series, got %d", count)
	}
}

func TestSeriesStore_CrossMeasurementIsolation(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")
	_ = cs.CreateMeasurement("testdb", "memory")

	ss := newSeriesStore(db)

	tags := map[string]string{"host": "server1"}
	sidCPU, _ := ss.AllocateSID("testdb", "cpu", tags)
	sidMem, _ := ss.AllocateSID("testdb", "memory", tags)

	if sidCPU != 0 {
		t.Fatalf("expected cpu SID 0, got %d", sidCPU)
	}
	if sidMem != 0 {
		t.Fatalf("expected memory SID 0, got %d", sidMem)
	}
}

func TestSeriesStore_RebuildCache(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	ss := newSeriesStore(db)

	ss.AllocateSID("testdb", "cpu", map[string]string{"host": "server1"})
	ss.AllocateSID("testdb", "cpu", map[string]string{"host": "server2"})

	// 创建新的 seriesStore 并从 bbolt 重建缓存
	ss2 := newSeriesStore(db)
	if err := ss2.rebuildCache(); err != nil {
		t.Fatalf("rebuildCache failed: %v", err)
	}

	if count := ss2.SeriesCount("testdb", "cpu"); count != 2 {
		t.Fatalf("expected 2 series after rebuild, got %d", count)
	}

	tags, ok := ss2.GetTags("testdb", "cpu", 0)
	if !ok || tags["host"] != "server1" {
		t.Fatalf("expected server1 tags after rebuild, got %v", tags)
	}
}

func TestMeasSeriesStore(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	ss := newSeriesStore(db)
	adapter := &MeasSeriesStore{store: ss, db: "testdb", meas: "cpu"}

	tags := map[string]string{"host": "server1"}
	sid, err := adapter.AllocateSID(tags)
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	if sid != 0 {
		t.Fatalf("expected SID 0, got %d", sid)
	}

	result, ok := adapter.GetTagsBySID(sid)
	if !ok {
		t.Fatal("expected GetTagsBySID ok")
	}
	if result["host"] != "server1" {
		t.Fatalf("expected host=server1, got %v", result)
	}
}

func TestSimpleSeriesStore(t *testing.T) {
	ss := NewSimpleSeriesStore()

	sid1, _ := ss.AllocateSID(map[string]string{"host": "a"})
	sid2, _ := ss.AllocateSID(map[string]string{"host": "b"})
	if sid1 != 0 || sid2 != 1 {
		t.Fatalf("expected sequential SIDs, got %d %d", sid1, sid2)
	}

	// 相同 tags 去重
	sid, _ := ss.AllocateSID(map[string]string{"host": "a"})
	if sid != 0 {
		t.Fatalf("expected dedup SID 0, got %d", sid)
	}

	tags, ok := ss.GetTagsBySID(1)
	if !ok || tags["host"] != "b" {
		t.Fatalf("expected host=b, got %v, ok=%v", tags, ok)
	}
}
```

- [ ] **Step 4: 编写 shard_index_test.go**

```go
package metadata

import (
	"testing"
)

func TestShardIndex_RegisterAndQuery(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	idx := newShardIndex(db)

	info := ShardInfo{
		ID:        "0_86400",
		StartTime: 0,
		EndTime:   86400000000000,
		DataDir:   "testdb/cpu/shards/0_86400",
		CreatedAt: 1000,
	}
	if err := idx.RegisterShard("testdb", "cpu", info); err != nil {
		t.Fatalf("RegisterShard failed: %v", err)
	}

	// 查询完全重叠
	results := idx.QueryShards("testdb", "cpu", 0, 86400000000000)
	if len(results) != 1 {
		t.Fatalf("expected 1 shard, got %d", len(results))
	}
	if results[0].ID != "0_86400" {
		t.Fatalf("expected ID 0_86400, got %s", results[0].ID)
	}

	// 查询无重叠
	results = idx.QueryShards("testdb", "cpu", 86400000000000, 172800000000000)
	if len(results) != 0 {
		t.Fatalf("expected 0 shards, got %d", len(results))
	}
}

func TestShardIndex_DuplicateRegister(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	idx := newShardIndex(db)

	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{ID: "0_86400", StartTime: 0, EndTime: 86400000000000})
	err := idx.RegisterShard("testdb", "cpu", ShardInfo{ID: "0_86400", StartTime: 0, EndTime: 86400000000000})
	if err == nil {
		t.Fatal("expected error for duplicate shard")
	}
}

func TestShardIndex_Unregister(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	idx := newShardIndex(db)

	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{ID: "0_86400", StartTime: 0, EndTime: 86400000000000})
	if err := idx.UnregisterShard("testdb", "cpu", "0_86400"); err != nil {
		t.Fatalf("UnregisterShard failed: %v", err)
	}
	if err := idx.UnregisterShard("testdb", "cpu", "nonexistent"); err == nil {
		t.Fatal("expected error for nonexistent shard")
	}

	results := idx.QueryShards("testdb", "cpu", 0, 86400000000000)
	if len(results) != 0 {
		t.Fatalf("expected 0 shards after unregister, got %d", len(results))
	}
}

func TestShardIndex_ListShards(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	idx := newShardIndex(db)

	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{ID: "86400_172800", StartTime: 86400000000000, EndTime: 172800000000000})
	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{ID: "0_86400", StartTime: 0, EndTime: 86400000000000})

	results := idx.ListShards("testdb", "cpu")
	if len(results) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(results))
	}
	if results[0].StartTime != 0 {
		t.Fatalf("expected first shard start=0, got %d", results[0].StartTime)
	}
	if results[1].StartTime != 86400000000000 {
		t.Fatalf("expected second shard start=86400..., got %d", results[1].StartTime)
	}
}

func TestShardIndex_UpdateStats(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	idx := newShardIndex(db)

	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{ID: "0_86400", StartTime: 0, EndTime: 86400000000000})
	if err := idx.UpdateShardStats("testdb", "cpu", "0_86400", 5, 1024); err != nil {
		t.Fatalf("UpdateShardStats failed: %v", err)
	}

	results := idx.ListShards("testdb", "cpu")
	if results[0].SSTableCount != 5 || results[0].TotalSize != 1024 {
		t.Fatalf("expected stats updated, got count=%d size=%d", results[0].SSTableCount, results[0].TotalSize)
	}
}

func TestShardIndex_QueryShards_Multiple(t *testing.T) {
	db, _ := openTestDB(t)
	cs := newCatalogStore(db)
	_ = cs.CreateDatabase("testdb")
	_ = cs.CreateMeasurement("testdb", "cpu")

	idx := newShardIndex(db)

	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{ID: "0_100", StartTime: 0, EndTime: 100})
	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{ID: "100_200", StartTime: 100, EndTime: 200})
	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{ID: "200_300", StartTime: 200, EndTime: 300})

	results := idx.QueryShards("testdb", "cpu", 50, 250)
	if len(results) != 3 {
		t.Fatalf("expected 3 overlapping shards, got %d", len(results))
	}
}
```

- [ ] **Step 5: 编写 manager_test.go**

```go
package metadata

import (
	"os"
	"testing"
)

func TestNewManager(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mts-mgr-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	if mgr.Catalog() == nil {
		t.Fatal("Catalog is nil")
	}
	if mgr.Series() == nil {
		t.Fatal("Series is nil")
	}
	if mgr.Shards() == nil {
		t.Fatal("Shards is nil")
	}

	// 验证 metadata.db 文件存在
	if _, err := os.Stat(tmpDir + "/metadata.db"); err != nil {
		t.Fatalf("metadata.db not found: %v", err)
	}
}

func TestManager_Load_RebuildsCache(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mts-mgr-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建第一个 Manager，写入数据
	mgr1, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	_ = mgr1.Catalog().CreateDatabase("testdb")
	_ = mgr1.Catalog().CreateMeasurement("testdb", "cpu")
	_, _ = mgr1.Series().AllocateSID("testdb", "cpu", map[string]string{"host": "s1"})
	_ = mgr1.Close()

	// 创建第二个 Manager，Load 重建缓存
	mgr2, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr2.Close()

	if err := mgr2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// 验证数据恢复
	if !mgr2.Catalog().DatabaseExists("testdb") {
		t.Fatal("expected testdb to exist after Load")
	}
	tags, ok := mgr2.Series().GetTags("testdb", "cpu", 0)
	if !ok || tags["host"] != "s1" {
		t.Fatalf("expected host=s1 after Load, got %v", tags)
	}
}

func TestManager_GetOrCreateSeriesStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mts-mgr-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mgr, _ := NewManager(tmpDir)
	defer mgr.Close()

	ss := mgr.GetOrCreateSeriesStore("testdb", "cpu")
	if ss == nil {
		t.Fatal("GetOrCreateSeriesStore returned nil")
	}

	sid, err := ss.AllocateSID(map[string]string{"host": "s1"})
	if err != nil {
		t.Fatalf("AllocateSID failed: %v", err)
	}
	if sid != 0 {
		t.Fatalf("expected SID 0, got %d", sid)
	}
}

func TestManager_Sync(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mts-mgr-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mgr, _ := NewManager(tmpDir)
	defer mgr.Close()

	if err := mgr.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

func TestManager_Close(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mts-mgr-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mgr, _ := NewManager(tmpDir)
	if err := mgr.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 重复 close 不 panic
	if err := mgr.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}
```

- [ ] **Step 6: Commit**

```bash
git add internal/storage/metadata/*_test.go internal/storage/metadata/test_helpers_test.go
git commit -m "test(metadata): 编写 bbolt 版 metadata 子系统测试"
```

---

### Task 10: 运行测试并修复编译错误

**Files:**
- All modified files

- [ ] **Step 1: 编译检查**

```bash
cd /root/projects/mts && go build ./...
```

修复所有编译错误。预期需要修复的问题：
- 未使用的 import
- `json` 未导入的文件（series_impl.go 需要 `encoding/json` import）
- `Manager.Persist()` → `Manager.Sync()` 迁移遗漏
- measurement 包的残余引用

- [ ] **Step 2: 运行 metadata 包测试**

```bash
cd /root/projects/mts && go test ./internal/storage/metadata/... -v -count=1
```

Expected: 所有测试 PASS

- [ ] **Step 3: 运行 shard 包测试**

```bash
cd /root/projects/mts && go test ./internal/storage/shard/... -v -count=1 -timeout 120s
```

Expected: 所有测试 PASS

- [ ] **Step 4: 运行 query 包测试**

```bash
cd /root/projects/mts && go test ./internal/query/... -v -count=1
```

Expected: 所有测试 PASS

- [ ] **Step 5: 运行 engine 包测试**

```bash
cd /root/projects/mts && go test ./internal/engine/... -v -count=1 -timeout 120s
```

Expected: 所有测试 PASS

- [ ] **Step 6: 运行全量测试 + race detector**

```bash
cd /root/projects/mts && go test -race ./... -count=1 -timeout 300s
```

Expected: 无竞态报告，所有测试 PASS

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "fix: 修复 bbolt 迁移后的编译错误与测试"
```

---

### Task 11: 格式化与 Lint

**Files:**
- All Go files

- [ ] **Step 1: 运行 goimports-reviser**

```bash
cd /root/projects/mts && goimports-reviser -rm-unused -format ./...
```

- [ ] **Step 2: 运行 golangci-lint**

```bash
cd /root/projects/mts && golangci-lint run ./...
```

Expected: 0 issues

- [ ] **Step 3: 确认覆盖率**

```bash
cd /root/projects/mts && go test -coverprofile=coverage.out ./internal/storage/metadata/... && go tool cover -func=coverage.out | grep total
```

Expected: metadata 包 ≥ 90%

- [ ] **Step 4: 清理临时产物**

```bash
rm -f coverage.out
find . -name '*.test' -delete
```

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "style: goimports-reviser + golangci-lint 格式化"
```

---

### Task 12: 运行 E2E 测试

**Files:**
- `tests/e2e/` (无需修改)

- [ ] **Step 1: 编译并运行各 E2E 测试**

```bash
cd /root/projects/mts/tests/e2e

for dir in compaction_test grpc_write_query integrity persistence_test retention_test wal_test simple_integrity write_1k write_10k write_100k write_1m query_1k query_10k query_100k query_1m check_fields check_schema; do
  echo "=== Running $dir ==="
  (cd "$dir" && go build -o test_bin && ./test_bin && rm -f test_bin) || echo "FAILED: $dir"
done
```

- [ ] **Step 2: 清理 E2E 构建产物**

```bash
find /root/projects/mts/tests/e2e -name 'test_bin' -delete
find /root/projects/mts/tests/e2e -type f -executable -delete 2>/dev/null
```

- [ ] **Step 3: 确认全部通过后 Commit**

```bash
git add -A && git commit -m "test: E2E 测试全部通过，确认 bbolt 元数据方案"
```

---

### Task 13: 更新 README.md

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 更新架构描述**

在 README 的架构/存储部分添加 bbolt 元数据存储的描述。

- [ ] **Step 2: Commit**

```bash
git add README.md && git commit -m "docs: 更新 README 反映 bbolt 元数据存储架构"
```
