package metadata

import (
	bolt "go.etcd.io/bbolt"
)

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
					s.cache.store(ck, tags)

					// 重建 hash 缓存
					h := tagsHash(tags)
					s.storeHashSid(string(dbName), measName, h, sid)
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
