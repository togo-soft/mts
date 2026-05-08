package metadata

import (
	"os"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func openSeriesDB(t *testing.T) (*bolt.DB, *seriesStore) {
	t.Helper()
	db, _ := openTestDB(t)
	ss := newSeriesStore(db)
	return db, ss
}

func setupSeriesDBMeas(t *testing.T, ss *seriesStore) {
	t.Helper()
	_ = ss.db.Update(func(tx *bolt.Tx) error {
		_, _ = ensureMeasBuckets(tx, "testdb", "cpu")
		return nil
	})
}

func TestSeriesStore_AllocateSID_New(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	sid, err := ss.AllocateSID("testdb", "cpu", map[string]string{"host": "s1"})
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid != 0 {
		t.Errorf("expected SID 0, got %d", sid)
	}
}

func TestSeriesStore_AllocateSID_SameTags(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	tags := map[string]string{"host": "s1", "region": "us"}
	sid1, _ := ss.AllocateSID("testdb", "cpu", tags)
	sid2, err := ss.AllocateSID("testdb", "cpu", tags)
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid1 != sid2 {
		t.Errorf("expected same SID %d, got %d", sid1, sid2)
	}
}

func TestSeriesStore_AllocateSID_DifferentTags(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	sid1, _ := ss.AllocateSID("testdb", "cpu", map[string]string{"host": "s1"})
	sid2, err := ss.AllocateSID("testdb", "cpu", map[string]string{"host": "s2"})
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid1 == sid2 {
		t.Error("expected different SIDs for different tags")
	}
}

func TestSeriesStore_AllocateSID_TagOrderIndependence(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	sid1, _ := ss.AllocateSID("testdb", "cpu", map[string]string{"a": "1", "b": "2"})
	sid2, _ := ss.AllocateSID("testdb", "cpu", map[string]string{"b": "2", "a": "1"})
	if sid1 != sid2 {
		t.Errorf("expected same SID for tag-order-independent, got %d vs %d", sid1, sid2)
	}
}

func TestSeriesStore_AllocateSID_EmptyTags(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	sid, err := ss.AllocateSID("testdb", "cpu", map[string]string{})
	if err != nil {
		t.Fatal("AllocateSID with empty tags failed:", err)
	}
	if sid != 0 {
		t.Errorf("expected SID 0, got %d", sid)
	}
}

func TestSeriesStore_AllocateSID_SequenceIncrement(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	sid1, _ := ss.AllocateSID("testdb", "cpu", map[string]string{"host": "a"})
	sid2, _ := ss.AllocateSID("testdb", "cpu", map[string]string{"host": "b"})
	sid3, _ := ss.AllocateSID("testdb", "cpu", map[string]string{"host": "c"})

	if sid1 != 0 || sid2 != 1 || sid3 != 2 {
		t.Errorf("expected SIDs 0,1,2 got %d,%d,%d", sid1, sid2, sid3)
	}
}

func TestSeriesStore_GetTags_CacheHit(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	tags := map[string]string{"host": "s1", "region": "us"}
	sid, _ := ss.AllocateSID("testdb", "cpu", tags)

	got, ok := ss.GetTags("testdb", "cpu", sid)
	if !ok {
		t.Fatal("GetTags returned false")
	}
	if got["host"] != "s1" || got["region"] != "us" {
		t.Errorf("unexpected tags: %v", got)
	}
}

func TestSeriesStore_GetTags_CacheCopy(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	tags := map[string]string{"host": "s1"}
	sid, _ := ss.AllocateSID("testdb", "cpu", tags)

	got, _ := ss.GetTags("testdb", "cpu", sid)
	got["host"] = "modified"

	got2, _ := ss.GetTags("testdb", "cpu", sid)
	if got2["host"] != "s1" {
		t.Error("GetTags should return a copy, not reference to cached data")
	}
}

func TestSeriesStore_GetTags_NotFound(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	_, ok := ss.GetTags("testdb", "cpu", 999)
	if ok {
		t.Error("expected false for nonexistent SID")
	}
}

func TestSeriesStore_GetTags_NonexistentMeas(t *testing.T) {
	_, ss := openSeriesDB(t)

	_, ok := ss.GetTags("testdb", "nonexistent", 0)
	if ok {
		t.Error("expected false for nonexistent measurement")
	}
}

func TestSeriesStore_GetSIDsByTag_Success(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	_, _ = ss.AllocateSID("testdb", "cpu", map[string]string{"host": "s1", "region": "us"})
	_, _ = ss.AllocateSID("testdb", "cpu", map[string]string{"host": "s2", "region": "us"})
	_, _ = ss.AllocateSID("testdb", "cpu", map[string]string{"host": "s1", "region": "eu"})

	sids := ss.GetSIDsByTag("testdb", "cpu", "host", "s1")
	if len(sids) != 2 {
		t.Errorf("expected 2 SIDs for host=s1, got %d", len(sids))
	}
}

func TestSeriesStore_GetSIDsByTag_NoMatch(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	sids := ss.GetSIDsByTag("testdb", "cpu", "host", "nonexistent")
	if len(sids) != 0 {
		t.Errorf("expected 0 SIDs, got %d", len(sids))
	}
}

func TestSeriesStore_GetSIDsByTag_NonexistentDB(t *testing.T) {
	_, ss := openSeriesDB(t)

	sids := ss.GetSIDsByTag("nonexistent", "cpu", "host", "s1")
	if len(sids) != 0 {
		t.Errorf("expected 0 SIDs, got %d", len(sids))
	}
}

func TestSeriesStore_SeriesCount_Empty(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	count := ss.SeriesCount("testdb", "cpu")
	if count != 0 {
		t.Errorf("expected 0 series, got %d", count)
	}
}

func TestSeriesStore_SeriesCount_Multiple(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	_, _ = ss.AllocateSID("testdb", "cpu", map[string]string{"host": "s1"})
	_, _ = ss.AllocateSID("testdb", "cpu", map[string]string{"host": "s2"})

	count := ss.SeriesCount("testdb", "cpu")
	if count != 2 {
		t.Errorf("expected 2 series, got %d", count)
	}
}

func TestSeriesStore_SeriesCount_NonexistentDB(t *testing.T) {
	_, ss := openSeriesDB(t)

	count := ss.SeriesCount("nonexistent", "cpu")
	if count != 0 {
		t.Errorf("expected 0 series, got %d", count)
	}
}

func TestSeriesStore_RebuildCache(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}

	catalog := m.Catalog()
	_ = catalog.CreateDatabase("testdb")
	_ = catalog.CreateMeasurement("testdb", "cpu")

	series := m.Series()
	sid, _ := series.AllocateSID("testdb", "cpu", map[string]string{"host": "s1"})
	_ = m.Close()

	// reopen and rebuild cache
	m2, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager reopen failed:", err)
	}
	defer func() { _ = m2.Close() }()
	if err := m2.Load(); err != nil {
		t.Fatal("Load failed:", err)
	}

	tags, ok := m2.Series().GetTags("testdb", "cpu", sid)
	if !ok {
		t.Fatal("GetTags after rebuild should find tags")
	}
	if tags["host"] != "s1" {
		t.Errorf("expected host=s1, got %v", tags["host"])
	}
}

func TestSeriesStore_RebuildCache_EmptyDB(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}
	_ = m.Close()

	m2, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager reopen failed:", err)
	}
	defer func() { _ = m2.Close() }()
	if err := m2.Load(); err != nil {
		t.Fatal("Load on empty db should succeed:", err)
	}
}

func TestMeasSeriesStore_AllocateSID(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	measSS := NewMeasSeriesStore(ss, "testdb", "cpu")
	sid, err := measSS.AllocateSID(map[string]string{"host": "s1"})
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid != 0 {
		t.Errorf("expected SID 0, got %d", sid)
	}
}

func TestMeasSeriesStore_GetTagsBySID(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	measSS := NewMeasSeriesStore(ss, "testdb", "cpu")
	sid, _ := measSS.AllocateSID(map[string]string{"host": "s1"})

	tags, ok := measSS.GetTagsBySID(sid)
	if !ok {
		t.Fatal("GetTagsBySID returned false")
	}
	if tags["host"] != "s1" {
		t.Errorf("expected host=s1, got %s", tags["host"])
	}
}

func TestMeasSeriesStore_GetTagsBySID_NotFound(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	measSS := NewMeasSeriesStore(ss, "testdb", "cpu")
	_, ok := measSS.GetTagsBySID(999)
	if ok {
		t.Error("expected false for nonexistent SID")
	}
}

func TestSeriesStore_EnsureMeasBuckets(t *testing.T) {
	_, ss := openSeriesDB(t)

	err := ss.db.Update(func(tx *bolt.Tx) error {
		_, err := ensureMeasBuckets(tx, "testdb", "testmeas")
		return err
	})
	if err != nil {
		t.Fatal("ensureMeasBuckets failed:", err)
	}
}

func TestSeriesStore_HelperFunctions(t *testing.T) {
	// tagsHash
	h1 := tagsHash(map[string]string{"a": "1", "b": "2"})
	h2 := tagsHash(map[string]string{"b": "2", "a": "1"})
	if h1 != h2 {
		t.Error("tagsHash should be order-independent")
	}
	if tagsHash(map[string]string{}) != 0 {
		t.Error("tagsHash of empty map should be 0")
	}

	// tagsEqual
	if !tagsEqual(map[string]string{"a": "1"}, map[string]string{"a": "1"}) {
		t.Error("tagsEqual should return true for equal maps")
	}
	if tagsEqual(map[string]string{"a": "1"}, map[string]string{"a": "2"}) {
		t.Error("tagsEqual should return false for different values")
	}
	if tagsEqual(map[string]string{"a": "1"}, map[string]string{"a": "1", "b": "2"}) {
		t.Error("tagsEqual should return false for different lengths")
	}

	// copyTags
	original := map[string]string{"x": "y"}
	copied := copyTags(original)
	copied["x"] = "z"
	if original["x"] != "y" {
		t.Error("copyTags should deep copy")
	}
	if copyTags(nil) != nil {
		t.Error("copyTags(nil) should return nil")
	}

	// encodeSIDKey / decodeSIDKey
	enc := encodeSIDKey(42)
	dec := decodeSIDKey(enc)
	if dec != 42 {
		t.Errorf("encode/decode roundtrip: expected 42, got %d", dec)
	}
	if decodeSIDKey([]byte{}) != 0 {
		t.Error("decodeSIDKey of short data should return 0")
	}

	// encodeUint64 / decodeUint64
	enc2 := encodeUint64(12345)
	dec2 := decodeUint64(enc2)
	if dec2 != 12345 {
		t.Errorf("encode/decode varint: expected 12345, got %d", dec2)
	}

	// marshalTags / unmarshalTags
	tags := map[string]string{"host": "s1", "region": "us-east"}
	data, err := marshalTags(tags)
	if err != nil {
		t.Fatal("marshalTags failed:", err)
	}
	restored, err := unmarshalTags(data)
	if err != nil {
		t.Fatal("unmarshalTags failed:", err)
	}
	if restored["host"] != "s1" || restored["region"] != "us-east" {
		t.Errorf("roundtrip failed: %v", restored)
	}
	_, err = unmarshalTags([]byte("not json"))
	if err == nil {
		t.Error("unmarshalTags should fail on invalid JSON")
	}
}

func TestSimpleSeriesStore_AllocateSID_New(t *testing.T) {
	s := NewSimpleSeriesStore()

	sid, err := s.AllocateSID(map[string]string{"host": "s1"})
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid != 0 {
		t.Errorf("expected SID 0, got %d", sid)
	}
}

func TestSimpleSeriesStore_AllocateSID_SameTags(t *testing.T) {
	s := NewSimpleSeriesStore()

	sid1, _ := s.AllocateSID(map[string]string{"host": "s1", "region": "us"})
	sid2, err := s.AllocateSID(map[string]string{"host": "s1", "region": "us"})
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid1 != sid2 {
		t.Errorf("expected same SID %d, got %d", sid1, sid2)
	}
}

func TestSimpleSeriesStore_GetTagsBySID(t *testing.T) {
	s := NewSimpleSeriesStore()

	sid, _ := s.AllocateSID(map[string]string{"host": "s1", "region": "us"})
	tags, ok := s.GetTagsBySID(sid)
	if !ok {
		t.Fatal("GetTagsBySID returned false")
	}
	if tags["host"] != "s1" {
		t.Errorf("expected host=s1, got %s", tags["host"])
	}
}

func TestSimpleSeriesStore_GetTagsBySID_NotFound(t *testing.T) {
	s := NewSimpleSeriesStore()

	_, ok := s.GetTagsBySID(999)
	if ok {
		t.Error("expected false for nonexistent SID")
	}
}

func TestSimpleSeriesStore_MultipleSIDs(t *testing.T) {
	s := NewSimpleSeriesStore()

	sid1, _ := s.AllocateSID(map[string]string{"host": "a"})
	sid2, _ := s.AllocateSID(map[string]string{"host": "b"})
	sid3, _ := s.AllocateSID(map[string]string{"host": "c"})

	tags1, _ := s.GetTagsBySID(sid1)
	tags2, _ := s.GetTagsBySID(sid2)
	tags3, _ := s.GetTagsBySID(sid3)

	if tags1["host"] != "a" || tags2["host"] != "b" || tags3["host"] != "c" {
		t.Error("tags mismatch for multiple SIDs")
	}
}

func TestSimpleSeriesStore_CopyIsolation(t *testing.T) {
	s := NewSimpleSeriesStore()

	sid, _ := s.AllocateSID(map[string]string{"host": "s1"})
	tags, _ := s.GetTagsBySID(sid)
	tags["host"] = "modified"

	tags2, _ := s.GetTagsBySID(sid)
	if tags2["host"] != "s1" {
		t.Error("GetTagsBySID should return a copy")
	}
}

func TestCatalogCreateMeasurement_BoltPersistence(t *testing.T) {
	dir := t.TempDir()
	m1, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}

	cat := m1.Catalog()
	_ = cat.CreateDatabase("testdb")
	_ = cat.CreateMeasurement("testdb", "m1")
	_ = cat.CreateMeasurement("testdb", "m2")

	_ = m1.Close()

	// Verify persistence by reopening
	m2, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager reopen failed:", err)
	}
	defer func() { _ = m2.Close() }()
	if err := m2.Load(); err != nil {
		t.Fatal("Load failed:", err)
	}

	if !m2.Catalog().DatabaseExists("testdb") {
		t.Fatal("expected testdb to exist after reopen")
	}
	if !m2.Catalog().MeasurementExists("testdb", "m1") {
		t.Error("expected m1 to exist after reopen")
	}
	if !m2.Catalog().MeasurementExists("testdb", "m2") {
		t.Error("expected m2 to exist after reopen")
	}
}

func TestSeriesStore_PersistenceAcrossSessions(t *testing.T) {
	dir := t.TempDir()
	m1, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}

	cat := m1.Catalog()
	_ = cat.CreateDatabase("testdb")
	_ = cat.CreateMeasurement("testdb", "cpu")

	sid1, _ := m1.Series().AllocateSID("testdb", "cpu", map[string]string{"host": "s1"})
	sid2, _ := m1.Series().AllocateSID("testdb", "cpu", map[string]string{"host": "s2"})

	_ = m1.Close()

	// reopen
	m2, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager reopen failed:", err)
	}
	defer func() { _ = m2.Close() }()
	if err := m2.Load(); err != nil {
		t.Fatal("Load failed:", err)
	}

	t1, ok1 := m2.Series().GetTags("testdb", "cpu", sid1)
	if !ok1 || t1["host"] != "s1" {
		t.Errorf("sid1 after reopen: ok=%v tags=%v", ok1, t1)
	}
	t2, ok2 := m2.Series().GetTags("testdb", "cpu", sid2)
	if !ok2 || t2["host"] != "s2" {
		t.Errorf("sid2 after reopen: ok=%v tags=%v", ok2, t2)
	}
}

func TestShardIndex_PersistenceAcrossSessions(t *testing.T) {
	dir := t.TempDir()
	m1, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}

	info := ShardInfo{
		ID:        "0_3600",
		StartTime: 0,
		EndTime:   3600000000000,
	}
	_ = m1.Shards().RegisterShard("testdb", "cpu", info)
	_ = m1.Close()

	// reopen
	m2, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager reopen failed:", err)
	}
	defer func() { _ = m2.Close() }()

	shards := m2.Shards().ListShards("testdb", "cpu")
	if len(shards) != 1 {
		t.Fatalf("expected 1 shard after reopen, got %d", len(shards))
	}
	if shards[0].ID != "0_3600" {
		t.Errorf("expected ID 0_3600, got %s", shards[0].ID)
	}
}

func TestSeriesStore_Persistence_CacheLoad(t *testing.T) {
	dir := t.TempDir()
	m1, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}

	cat := m1.Catalog()
	_ = cat.CreateDatabase("testdb")
	_ = cat.CreateMeasurement("testdb", "cpu")

	sid, _ := m1.Series().AllocateSID("testdb", "cpu", map[string]string{"host": "s1"})
	_, _ = m1.Series().AllocateSID("testdb", "cpu", map[string]string{"host": "s2"})

	sids := m1.Series().GetSIDsByTag("testdb", "cpu", "host", "s1")
	if len(sids) != 1 {
		t.Errorf("expected 1 SID for host=s1, got %d", len(sids))
	}

	count := m1.Series().SeriesCount("testdb", "cpu")
	if count < 2 {
		t.Errorf("expected at least 2 series, got %d", count)
	}
	_ = sid
	_ = m1.Close()
}

func TestSeriesStore_TagIndexQuery(t *testing.T) {
	_, ss := openSeriesDB(t)
	setupSeriesDBMeas(t, ss)

	_, _ = ss.AllocateSID("testdb", "cpu", map[string]string{"region": "us", "host": "a"})
	_, _ = ss.AllocateSID("testdb", "cpu", map[string]string{"region": "us", "host": "b"})
	_, _ = ss.AllocateSID("testdb", "cpu", map[string]string{"region": "eu", "host": "c"})

	usSids := ss.GetSIDsByTag("testdb", "cpu", "region", "us")
	if len(usSids) != 2 {
		t.Errorf("expected 2 SIDs for region=us, got %d", len(usSids))
	}

	euSids := ss.GetSIDsByTag("testdb", "cpu", "region", "eu")
	if len(euSids) != 1 {
		t.Errorf("expected 1 SID for region=eu, got %d", len(euSids))
	}

	noSids := ss.GetSIDsByTag("testdb", "cpu", "region", "ap")
	if len(noSids) != 0 {
		t.Errorf("expected 0 SIDs for region=ap, got %d", len(noSids))
	}
}

func TestRetentionRoundtrip(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}
	defer func() { _ = m.Close() }()

	cat := m.Catalog()
	_ = cat.CreateDatabase("db")
	_ = cat.CreateMeasurement("db", "meas")

	tests := []time.Duration{0, time.Second, time.Minute, 7 * 24 * time.Hour}
	for _, d := range tests {
		if err := cat.SetRetention("db", "meas", d); err != nil {
			t.Errorf("SetRetention(%v) failed: %v", d, err)
			continue
		}
		got, err := cat.GetRetention("db", "meas")
		if err != nil {
			t.Errorf("GetRetention failed: %v", err)
			continue
		}
		if got != d {
			t.Errorf("retention roundtrip: expected %v, got %v", d, got)
		}
	}
}

func TestManager_CloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}

	if err := m.Close(); err != nil {
		t.Fatal("first Close failed:", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal("second Close should succeed:", err)
	}
}

func TestManager_Sync(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}
	defer func() { _ = m.Close() }()

	if err := m.Sync(); err != nil {
		t.Fatal("Sync failed:", err)
	}
}

func TestManager_GetOrCreateSeriesStore(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatal("NewManager failed:", err)
	}
	defer func() { _ = m.Close() }()

	ss := m.GetOrCreateSeriesStore("db", "meas")
	if ss == nil {
		t.Fatal("expected non-nil MeasSeriesStore")
	}

	sid, err := ss.AllocateSID(map[string]string{"host": "s1"})
	if err != nil {
		t.Fatal("AllocateSID failed:", err)
	}
	if sid != 0 {
		t.Errorf("expected SID 0, got %d", sid)
	}
}

func TestSeriesStore_GetTags_BoltFallback(t *testing.T) {
	db, _ := openTestDB(t)
	ss1 := newSeriesStore(db)
	setupSeriesDBMeas(t, ss1)

	// allocate via ss1 (populates cache)
	sid, _ := ss1.AllocateSID("testdb", "cpu", map[string]string{"host": "s1"})

	// create a NEW seriesStore with same bolt.DB (no cache)
	ss2 := newSeriesStore(db)
	tags, ok := ss2.GetTags("testdb", "cpu", sid)
	if !ok {
		t.Fatal("GetTags should find tags via bolt fallback")
	}
	if tags["host"] != "s1" {
		t.Errorf("expected host=s1, got %s", tags["host"])
	}
}

func TestSeriesStore_GetTags_NotFoundBoltFallback(t *testing.T) {
	db, _ := openTestDB(t)
	ss := newSeriesStore(db)
	_ = ss.db.Update(func(tx *bolt.Tx) error {
		_, _ = ensureMeasBuckets(tx, "testdb", "cpu")
		return nil
	})

	_, ok := ss.GetTags("testdb", "cpu", 999)
	if ok {
		t.Error("expected false for nonexistent SID via bolt fallback")
	}
}

func TestSeriesStore_GetTags_NoDB(t *testing.T) {
	db, _ := openTestDB(t)
	ss := newSeriesStore(db)
	_, ok := ss.GetTags("nonexistent", "cpu", 0)
	if ok {
		t.Error("expected false for nonexistent db")
	}
}

func TestSeriesStore_GetTags_NoMeasurement(t *testing.T) {
	db, _ := openTestDB(t)
	ss := newSeriesStore(db)
	_ = ss.db.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte("testdb"))
		return nil
	})
	_, ok := ss.GetTags("testdb", "nonexistent", 0)
	if ok {
		t.Error("expected false for nonexistent measurement")
	}
}

func TestSeriesStore_GetTags_NoSeriesBucket(t *testing.T) {
	db, _ := openTestDB(t)
	ss := newSeriesStore(db)
	_ = ss.db.Update(func(tx *bolt.Tx) error {
		dbBucket, _ := tx.CreateBucketIfNotExists([]byte("testdb"))
		_, _ = dbBucket.CreateBucketIfNotExists([]byte("cpu"))
		return nil
	})
	_, ok := ss.GetTags("testdb", "cpu", 0)
	if ok {
		t.Error("expected false when no series bucket")
	}
}

func TestSeriesStore_GetSIDsByTag_NoBucketPaths(t *testing.T) {
	db, _ := openTestDB(t)
	ss := newSeriesStore(db)

	// test all the nil-path returns in GetSIDsByTag
	sids := ss.GetSIDsByTag("nonexistent", "cpu", "k", "v")
	if len(sids) != 0 {
		t.Errorf("expected 0 for nonexistent db, got %d", len(sids))
	}
}

func TestSeriesStore_SeriesCount_NoBuckets(t *testing.T) {
	db, _ := openTestDB(t)
	ss := newSeriesStore(db)

	count := ss.SeriesCount("nonexistent", "cpu")
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}
}

func TestNewManager_InvalidDir(t *testing.T) {
	// create a file where a directory is expected
	f, _ := os.CreateTemp("", "mts_test_*")
	fPath := f.Name()
	_ = f.Close()
	defer func() { _ = os.Remove(fPath) }()

	_, err := NewManager(fPath + "/subdir")
	if err == nil {
		t.Error("expected error when data dir cannot be created")
	}
}

func TestSeriesStore_SeriesCount_NoMeasBucket(t *testing.T) {
	db, _ := openTestDB(t)
	ss := newSeriesStore(db)
	_ = ss.db.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte("testdb"))
		return nil
	})

	count := ss.SeriesCount("testdb", "cpu")
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}
}

func TestSeriesStore_SeriesCount_NoSeriesBucket(t *testing.T) {
	db, _ := openTestDB(t)
	ss := newSeriesStore(db)
	_ = ss.db.Update(func(tx *bolt.Tx) error {
		dbBucket, _ := tx.CreateBucketIfNotExists([]byte("testdb"))
		_, _ = dbBucket.CreateBucketIfNotExists([]byte("cpu"))
		return nil
	})

	count := ss.SeriesCount("testdb", "cpu")
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}
}

func TestShardIndex_UnregisterShard_BeforeBucket(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	_ = idx.RegisterShard("testdb", "cpu", ShardInfo{ID: "x", StartTime: 0, EndTime: 100})
	// create measurement without shards bucket
	_ = db.Update(func(tx *bolt.Tx) error {
		_, _ = ensureMeasBuckets(tx, "testdb", "cpu2")
		return nil
	})

	err := idx.UnregisterShard("testdb", "cpu2", "x")
	if err == nil {
		t.Error("expected error when shards bucket doesn't exist")
	}
}

func TestShardIndex_UpdateShardStats_NoShardsBucket(t *testing.T) {
	db, _ := openTestDB(t)
	idx := newShardIndex(db)

	_ = db.Update(func(tx *bolt.Tx) error {
		_, _ = ensureMeasBuckets(tx, "testdb", "cpu")
		return nil
	})

	err := idx.UpdateShardStats("testdb", "cpu", "x", 0, 0)
	if err == nil {
		t.Error("expected error when shards bucket doesn't exist")
	}
}
