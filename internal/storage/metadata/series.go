package metadata

// SeriesStore 管理 Series ID 分配和标签索引。
type SeriesStore interface {
	AllocateSID(database, measurement string, tags map[string]string) (uint64, error)
	GetTags(database, measurement string, sid uint64) (map[string]string, bool)
	GetSIDsByTag(database, measurement string, tagKey, tagValue string) []uint64
	SeriesCount(database, measurement string) int
}
