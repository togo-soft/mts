// internal/storage/measurement/meas_meta_bench_test.go
package measurement

import (
	"fmt"
	"testing"
)

// BenchmarkAllocateSID 测试 SID 分配性能
// 每次写入都需要分配/查找 SID，1000 tps 场景
func BenchmarkAllocateSID(b *testing.B) {
	store := NewMeasurementMetaStore()

	tags := map[string]string{
		"host":   "server1",
		"region": "us-west",
		"env":    "prod",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 每次稍微不同的 tags 以模拟新 series
		tags["host"] = fmt.Sprintf("server%d", i%100)
		_, _ = store.AllocateSID(tags)
	}
}

// BenchmarkAllocateSIDParallel 测试并发 SID 分配
func BenchmarkAllocateSIDParallel(b *testing.B) {
	store := NewMeasurementMetaStore()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tags := map[string]string{
				"host":   fmt.Sprintf("server%d", i%100),
				"region": "us-west",
				"env":    "prod",
			}
			_, _ = store.AllocateSID(tags)
			i++
		}
	})
}

// BenchmarkGetSidsByTag 测试 Tag 索引查询性能
// 查询时需要频繁使用
func BenchmarkGetSidsByTag(b *testing.B) {
	store := NewMeasurementMetaStore()

	// 预填充 1000 个 series
	for i := 0; i < 1000; i++ {
		tags := map[string]string{
			"host":   fmt.Sprintf("server%d", i),
			"region": "us-west",
			"env":    "prod",
		}
		_, _ = store.AllocateSID(tags)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		store.GetSidsByTag("region", "us-west")
	}
}

// BenchmarkTagsHash 测试 tags 哈希计算性能
func BenchmarkTagsHash(b *testing.B) {
	tags := map[string]string{
		"host":   "server1",
		"region": "us-west",
		"env":    "prod",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = tagsHash(tags)
	}
}
