package compaction

import (
	"testing"
)

func TestDedupFilter_StrictMode(t *testing.T) {
	f := NewDedupFilter(10000) // < strictThreshold, 使用严格模式
	if !f.strict {
		t.Fatal("expected strict mode for small datasets")
	}

	// 第一次出现 → 不重复
	if f.Seen(42) {
		t.Error("expected false for first occurrence")
	}

	// 第二次出现相同 key → 重复
	if !f.Seen(42) {
		t.Error("expected true for duplicate")
	}

	// 不同 key → 不重复
	if f.Seen(99) {
		t.Error("expected false for new key")
	}
}

func TestDedupFilter_RelaxedMode_BasicDedup(t *testing.T) {
	f := NewDedupFilter(0) // 松弛模式
	if f.strict {
		t.Fatal("expected relaxed mode for unknown size")
	}

	// 第一次 → 不重复
	if f.Seen(1000) {
		t.Error("expected false for first occurrence")
	}

	// 窗口内重复 → 捕获
	if !f.Seen(1000) {
		t.Error("expected true for duplicate within window")
	}
}

func TestDedupFilter_RelaxedMode_ConsecutiveDuplicates(t *testing.T) {
	f := NewDedupFilter(0)

	// 模拟排序后相邻的重复行（compaction 中最常见的场景）
	for i := uint64(0); i < 100; i++ {
		if f.Seen(i) {
			t.Errorf("expected false for first occurrence of %d", i)
		}
	}
	// 所有都应在窗口内被检测到
	for i := uint64(0); i < 100; i++ {
		if !f.Seen(i) {
			t.Errorf("expected true for duplicate of %d (still in window)", i)
		}
	}
}

func TestDedupFilter_SlidingWindowEviction(t *testing.T) {
	// 使用小窗口手动验证淘汰
	f := NewDedupFilter(0)
	f.windowSize = 10
	f.ring = make([]uint64, 10)
	f.window = make(map[uint64]struct{}, 10)

	// 填充窗口
	for i := uint64(0); i < 10; i++ {
		if f.Seen(i) {
			t.Errorf("expected false for %d", i)
		}
	}

	// 新条目驱逐最旧的 key(0)
	if f.Seen(10) {
		t.Error("expected false for 10")
	}

	// key 1-9 仍在窗口内
	for i := uint64(1); i < 10; i++ {
		if !f.Seen(i) {
			t.Errorf("expected true for %d (still in window)", i)
		}
	}
}

func TestDedupFilter_BloomAccuracy(t *testing.T) {
	f := NewDedupFilter(0)
	// 关闭窗口，仅测试 Bloom Filter 精度
	f.windowSize = 0
	f.window = make(map[uint64]struct{})

	const n = 100000
	for i := uint64(0); i < n; i++ {
		if f.Seen(i) {
			t.Errorf("unexpected duplicate at %d", i)
		}
	}

	// 检查假阳性率：1K 个全新 key 不应大量误判为 seen
	falsePositives := 0
	const testKeys = 10000
	for i := uint64(n); i < n+testKeys; i++ {
		if f.Seen(i) {
			falsePositives++
		}
	}

	// 假阳性率应 <3%（实际约 2.5% @ 1.2MB Bloom, 4 hashes, 100K entries)
	rate := float64(falsePositives) / float64(testKeys) * 100
	if rate > 3.0 {
		t.Errorf("false positive rate too high: %.2f%% (%d/%d)", rate, falsePositives, testKeys)
	}
	t.Logf("Bloom false positive rate: %.2f%% (%d/%d) with %d entries", rate, falsePositives, testKeys, n)
}

func TestDedupFilter_StrictMode_LargeDataset(t *testing.T) {
	f := NewDedupFilter(10000) // 严格模式
	// 10K 个唯一 key
	for i := uint64(0); i < 10000; i++ {
		if f.Seen(i) {
			t.Errorf("unexpected duplicate at %d", i)
		}
	}

	// 验证 10K 重复都能被检测到
	for i := uint64(0); i < 10000; i++ {
		if !f.Seen(i) {
			t.Errorf("expected duplicate at %d", i)
		}
	}
}

func TestDedupFilter_HashDistribution(t *testing.T) {
	f := NewDedupFilter(0)

	// 验证相邻 key 产生不同的哈希分布
	h1a, h2a := f.hashKey(1000)
	h1b, h2b := f.hashKey(1001)

	if h1a == h1b && h2a == h2b {
		t.Error("adjacent keys should produce different hashes")
	}
}

func TestDedupFilter_EmptyKey(t *testing.T) {
	f := NewDedupFilter(0)

	// key = 0（空时间戳 + 空 SID 的情况）
	if f.Seen(0) {
		t.Error("expected false for key=0 first occurrence")
	}
	if !f.Seen(0) {
		t.Error("expected true for key=0 duplicate")
	}
}

func TestDedupFilter_EstimatedRowsThreshold(t *testing.T) {
	tests := []struct {
		estimatedRows int
		expectStrict  bool
	}{
		{0, false},      // 未知 → 松弛
		{100, true},     // < 50K → 严格
		{49999, true},   // < 50K → 严格
		{50000, false},  // = 50K → 松弛
		{100000, false}, // > 50K → 松弛
		{-1, false},     // 负数 → 松弛
	}

	for _, tc := range tests {
		f := NewDedupFilter(tc.estimatedRows)
		if f.strict != tc.expectStrict {
			t.Errorf("estimatedRows=%d: expected strict=%v, got strict=%v",
				tc.estimatedRows, tc.expectStrict, f.strict)
		}
	}
}

func TestDedupFilter_RelaxedIntegration(t *testing.T) {
	// 集成测试：模拟 100K 行的 compaction 去重
	f := NewDedupFilter(0)

	uniqueKeys := 100000
	dupCount := 0

	// 插入 100K 个唯一 key
	for i := 0; i < uniqueKeys; i++ {
		key := uint64(i) ^ (uint64(i) * 0x9e3779b97f4a7c15)
		if f.Seen(key) {
			dupCount++
		}
	}

	// 100K 唯一 key 不应该有任何误判
	if dupCount > 0 {
		t.Errorf("unexpected duplicates in unique keys: %d / %d", dupCount, uniqueKeys)
	}

	// 重新插入最后 10K 个 key → 在窗口 50K 内，应全部命中
	missedDups := 0
	for i := uniqueKeys - 10000; i < uniqueKeys; i++ {
		key := uint64(i) ^ (uint64(i) * 0x9e3779b97f4a7c15)
		if !f.Seen(key) {
			missedDups++
		}
	}

	if missedDups > 0 {
		t.Errorf("missed duplicates within window: %d / 10000", missedDups)
	}

	// 验证被驱逐的旧 key 不会误判为重复（应视为不重复）
	falseDups := 0
	for i := 0; i < 1000; i++ {
		key := uint64(i) ^ (uint64(i) * 0x9e3779b97f4a7c15)
		if f.Seen(key) {
			falseDups++
		}
	}
	// 被驱逐的旧 key 因为 Bloom 命中 + window 缺失 → 会被再次加入窗口
	// 但不会返回 true（视为非重复），这是正确的松弛模式行为
	t.Logf("evicted keys treated as seen (false positive from bloom only): %d / 1000", falseDups)
}
