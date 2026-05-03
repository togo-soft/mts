package measurement

import "sort"

// tagsHash 计算 tags 的哈希值（与顺序无关）
func tagsHash(tags map[string]string) uint64 {
	if len(tags) == 0 {
		return 0
	}

	// Get sorted keys for order-independent hashing
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var h uint64 = 14695981039346656037 // FNV offset basis
	for _, k := range keys {
		v := tags[k]
		// Use FNV-1a hash combining key and value content
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

// tagsEqual 比较两个 tags 是否相等（与顺序无关）
func tagsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	// Check a subset of b
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	// Check b subset of a (redundant but explicit)
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}
