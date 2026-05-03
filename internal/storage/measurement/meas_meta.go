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
		h ^= uint64(len(k))
		h *= 1099511628211
		h ^= uint64(len(v))
		h *= 1099511628211
	}
	return h
}

// tagsEqual 比较两个 tags 是否相等（与顺序无关）
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
