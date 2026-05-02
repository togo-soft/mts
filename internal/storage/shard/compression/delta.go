// internal/storage/shard/compression/delta.go
package compression

// DeltaEncode 计算差值编码
func DeltaEncode(values []int64) []int64 {
	if len(values) == 0 {
		return nil
	}
	deltas := make([]int64, len(values))
	deltas[0] = values[0]
	for i := 1; i < len(values); i++ {
		deltas[i] = values[i] - values[i-1]
	}
	return deltas
}

// DeltaDecode 差值解码
func DeltaDecode(deltas []int64) []int64 {
	if len(deltas) == 0 {
		return nil
	}
	values := make([]int64, len(deltas))
	values[0] = deltas[0]
	for i := 1; i < len(deltas); i++ {
		values[i] = values[i-1] + deltas[i]
	}
	return values
}
