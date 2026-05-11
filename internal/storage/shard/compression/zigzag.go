package compression

// ZigZagEncode 将有符号 int64 编码为无符号 uint64。
// 映射: zigzag(n) = uint64(n<<1) ^ uint64(n>>63)
func ZigZagEncode(values []int64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	encoded := make([]uint64, len(values))
	for i, v := range values {
		encoded[i] = uint64(v<<1) ^ uint64(v>>63)
	}
	return encoded
}

// ZigZagDecode 将无符号 uint64 解码为有符号 int64。
// 映射: unzigzag(m) = int64(m>>1) ^ -int64(m&1)
func ZigZagDecode(encoded []uint64) []int64 {
	if len(encoded) == 0 {
		return nil
	}
	values := make([]int64, len(encoded))
	for i, m := range encoded {
		values[i] = int64(m>>1) ^ -int64(m&1)
	}
	return values
}
