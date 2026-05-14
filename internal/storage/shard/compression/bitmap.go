package compression

// BitmapEncode 对 bool 序列进行位图编码，MSB first。
//
// 编码格式:
//
//	[bits: ceil(count/8) bytes]
//
// 字节 j 的第 k bit (MSB first) 对应行号 (j*8 + k):
//
//	1 = true, 0 = false
func BitmapEncode(values []bool) []byte {
	if len(values) == 0 {
		return nil
	}

	byteCount := (len(values) + 7) / 8
	buf := make([]byte, byteCount)

	for i, v := range values {
		if v {
			byteIdx := i / 8
			bitIdx := i % 8
			buf[byteIdx] |= 1 << (7 - bitIdx)
		}
	}

	return buf
}

// BitmapDecode 解码位图数据。
func BitmapDecode(data []byte, count int) []bool {
	if count == 0 || len(data) == 0 {
		return make([]bool, count)
	}

	values := make([]bool, count)
	for i := range count {
		byteIdx := i / 8
		bitIdx := i % 8
		if byteIdx < len(data) {
			values[i] = (data[byteIdx]>>(7-bitIdx))&1 != 0
		}
	}

	return values
}
