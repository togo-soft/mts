// internal/storage/shard/compression/varint.go
package compression

// PutVarint 编码 varint，返回写入的字节数
func PutVarint(buf []byte, v uint64) int {
	i := 0
	for v >= 0x80 {
		buf[i] = byte(v&0x7F) | 0x80
		v >>= 7
		i++
	}
	buf[i] = byte(v)
	return i + 1
}

// Varint 解码 varint
func Varint(buf []byte) (uint64, int) {
	var shift uint
	var v uint64
	for i, b := range buf {
		v |= uint64(b&0x7F) << shift
		if b < 0x80 {
			return v, i + 1
		}
		shift += 7
	}
	return v, len(buf)
}
