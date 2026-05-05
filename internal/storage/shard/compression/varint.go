// Package compression 提供变长整数编码工具。
//
// Varint 编码将小数值存储在更少的字节中。
package compression

// PutVarint 将 uint64 编码为变长格式。
//
// 参数：
//   - buf: 目标缓冲区（需要足够空间，至少 10 字节）
//   - v:   要编码的值
//
// 返回：
//   - int: 实际写入的字节数
//
// 编码规则：
//
//	每个字节的最高位表示是否有后续字节。
//	低 7 位存储数据。
//	小数值（<128）只需 1 字节。
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

// Varint 从缓冲区解码变长整数。
//
// 参数：
//   - buf: 包含变长编码数据的缓冲区
//
// 返回：
//   - uint64: 解码后的值
//   - int:    消耗的字节数
//
// 错误处理：
//
//	如果缓冲区不完整（缺少终止字节），
//	返回已解码的值和缓冲区长度。
//
// 示例：
//
//	buf := make([]byte, 10)
//	n := PutVarint(buf, 300)
//	// n = 2, buf[0] = 0xAC, buf[1] = 0x02
//	v, n := Varint(buf)
//	// v = 300, n = 2
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
