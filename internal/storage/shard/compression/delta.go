// Package compression 提供数据压缩工具。
//
// 包含差值编码（Delta）和变长整数（Varint）编码，
// 用于减少时间戳等单调递增数据的存储空间。
package compression

// DeltaEncode 对 int64 数组进行差值编码。
//
// 参数：
//   - values: 原始数组
//
// 返回：
//   - []int64: 编码后的数组
//
// 编码规则：
//
//	deltas[0] = values[0]
//	deltas[i] = values[i] - values[i-1]
//
// 适用场景：
//
//	时间戳通常是单调递增的，差值编码后会产生很多小数值，
//	便于后续的变长整数编码进一步压缩。
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

// DeltaDecode 解码差值编码的数组。
//
// 参数：
//   - deltas: 差值编码的数组
//
// 返回：
//   - []int64: 原始数组
//
// 解码规则：
//
//	values[0] = deltas[0]
//	values[i] = values[i-1] + deltas[i]
//
// 注意：
//
// DeltaEncode 和 DeltaDecode 是一对互逆的操作。
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
