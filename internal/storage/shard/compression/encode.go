package compression

import (
	"encoding/binary"
	"fmt"
	"math"
)

// EncodeTimestamps 对 int64 时间戳进行 Delta-of-Delta + Varint 编码。
func EncodeTimestamps(values []int64) []byte {
	if len(values) == 0 {
		return nil
	}

	buf := make([]byte, 0, len(values)*4)
	var tmp [10]byte

	// base
	n := PutVarint(tmp[:], uint64(values[0]))
	buf = append(buf, tmp[:n]...)

	if len(values) == 1 {
		return buf
	}

	// delta1
	delta1 := values[1] - values[0]
	n = PutVarint(tmp[:], uint64(delta1))
	buf = append(buf, tmp[:n]...)

	// delta-of-delta
	for i := 2; i < len(values); i++ {
		dod := (values[i] - values[i-1]) - (values[i-1] - values[i-2])
		n = PutVarint(tmp[:], uint64(dod))
		buf = append(buf, tmp[:n]...)
	}

	return buf
}

// DecodeTimestamps 解码 Delta-of-Delta + Varint 编码的时间戳。
func DecodeTimestamps(data []byte, count int) ([]int64, error) {
	if count == 0 {
		return nil, nil
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("decode timestamps: empty data")
	}

	pos := 0
	values := make([]int64, 0, count)

	// base
	v, n := Varint(data[pos:])
	pos += n
	values = append(values, int64(v))

	if count == 1 {
		return values, nil
	}
	if pos >= len(data) {
		return nil, fmt.Errorf("decode timestamps: truncated at delta1")
	}

	// delta1
	v, n = Varint(data[pos:])
	pos += n
	values = append(values, values[0]+int64(v))

	// delta-of-delta
	for i := 2; i < count; i++ {
		if pos >= len(data) {
			return nil, fmt.Errorf("decode timestamps: truncated at dod[%d]", i)
		}
		v, n = Varint(data[pos:])
		pos += n
		next := 2*values[i-1] - values[i-2] + int64(v)
		values = append(values, next)
	}

	return values, nil
}

// EncodeSids 对 uint64 SID 进行 Varint 编码。
func EncodeSids(values []uint64) []byte {
	if len(values) == 0 {
		return nil
	}

	buf := make([]byte, 0, len(values)*4)
	var tmp [10]byte
	for _, v := range values {
		n := PutVarint(tmp[:], v)
		buf = append(buf, tmp[:n]...)
	}
	return buf
}

// DecodeSids 解码 Varint 编码的 SID。
func DecodeSids(data []byte, count int) ([]uint64, error) {
	if count == 0 {
		return nil, nil
	}

	pos := 0
	values := make([]uint64, 0, count)
	for i := 0; i < count; i++ {
		if pos >= len(data) {
			return nil, fmt.Errorf("decode sids: truncated at %d", i)
		}
		v, n := Varint(data[pos:])
		pos += n
		values = append(values, v)
	}
	return values, nil
}

// EncodeInt64Values 对 int64 序列进行 ZigZag + Varint 编码。
func EncodeInt64Values(values []int64) []byte {
	if len(values) == 0 {
		return nil
	}

	encoded := ZigZagEncode(values)
	buf := make([]byte, 0, len(values)*4)
	var tmp [10]byte
	for _, v := range encoded {
		n := PutVarint(tmp[:], v)
		buf = append(buf, tmp[:n]...)
	}
	return buf
}

// DecodeInt64Values 解码 ZigZag + Varint 编码的 int64。
func DecodeInt64Values(data []byte, count int) ([]int64, error) {
	if count == 0 {
		return nil, nil
	}

	pos := 0
	encoded := make([]uint64, count)
	for i := 0; i < count; i++ {
		if pos >= len(data) {
			return nil, fmt.Errorf("decode int64: truncated at %d", i)
		}
		v, n := Varint(data[pos:])
		pos += n
		encoded[i] = v
	}
	return ZigZagDecode(encoded), nil
}

// EncodeFloat64Values 对 float64 序列进行 XOR 编码。
func EncodeFloat64Values(values []float64) []byte {
	return XorFloatEncode(values)
}

// DecodeFloat64Values 解码 XOR 编码的 float64。
func DecodeFloat64Values(data []byte, count int) ([]float64, error) {
	return XorFloatDecode(data, count)
}

// EncodeStringValues 对 string 序列进行字典编码，失败回退原始编码。
func EncodeStringValues(values []string) ([]byte, bool) {
	encoded := DictEncode(values)
	if encoded != nil {
		return encoded, true
	}
	// 回退为原始编码
	return EncodeStringValuesRaw(values), false
}

// DecodeStringValues 解码字典编码或原始编码的 string。
func DecodeStringValues(data []byte, count int, isDict bool) ([]string, error) {
	if isDict {
		return DictDecode(data, count)
	}
	return decodeStringRaw(data, count)
}

// EncodeBoolValues 对 bool 序列进行位图编码。
func EncodeBoolValues(values []bool) []byte {
	return BitmapEncode(values)
}

// DecodeBoolValues 解码位图编码的 bool。
func DecodeBoolValues(data []byte, count int) []bool {
	return BitmapDecode(data, count)
}

// EncodeStringValuesRaw 原始字符串编码: [str_len:4B BigEndian][data]...
func EncodeStringValuesRaw(values []string) []byte {
	if len(values) == 0 {
		return nil
	}
	size := 0
	for _, v := range values {
		size += 4 + len(v)
	}
	buf := make([]byte, 0, size)
	var lenBuf [4]byte
	for _, v := range values {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(v)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, v...)
	}
	return buf
}

// decodeStringRaw 解码原始字符串编码。
func decodeStringRaw(data []byte, count int) ([]string, error) {
	if count == 0 {
		return nil, nil
	}
	values := make([]string, 0, count)
	pos := 0
	for i := 0; i < count; i++ {
		if pos+4 > len(data) {
			return nil, fmt.Errorf("decode string raw: truncated at %d", i)
		}
		strLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+strLen > len(data) {
			return nil, fmt.Errorf("decode string raw: str data truncated at %d", i)
		}
		values = append(values, string(data[pos:pos+strLen]))
		pos += strLen
	}
	return values, nil
}

// extractInt64Data 从原始 8B 定长格式中提取 int64 序列。
func ExtractInt64Data(data []byte, count int) []int64 {
	values := make([]int64, count)
	for i := 0; i < count && i*8+8 <= len(data); i++ {
		values[i] = int64(binary.BigEndian.Uint64(data[i*8:]))
	}
	return values
}

// extractUint64Data 从原始 8B 定长格式中提取 uint64 序列。
func ExtractUint64Data(data []byte, count int) []uint64 {
	values := make([]uint64, count)
	for i := 0; i < count && i*8+8 <= len(data); i++ {
		values[i] = binary.BigEndian.Uint64(data[i*8:])
	}
	return values
}

// extractFloat64Data 从原始 8B 定长格式中提取 float64 序列。
func ExtractFloat64Data(data []byte, count int) []float64 {
	values := make([]float64, count)
	for i := 0; i < count && i*8+8 <= len(data); i++ {
		values[i] = math.Float64frombits(binary.BigEndian.Uint64(data[i*8:]))
	}
	return values
}

// extractBoolData 从原始 1B/行 格式中提取 bool 序列。
func ExtractBoolData(data []byte, count int) []bool {
	values := make([]bool, count)
	for i := 0; i < count && i < len(data); i++ {
		values[i] = data[i] != 0
	}
	return values
}

// extractStringData 从原始格式 [4B len BigEndian][data] 中提取 string 序列。
func ExtractStringData(data []byte, count int) []string {
	values := make([]string, 0, count)
	pos := 0
	for i := 0; i < count && pos+4 <= len(data); i++ {
		strLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+strLen > len(data) {
			break
		}
		values = append(values, string(data[pos:pos+strLen]))
		pos += strLen
	}
	return values
}
