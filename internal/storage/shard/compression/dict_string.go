package compression

import (
	"encoding/binary"
)

// DictEncode 对字符串序列进行字典编码。
//
// 编码格式:
//
//	[dict_size: 2B BigEndian]
//	[dict_entries...]  每个: [str_len: 2B BigEndian][data]
//	[indices...]       每个: [varint]
func DictEncode(values []string) []byte {
	if len(values) == 0 {
		return nil
	}

	// 构建字典
	dict := make(map[string]int)
	unique := make([]string, 0)
	for _, v := range values {
		if _, ok := dict[v]; !ok {
			dict[v] = len(unique)
			unique = append(unique, v)
		}
	}

	// 预估大小
	estSize := 2 // dict size
	for _, s := range unique {
		estSize += 2 + len(s) // str_len + data
	}
	for _, v := range values {
		estSize += varintSize(uint64(dict[v]))
	}

	// 若字典编码后大于原始编码，回退
	rawSize := 0
	for _, v := range values {
		rawSize += 2 + len(v)
	}
	if estSize >= rawSize {
		return nil // 回退信号
	}

	// 构建输出
	buf := make([]byte, 0, estSize)

	// dict_size
	var sizeBuf [2]byte
	binary.BigEndian.PutUint16(sizeBuf[:], uint16(len(unique)))
	buf = append(buf, sizeBuf[:]...)

	// dict entries
	for _, s := range unique {
		binary.BigEndian.PutUint16(sizeBuf[:], uint16(len(s)))
		buf = append(buf, sizeBuf[:]...)
		buf = append(buf, s...)
	}

	// indices
	var varintBuf [10]byte
	for _, v := range values {
		idx := uint64(dict[v])
		n := PutVarint(varintBuf[:], idx)
		buf = append(buf, varintBuf[:n]...)
	}

	return buf
}

// DictDecode 解码字典编码的数据。
func DictDecode(data []byte, count int) ([]string, error) {
	if count == 0 {
		return nil, nil
	}
	if len(data) < 2 {
		return nil, ioError("dict data too short")
	}

	pos := 0
	dictSize := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2

	// 读取字典
	dict := make([]string, dictSize)
	for i := 0; i < dictSize; i++ {
		if pos+2 > len(data) {
			return nil, ioError("dict entry truncated")
		}
		strLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+strLen > len(data) {
			return nil, ioError("dict str data truncated")
		}
		dict[i] = string(data[pos : pos+strLen])
		pos += strLen
	}

	// 读取索引
	values := make([]string, count)
	for i := 0; i < count; i++ {
		if pos >= len(data) {
			return nil, ioError("dict index truncated")
		}
		idx, n := Varint(data[pos:])
		pos += n
		if int(idx) >= dictSize {
			return nil, ioError("dict index out of range")
		}
		values[i] = dict[idx]
	}

	return values, nil
}

// ShouldUseDict 预估编码是否有收益。
func ShouldUseDict(values []string) bool {
	if len(values) == 0 {
		return false
	}
	dict := make(map[string]struct{})
	estSize := 2
	for _, v := range values {
		if _, ok := dict[v]; !ok {
			dict[v] = struct{}{}
			estSize += 2 + len(v)
		}
		estSize += varintSize(uint64(len(dict) - 1))
	}
	rawSize := 0
	for _, v := range values {
		rawSize += 2 + len(v)
	}
	return estSize < rawSize
}

// varintSize 估计 Varint 编码所需的字节数。
func varintSize(v uint64) int {
	n := 1
	for v >= 0x80 {
		n++
		v >>= 7
	}
	return n
}

func ioError(msg string) error {
	return &strError{msg: msg}
}

type strError struct{ msg string }

func (e *strError) Error() string { return e.msg }
