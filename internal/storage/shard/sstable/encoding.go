package sstable

// EncodingType 段编码类型。
type EncodingType uint8

const (
	EncodingRaw          EncodingType = 0 // 原始编码
	EncodingDeltaVarint  EncodingType = 1 // Delta-of-Delta + Varint (时间戳)
	EncodingVarint       EncodingType = 2 // Varint (SID)
	EncodingZigZagVarint EncodingType = 3 // ZigZag + Varint (int64)
	EncodingXORFloat     EncodingType = 4 // XOR 浮点 (float64)
	EncodingDictString   EncodingType = 5 // 字典编码 (string)
	EncodingBitmapBool   EncodingType = 6 // 位图 (bool)
)
