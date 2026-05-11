package compression

import (
	"math"
	"math/bits"
)

// XorFloatEncode 对 float64 序列进行 Gorilla 风格 XOR 编码。
//
// 编码格式:
//
//	[first: 8B IEEE 754 BigEndian]
//	[control bits...]
//
// 对后续值 (i>=1):
//
//	若 xor==0: 写入 1 bit '0'
//	否则: 写入 1 bit '1' + leading(6b) + meaningful-1(6b) + meaningful_bits
func XorFloatEncode(values []float64) []byte {
	if len(values) == 0 {
		return nil
	}

	w := NewBitWriter(len(values) * 8)
	first := math.Float64bits(values[0])
	w.WriteBits(first, 64)

	prev := first
	for i := 1; i < len(values); i++ {
		curr := math.Float64bits(values[i])
		xor := prev ^ curr

		if xor == 0 {
			w.WriteBit(0)
		} else {
			w.WriteBit(1)
			leading := bits.LeadingZeros64(xor)
			trailing := bits.TrailingZeros64(xor)
			meaningful := 64 - leading - trailing

			// leading_zeros: 6 bits (0-63), 64 映射为 63 且 meaningful=0
			lz := uint64(leading)
			if lz == 64 {
				lz = 63
				meaningful = 0
			}
			w.WriteBits(lz, 6)

			// meaningful bits count - 1, stored as 6 bits (0-63)
			mb := uint64(0)
			if meaningful > 0 {
				mb = uint64(meaningful - 1)
			}
			w.WriteBits(mb, 6)

			// meaningful bits 数据
			if meaningful > 0 {
				val := xor >> trailing
				w.WriteBits(val, meaningful)
			}
		}
		prev = curr
	}

	w.Flush()
	return w.Bytes()
}

// XorFloatDecode 解码 XorFloatEncode 的输出。
func XorFloatDecode(data []byte, count int) ([]float64, error) {
	if count == 0 {
		return nil, nil
	}

	r := NewBitReader(data)

	first, err := r.ReadBits(64)
	if err != nil {
		return nil, err
	}

	values := make([]float64, 0, count)
	values = append(values, math.Float64frombits(first))
	prev := first

	for i := 1; i < count; i++ {
		bit, err := r.ReadBit()
		if err != nil {
			return nil, err
		}

		var curr uint64
		if bit == 0 {
			curr = prev
		} else {
			lz, err := r.ReadBits(6)
			if err != nil {
				return nil, err
			}

			mb, err := r.ReadBits(6)
			if err != nil {
				return nil, err
			}
			meaningful := int(mb) + 1

			leading := int(lz)
			if leading == 63 {
				leading = 64
				meaningful = 0
			}

			var xor uint64
			if meaningful > 0 {
				xor, err = r.ReadBits(meaningful)
				if err != nil {
					return nil, err
				}
			}

			trailing := 64 - leading - meaningful
			xor <<= trailing
			curr = prev ^ xor
		}

		values = append(values, math.Float64frombits(curr))
		prev = curr
	}

	return values, nil
}
