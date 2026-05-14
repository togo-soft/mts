package compression

import "fmt"

// BitWriter 支持按位写入，MSB first。
type BitWriter struct {
	buf    []byte
	pos    int
	bitPos uint8
}

// NewBitWriter 创建 BitWriter。
func NewBitWriter(capacity int) *BitWriter {
	if capacity <= 0 {
		capacity = 256
	}
	return &BitWriter{buf: make([]byte, 0, capacity)}
}

// WriteBit 写入 1 个 bit。
func (w *BitWriter) WriteBit(v uint8) {
	if w.pos >= len(w.buf) {
		w.buf = append(w.buf, 0)
	}
	if v&1 != 0 {
		w.buf[w.pos] |= 1 << (7 - w.bitPos)
	}
	w.bitPos++
	if w.bitPos == 8 {
		w.pos++
		w.bitPos = 0
	}
}

// WriteBits 写入 n 个低位 bit，MSB first。
func (w *BitWriter) WriteBits(v uint64, n int) {
	for i := n - 1; i >= 0; i-- {
		w.WriteBit(uint8((v >> i) & 1))
	}
}

// Bytes 返回已写入的字节（包含未满字节的当前状态）。
func (w *BitWriter) Bytes() []byte {
	return w.buf
}

// BitLen 返回已写入的 bit 数。
func (w *BitWriter) BitLen() int {
	return w.pos*8 + int(w.bitPos)
}

// Flush 补齐当前字节到 8 位边界。
func (w *BitWriter) Flush() {
	if w.bitPos > 0 {
		w.pos++
		w.bitPos = 0
	}
}

// BitReader 支持按位读取，MSB first。
type BitReader struct {
	data   []byte
	pos    int
	bitPos uint8
}

// NewBitReader 创建 BitReader。
func NewBitReader(data []byte) *BitReader {
	return &BitReader{data: data}
}

// ReadBit 读取 1 个 bit。
func (r *BitReader) ReadBit() (uint8, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("bit reader: unexpected EOF at byte %d", r.pos)
	}
	bit := (r.data[r.pos] >> (7 - r.bitPos)) & 1
	r.bitPos++
	if r.bitPos == 8 {
		r.pos++
		r.bitPos = 0
	}
	return bit, nil
}

// ReadBits 读取 n 个 bit，MSB first。
func (r *BitReader) ReadBits(n int) (uint64, error) {
	var v uint64
	for range n {
		bit, err := r.ReadBit()
		if err != nil {
			return v, err
		}
		v = (v << 1) | uint64(bit)
	}
	return v, nil
}

// BitLen 返回已读取的 bit 数。
func (r *BitReader) BitLen() int {
	return r.pos*8 + int(r.bitPos)
}

// RemainingBits 返回剩余可读 bit 数。
func (r *BitReader) RemainingBits() int {
	return len(r.data)*8 - r.BitLen()
}
