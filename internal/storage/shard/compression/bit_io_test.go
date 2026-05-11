package compression

import (
	"testing"
)

func TestBitWriter_WriteBit(t *testing.T) {
	w := NewBitWriter(4)
	for i := 0; i < 8; i++ {
		w.WriteBit(1)
	}
	b := w.Bytes()
	if len(b) != 1 || b[0] != 0xFF {
		t.Errorf("expected [0xFF], got %v", b)
	}

	w = NewBitWriter(4)
	for i := 0; i < 8; i++ {
		w.WriteBit(0)
	}
	b = w.Bytes()
	if len(b) != 1 || b[0] != 0x00 {
		t.Errorf("expected [0x00], got %v", b)
	}
}

func TestBitWriter_WriteBits(t *testing.T) {
	w := NewBitWriter(4)
	w.WriteBits(0xA5, 8)
	if b := w.Bytes(); len(b) != 1 || b[0] != 0xA5 {
		t.Errorf("expected [0xA5], got %v", b)
	}

	w = NewBitWriter(4)
	w.WriteBits(0x0F, 4)
	w.WriteBits(0x0F, 4)
	if b := w.Bytes(); len(b) != 1 || b[0] != 0xFF {
		t.Errorf("expected [0xFF], got %v", b)
	}
}

func TestBitWriter_BitLen(t *testing.T) {
	w := NewBitWriter(4)
	w.WriteBits(0, 3)
	if w.BitLen() != 3 {
		t.Errorf("expected bitLen=3, got %d", w.BitLen())
	}
	w.WriteBits(0, 6)
	if w.BitLen() != 9 {
		t.Errorf("expected bitLen=9, got %d", w.BitLen())
	}
}

func TestBitWriter_Flush(t *testing.T) {
	w := NewBitWriter(4)
	w.WriteBits(0, 3)
	if w.BitLen() != 3 {
		t.Errorf("expected bitLen=3, got %d", w.BitLen())
	}
	w.Flush()
	// Flush 后 bitPos 归零，等同补齐到 8 位
	if w.BitLen() != 8 {
		t.Errorf("expected bitLen=8 after flush, got %d", w.BitLen())
	}
	if len(w.Bytes()) != 1 {
		t.Errorf("expected 1 byte after flush, got %d", len(w.Bytes()))
	}
}

func TestBitReader_ReadBit(t *testing.T) {
	r := NewBitReader([]byte{0xA5}) // 1010 0101
	expected := []uint8{1, 0, 1, 0, 0, 1, 0, 1}
	for i, exp := range expected {
		bit, err := r.ReadBit()
		if err != nil {
			t.Fatalf("readBit[%d] error: %v", i, err)
		}
		if bit != exp {
			t.Errorf("bit[%d]: expected %d, got %d", i, exp, bit)
		}
	}
}

func TestBitReader_ReadBits(t *testing.T) {
	r := NewBitReader([]byte{0xAB, 0xCD})
	v, err := r.ReadBits(4)
	if err != nil {
		t.Fatalf("readBits(4) error: %v", err)
	}
	if v != 0xA {
		t.Errorf("expected 0xA, got 0x%X", v)
	}

	v, err = r.ReadBits(8)
	if err != nil {
		t.Fatalf("readBits(8) error: %v", err)
	}
	if v != 0xBC {
		t.Errorf("expected 0xBC, got 0x%X", v)
	}

	v, err = r.ReadBits(4)
	if err != nil {
		t.Fatalf("readBits(4) error: %v", err)
	}
	if v != 0xD {
		t.Errorf("expected 0xD, got 0x%X", v)
	}
}

func TestBitReader_RemainingBits(t *testing.T) {
	r := NewBitReader([]byte{0x00, 0x00})
	if r.RemainingBits() != 16 {
		t.Errorf("expected 16 remaining bits, got %d", r.RemainingBits())
	}
	_, _ = r.ReadBits(5)
	if r.RemainingBits() != 11 {
		t.Errorf("expected 11 remaining bits, got %d", r.RemainingBits())
	}
}

func TestBitWriter_BitReader_RoundTrip(t *testing.T) {
	// 写入混合数据
	w := NewBitWriter(32)
	w.WriteBit(1)
	w.WriteBits(0x3, 2)
	w.WriteBit(0)
	w.WriteBits(0x55, 8)
	w.WriteBits(0x0F0F, 16)
	w.Flush()

	// 读取
	r := NewBitReader(w.Bytes())
	bit, _ := r.ReadBit()
	if bit != 1 {
		t.Errorf("bit0: expected 1, got %d", bit)
	}
	v, _ := r.ReadBits(2)
	if v != 0x3 {
		t.Errorf("bits[1:3]: expected 0x3, got 0x%X", v)
	}
	bit, _ = r.ReadBit()
	if bit != 0 {
		t.Errorf("bit3: expected 0, got %d", bit)
	}
	v, _ = r.ReadBits(8)
	if v != 0x55 {
		t.Errorf("bits[4:12]: expected 0x55, got 0x%X", v)
	}
	v, _ = r.ReadBits(16)
	if v != 0x0F0F {
		t.Errorf("bits[12:28]: expected 0x0F0F, got 0x%X", v)
	}
}

func TestBitReader_EOF(t *testing.T) {
	r := NewBitReader([]byte{0xFF})
	for i := 0; i < 8; i++ {
		_, err := r.ReadBit()
		if err != nil {
			t.Fatalf("readBit[%d] unexpected error: %v", i, err)
		}
	}
	_, err := r.ReadBit()
	if err == nil {
		t.Error("expected error on read past EOF")
	}
}
