package wal

import (
	"testing"
)

func TestEncodeDecodeSegmentHeader(t *testing.T) {
	var buf [segmentHeaderSize]byte
	encodeSegmentHeader(buf[:], 42, 0)

	version, segNum, _, err := decodeSegmentHeader(buf[:])
	if err != nil {
		t.Fatalf("decodeSegmentHeader: %v", err)
	}
	if version != currentVersion {
		t.Errorf("expected version %d, got %d", currentVersion, version)
	}
	if segNum != 42 {
		t.Errorf("expected segment num 42, got %d", segNum)
	}
}

func TestDecodeSegmentHeader_InvalidMagic(t *testing.T) {
	var buf [segmentHeaderSize]byte
	_, _, _, err := decodeSegmentHeader(buf[:])
	if err == nil {
		t.Error("expected error for invalid magic")
	}
}

func TestRecordSize_Padding(t *testing.T) {
	size := RecordSize(3)
	if size%8 != 0 {
		t.Errorf("record size must be 8-byte aligned, got %d", size)
	}

	size2 := RecordSize(1)
	if size2%8 != 0 {
		t.Errorf("record size must be 8-byte aligned, got %d", size2)
	}
}

func TestPad8(t *testing.T) {
	tests := []struct {
		length int
		want   int
	}{
		{0, 0},
		{1, 7},
		{7, 1},
		{8, 0},
		{9, 7},
		{16, 0},
	}
	for _, tt := range tests {
		got := pad8(tt.length)
		if got != tt.want {
			t.Errorf("pad8(%d) = %d, want %d", tt.length, got, tt.want)
		}
	}
}

func TestParseSegmentName(t *testing.T) {
	tests := []struct {
		name    string
		wantGen uint64
		wantNum uint64
		wantErr bool
	}{
		{"00000000695b8f00_00000001.wal", 0x695b8f00, 1, false},
		{"0000000000000001_00000002.wal", 1, 2, false},
		{"invalid.wal", 0, 0, true},
		{"abc_001", 0, 0, true},
		{"0000000000000001_00000002.txt", 0, 0, true},
	}
	for _, tt := range tests {
		gen, num, err := parseSegmentName(tt.name)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseSegmentName(%q) expected error", tt.name)
			}
		} else {
			if err != nil {
				t.Errorf("parseSegmentName(%q) unexpected error: %v", tt.name, err)
			}
			if gen != tt.wantGen {
				t.Errorf("parseSegmentName(%q) gen = %x, want %x", tt.name, gen, tt.wantGen)
			}
			if num != tt.wantNum {
				t.Errorf("parseSegmentName(%q) num = %d, want %d", tt.name, num, tt.wantNum)
			}
		}
	}
}

func TestEncodeRecord(t *testing.T) {
	payload := []byte("hello world")
	record := make([]byte, RecordSize(len(payload)))
	record = EncodeRecord(record, TypePointData, payload)

	if len(record) != RecordSize(len(payload)) {
		t.Errorf("expected record size %d, got %d", RecordSize(len(payload)), len(record))
	}

	// 验证 8 字节对齐
	if len(record)%8 != 0 {
		t.Errorf("record must be 8-byte aligned, got len=%d", len(record))
	}
}

func TestEncodeRecord_CRCValid(t *testing.T) {
	payload := []byte("test")
	record := make([]byte, RecordSize(len(payload)))
	record = EncodeRecord(record, TypePointData, payload)

	// Verify CRC by re-reading record header
	expectedCRC := uint32(record[0])<<24 | uint32(record[1])<<16 | uint32(record[2])<<8 | uint32(record[3])
	recType := record[4]
	payloadLen := int(record[5])<<24 | int(record[6])<<16 | int(record[7])<<8 | int(record[8])

	if recType != TypePointData {
		t.Errorf("expected type %d, got %d", TypePointData, recType)
	}
	if payloadLen != len(payload) {
		t.Errorf("expected payload len %d, got %d", len(payload), payloadLen)
	}

	// Recompute CRC
	actualCRC := crc32Sum(record[4:9])                         // type + len header
	actualCRC = crc32Update(actualCRC, record[9:9+payloadLen]) // payload
	if actualCRC != expectedCRC {
		t.Errorf("CRC mismatch: expected %08x, got %08x", expectedCRC, actualCRC)
	}
}
