package sstable

import (
	"testing"
)

func TestBlockSectionOffsets_BlockCount(t *testing.T) {
	tests := []struct {
		name    string
		offsets []uint64
		want    int
	}{
		{"empty", nil, 0},
		{"single block", []uint64{0, 100}, 1},
		{"three blocks", []uint64{0, 50, 120, 200}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bso := &BlockSectionOffsets{Offsets: tt.offsets}
			got := bso.BlockCount()
			if got != tt.want {
				t.Errorf("BlockCount() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBlockSectionOffsets_BlockRange(t *testing.T) {
	bso := &BlockSectionOffsets{
		Name:    "_timestamps",
		Offsets: []uint64{0, 45, 98, 150},
	}

	offset, size := bso.BlockRange(0)
	if offset != 0 || size != 45 {
		t.Errorf("BlockRange(0) = (%d,%d), want (0,45)", offset, size)
	}

	offset, size = bso.BlockRange(2)
	if offset != 98 || size != 52 {
		t.Errorf("BlockRange(2) = (%d,%d), want (98,52)", offset, size)
	}

	// out of range
	offset, size = bso.BlockRange(3)
	if offset != 0 || size != 0 {
		t.Errorf("BlockRange(3) = (%d,%d), want (0,0)", offset, size)
	}

	offset, size = bso.BlockRange(-1)
	if offset != 0 || size != 0 {
		t.Errorf("BlockRange(-1) = (%d,%d), want (0,0)", offset, size)
	}
}

func TestBlockSectionMap_RoundTrip(t *testing.T) {
	m := &BlockSectionMap{
		Sections: []BlockSectionOffsets{
			{Name: "_timestamps", Offsets: []uint64{0, 45, 98, 150}},
			{Name: "_sids", Offsets: []uint64{0, 60, 130}},
			{Name: "value", Offsets: []uint64{0, 200, 400, 580, 790}},
		},
	}

	data := m.Marshal()

	parsed, err := UnmarshalBlockSectionMap(data)
	if err != nil {
		t.Fatalf("UnmarshalBlockSectionMap: %v", err)
	}

	if len(parsed.Sections) != len(m.Sections) {
		t.Fatalf("section count = %d, want %d", len(parsed.Sections), len(m.Sections))
	}

	for i, orig := range m.Sections {
		got := parsed.Sections[i]
		if got.Name != orig.Name {
			t.Errorf("section[%d].Name = %q, want %q", i, got.Name, orig.Name)
		}
		if got.BlockCount() != orig.BlockCount() {
			t.Errorf("section[%d].BlockCount() = %d, want %d", i, got.BlockCount(), orig.BlockCount())
		}
		if len(got.Offsets) != len(orig.Offsets) {
			t.Errorf("section[%d].len(Offsets) = %d, want %d", i, len(got.Offsets), len(orig.Offsets))
		}
		for j := range orig.Offsets {
			if got.Offsets[j] != orig.Offsets[j] {
				t.Errorf("section[%d].Offsets[%d] = %d, want %d", i, j, got.Offsets[j], orig.Offsets[j])
			}
		}
	}
}

func TestBlockSectionMap_Lookup(t *testing.T) {
	m := &BlockSectionMap{
		Sections: []BlockSectionOffsets{
			{Name: "_timestamps", Offsets: []uint64{0, 45}},
			{Name: "value", Offsets: []uint64{0, 200, 400}},
		},
	}

	if bso := m.Lookup("_timestamps"); bso == nil {
		t.Error("Lookup(_timestamps) returned nil")
	} else if bso.Name != "_timestamps" {
		t.Errorf("Lookup(_timestamps).Name = %q", bso.Name)
	}

	if bso := m.Lookup("value"); bso == nil {
		t.Error("Lookup(value) returned nil")
	} else if bso.BlockCount() != 2 {
		t.Errorf("Lookup(value).BlockCount() = %d, want 2", bso.BlockCount())
	}

	if bso := m.Lookup("nonexistent"); bso != nil {
		t.Error("Lookup(nonexistent) should return nil")
	}
}

func TestUnmarshalBlockSectionMap_Invalid(t *testing.T) {
	_, err := UnmarshalBlockSectionMap([]byte{})
	if err == nil {
		t.Error("expected error for empty data")
	}

	// truncated: section_count=1 but no data
	_, err = UnmarshalBlockSectionMap([]byte{0, 1})
	if err == nil {
		t.Error("expected error for truncated data")
	}
}
