package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

func pointsToInternalWithSids(points []*types.Point, sids []uint64) []types.InternalPoint {
	result := make([]types.InternalPoint, len(points))
	for i, p := range points {
		sid := uint64(0)
		if i < len(sids) {
			sid = sids[i]
		}
		result[i] = types.PointToInternal(p, sid)
	}
	return result
}

func TestWriter_WritePoints(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5), "count": types.NewFieldValue(int64(100))},
		},
		{
			Timestamp: 2000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(90.0), "count": types.NewFieldValue(int64(200))},
		},
	}

	err = w.WritePoints(pointsToInternalWithSids(points, nil))
	if err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证 timestamp 文件存在
	tsPath := filepath.Join(tmpDir, "data", "sst_0", "_timestamps.bin")
	info, err := os.Stat(tsPath)
	if err != nil {
		t.Fatalf("stat timestamp file failed: %v", err)
	}
	if info.Size() == 0 {
		t.Errorf("timestamp file should not be empty")
	}

	// 验证 sids 文件存在
	sidPath := filepath.Join(tmpDir, "data", "sst_0", "_sids.bin")
	sidInfo, err := os.Stat(sidPath)
	if err != nil {
		t.Fatalf("stat sids file failed: %v", err)
	}
	if sidInfo.Size() == 0 {
		t.Errorf("sids file should not be empty (should contain zeros for nil sids)")
	}

	// 验证 field 文件存在
	for _, name := range []string{"usage", "count"} {
		fieldPath := filepath.Join(tmpDir, "data", "sst_0", "fields", name+".bin")
		info, err := os.Stat(fieldPath)
		if err != nil {
			t.Fatalf("stat field %s file failed: %v", name, err)
		}
		if info.Size() == 0 {
			t.Errorf("field %s file should not be empty", name)
		}
	}
}

func TestWriter_WritePointsWithSids(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 1, 0)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1000,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(int64(1))},
		},
		{
			Timestamp: 2000,
			Fields:    map[string]*types.FieldValue{"v": types.NewFieldValue(int64(2))},
		},
	}
	sids := []uint64{42, 99}

	if err := w.WritePoints(pointsToInternalWithSids(points, sids)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sidPath := filepath.Join(tmpDir, "data", "sst_1", "_sids.bin")
	info, err := os.Stat(sidPath)
	if err != nil {
		t.Fatalf("stat sids file failed: %v", err)
	}
	if info.Size() < 16 {
		t.Errorf("sids file too small, expected at least 16 bytes for 2 uint64, got %d", info.Size())
	}
}
