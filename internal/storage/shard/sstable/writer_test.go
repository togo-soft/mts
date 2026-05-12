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

	w, err := NewWriter(tmpDir, 0, 0, CompressionNone)
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

	// 验证单文件 SSTable 存在
	sstPath := filepath.Join(tmpDir, "data", "sst_0.bin")
	info, err := os.Stat(sstPath)
	if err != nil {
		t.Fatalf("stat sst file failed: %v", err)
	}
	if info.Size() == 0 {
		t.Errorf("sst file should not be empty")
	}

	// 通过 Reader 验证数据
	r, err := NewReader(sstPath, w.Schema())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	rows, err := r.ReadAll(nil)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}
}

func TestWriter_WritePointsWithSids(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 1, 0, CompressionNone)
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

	// 验证单文件 SSTable 存在
	sstPath := filepath.Join(tmpDir, "data", "sst_1.bin")
	info, err := os.Stat(sstPath)
	if err != nil {
		t.Fatalf("stat sst file failed: %v", err)
	}
	if info.Size() < 64 {
		t.Errorf("sst file too small, got %d bytes", info.Size())
	}

	// 通过 Reader 验证 SID
	r, err := NewReader(sstPath, w.Schema())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() { _ = r.Close() }()

	rows, err := r.ReadAll(nil)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Sid != 42 {
		t.Errorf("expected first row Sid=42, got %d", rows[0].Sid)
	}
	if rows[1].Sid != 99 {
		t.Errorf("expected second row Sid=99, got %d", rows[1].Sid)
	}
}
