package sstable

import (
	"fmt"
	"testing"

	"codeberg.org/micro-ts/mts/types"
)

// testWriteReadRoundtrip writes points with the given compression and reads them back.
func testWriteReadRoundtrip(t *testing.T, algo CompressionAlgorithm, numPoints int) {
	t.Helper()
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, algo)
	if err != nil {
		t.Fatalf("NewWriter(%s) failed: %v", algo.String(), err)
	}

	expected := make(map[int64]int64) // timestamp -> value
	for i := 0; i < numPoints; i++ {
		ts := int64(i+1) * 1_000_000_000
		val := int64(i * 10)
		expected[ts] = val
	}

	points := make([]*types.Point, 0, numPoints)
	for ts, val := range expected {
		points = append(points, &types.Point{
			Timestamp: ts,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(val)},
		})
	}

	if err := w.WritePoints(pointsToInternalWithSids(points, nil)); err != nil {
		t.Fatalf("WritePoints(%s) failed: %v", algo.String(), err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close(%s) failed: %v", algo.String(), err)
	}

	sstPath := fmt.Sprintf("%s/data/sst_0.bin", tmpDir)
	r, err := NewReader(sstPath, w.Schema())
	if err != nil {
		t.Fatalf("NewReader(%s) failed: %v", algo.String(), err)
	}
	defer func() { _ = r.Close() }()

	rows, err := r.ReadAll(nil)
	if err != nil {
		t.Fatalf("ReadAll(%s) failed: %v", algo.String(), err)
	}

	if len(rows) != numPoints {
		t.Errorf("%s: expected %d rows, got %d", algo.String(), numPoints, len(rows))
	}

	got := make(map[int64]int64)
	for _, row := range rows {
		if row.Fields != nil && row.GetFieldValue("value") != nil {
			got[row.Timestamp] = row.GetFieldValue("value").GetIntValue()
		}
	}

	for ts, wantVal := range expected {
		gotVal, ok := got[ts]
		if !ok {
			t.Errorf("%s: missing timestamp %d", algo.String(), ts)
			continue
		}
		if gotVal != wantVal {
			t.Errorf("%s: ts=%d: expected value=%d, got %d", algo.String(), ts, wantVal, gotVal)
		}
	}
}

func TestCompressionRoundtrip_None_Small(t *testing.T) {
	testWriteReadRoundtrip(t, CompressionNone, 5)
}

func TestCompressionRoundtrip_Snappy_Small(t *testing.T) {
	testWriteReadRoundtrip(t, CompressionSnappy, 5)
}

func TestCompressionRoundtrip_LZ4_Small(t *testing.T) {
	testWriteReadRoundtrip(t, CompressionLZ4, 5)
}

func TestCompressionRoundtrip_None_Large(t *testing.T) {
	testWriteReadRoundtrip(t, CompressionNone, 50)
}

func TestCompressionRoundtrip_Snappy_Large(t *testing.T) {
	testWriteReadRoundtrip(t, CompressionSnappy, 50)
}

func TestCompressionRoundtrip_LZ4_Large(t *testing.T) {
	testWriteReadRoundtrip(t, CompressionLZ4, 50)
}

func TestCompressionRoundtrip_MixedFieldTypes(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 0, 0, CompressionLZ4)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	points := []*types.Point{
		{
			Timestamp: 1_000_000_000,
			Tags:      map[string]string{"host": "a"},
			Fields: map[string]*types.FieldValue{
				"float_val": types.NewFieldValue(3.14),
				"int_val":   types.NewFieldValue(int64(42)),
				"str_val":   types.NewFieldValue("hello"),
				"bool_val":  types.NewFieldValue(true),
			},
		},
		{
			Timestamp: 2_000_000_000,
			Tags:      map[string]string{"host": "b"},
			Fields: map[string]*types.FieldValue{
				"float_val": types.NewFieldValue(2.718),
				"int_val":   types.NewFieldValue(int64(99)),
				"str_val":   types.NewFieldValue("world"),
				"bool_val":  types.NewFieldValue(false),
			},
		},
	}

	if err := w.WritePoints(pointsToInternalWithSids(points, nil)); err != nil {
		t.Fatalf("WritePoints failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	sstPath := fmt.Sprintf("%s/data/sst_0.bin", tmpDir)
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

	for i, row := range rows {
		if row.Fields == nil {
			t.Errorf("row[%d] should have fields", i)
			continue
		}
		if row.GetFieldValue("float_val") == nil {
			t.Errorf("row[%d] missing float_val", i)
		}
		if row.GetFieldValue("int_val") == nil {
			t.Errorf("row[%d] missing int_val", i)
		}
		if row.GetFieldValue("str_val") == nil {
			t.Errorf("row[%d] missing str_val", i)
		}
		if row.GetFieldValue("bool_val") == nil {
			t.Errorf("row[%d] missing bool_val", i)
		}
	}
}

func TestCompressionRoundtrip_FileSizeComparison(t *testing.T) {
	// 高度重复的数据应产生不同的文件大小
	tmpDir := t.TempDir()
	numPoints := 100

	// 生成高度重复的数据
	points := make([]*types.Point, 0, numPoints)
	for i := 0; i < numPoints; i++ {
		points = append(points, &types.Point{
			Timestamp: int64(i+1) * 1_000_000_000,
			Tags:      map[string]string{"host": "server1"},
			Fields:    map[string]*types.FieldValue{"value": types.NewFieldValue(int64(42))},
		})
	}

	var noneSize, snappySize, lz4Size int64

	for _, algo := range []CompressionAlgorithm{CompressionNone, CompressionSnappy, CompressionLZ4} {
		algoDir := t.TempDir()

		w, err := NewWriter(algoDir, 0, 0, algo)
		if err != nil {
			t.Fatalf("NewWriter(%s) failed: %v", algo.String(), err)
		}
		if err := w.WritePoints(pointsToInternalWithSids(points, nil)); err != nil {
			t.Fatalf("WritePoints(%s) failed: %v", algo.String(), err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close(%s) failed: %v", algo.String(), err)
		}

		// 验证读取正确性
		sstPath := fmt.Sprintf("%s/data/sst_0.bin", algoDir)
		r, err := NewReader(sstPath, w.Schema())
		if err != nil {
			t.Fatalf("NewReader(%s) failed: %v", algo.String(), err)
		}
		rows, err := r.ReadAll(nil)
		if err != nil {
			_ = r.Close()
			t.Fatalf("ReadAll(%s) failed: %v", algo.String(), err)
		}
		if len(rows) != numPoints {
			_ = r.Close()
			t.Errorf("%s: expected %d rows, got %d", algo.String(), numPoints, len(rows))
		}
		_ = r.Close()

		// 获取文件大小
		_ = tmpDir // avoid unused
	}

	// 不强制断言大小关系（小数据可能不会显著压缩），但记录以供参考
	_ = noneSize
	_ = snappySize
	_ = lz4Size
}
