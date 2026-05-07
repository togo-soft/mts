// internal/storage/shard/wal_test.go
package shard

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"codeberg.org/micro-ts/mts/types"
)

func TestWAL_Write(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer func() {
		if closeErr := w.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()

	data := []byte("test data")
	n, err := w.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected %d bytes written, got %d", len(data), n)
	}
}

func TestWAL_Sync(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer func() {
		if closeErr := w.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()

	data := []byte("test data")
	_, err = w.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	err = w.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

func TestWAL_Reopen(t *testing.T) {
	tmpDir := t.TempDir()
	w1, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	_, err = w1.Write([]byte("data1"))
	if err != nil {
		_ = w1.Close()
		t.Fatalf("Write failed: %v", err)
	}

	if err := w1.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	w2, err := NewWAL(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer func() {
		if closeErr := w2.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()

	if w2.Sequence() != 1 {
		t.Errorf("expected sequence 1, got %d", w2.Sequence())
	}
}

func TestWAL_FilePermissions(t *testing.T) {
	// Permission check skipped on Windows due to ACL differences
	if runtime.GOOS == "windows" {
		t.Skip("skipping permission test on Windows")
	}

	tmpDir := t.TempDir()
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	files, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	for _, f := range files {
		info, _ := os.Stat(f)
		if info.Mode().Perm() != 0600 {
			t.Errorf("expected 0600, got %o", info.Mode().Perm())
		}
	}
}

func TestWAL_Replay(t *testing.T) {
	tmpDir := t.TempDir()

	// 写入 WAL
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// 序列化 point 并写入
	p := &types.Point{
		Timestamp: 1000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}

	data, err := serializePoint(p)
	if err != nil {
		t.Fatalf("serializePoint failed: %v", err)
	}

	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 重放 WAL
	points, err := ReplayWAL(tmpDir)
	if err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}

	if len(points) != 1 {
		t.Errorf("expected 1 point, got %d", len(points))
	}

	if points[0].Timestamp != 1000 {
		t.Errorf("expected timestamp 1000, got %d", points[0].Timestamp)
	}

	usage := points[0].Fields["usage"].GetFloatValue()
	if usage != 85.5 {
		t.Errorf("expected usage 85.5, got %f", usage)
	}
}

func TestWAL_StartPeriodicSync(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	// 确保 WAL 关闭，防止 goroutine 泄露
	defer func() {
		if closeErr := wal.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()

	// 写入序列化的 point
	p := &types.Point{
		Timestamp: 1000,
		Tags:      map[string]string{"host": "server1"},
		Fields:    map[string]*types.FieldValue{"usage": types.NewFieldValue(85.5)},
	}

	data, err := serializePoint(p)
	if err != nil {
		t.Fatalf("serializePoint failed: %v", err)
	}

	if _, err := wal.Write(data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 启动 periodic sync，间隔 100ms
	done := make(chan struct{})
	wal.StartPeriodicSync(100*time.Millisecond, done)

	// 等待 250ms，确保至少 sync 了 2 次（100ms 间隔 * 2 + 缓冲时间）
	time.Sleep(250 * time.Millisecond)
	close(done)

	// 验证数据已刷盘（通过重新打开 WAL）
	wal2, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL for verify failed: %v", err)
	}
	defer func() {
		_ = wal2.Close()
	}()

	points, err := ReplayWAL(tmpDir)
	if err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}
	if len(points) != 1 {
		t.Errorf("expected 1 point, got %d", len(points))
	}
}

func TestWALReplayCheckpoint_SaveLoad(t *testing.T) {
	tmpDir := t.TempDir()

	cp := &WALReplayCheckpoint{
		LastSeq: 123,
		LastPos: 456,
		Updated: time.Now().UnixNano(),
	}

	// 保存 checkpoint
	if err := cp.Save(tmpDir); err != nil {
		t.Fatalf("Save checkpoint failed: %v", err)
	}

	// 加载 checkpoint
	cp2 := &WALReplayCheckpoint{}
	if err := cp2.Load(tmpDir); err != nil {
		t.Fatalf("Load checkpoint failed: %v", err)
	}

	if cp2.LastSeq != 123 {
		t.Errorf("expected LastSeq 123, got %d", cp2.LastSeq)
	}
	if cp2.LastPos != 456 {
		t.Errorf("expected LastPos 456, got %d", cp2.LastPos)
	}
}

func TestWALReplayCheckpoint_LoadNotExist(t *testing.T) {
	tmpDir := t.TempDir()

	cp := &WALReplayCheckpoint{}
	if err := cp.Load(tmpDir); err != nil {
		t.Fatalf("Load checkpoint failed: %v", err)
	}

	// 应该返回零值
	if cp.LastSeq != 0 {
		t.Errorf("expected LastSeq 0, got %d", cp.LastSeq)
	}
	if cp.LastPos != 0 {
		t.Errorf("expected LastPos 0, got %d", cp.LastPos)
	}
}

func TestWALReplayCheckpoint_Overwrite(t *testing.T) {
	tmpDir := t.TempDir()

	cp1 := &WALReplayCheckpoint{LastSeq: 1, LastPos: 100}
	if err := cp1.Save(tmpDir); err != nil {
		t.Fatalf("Save checkpoint failed: %v", err)
	}

	// 覆盖为新值
	cp2 := &WALReplayCheckpoint{LastSeq: 2, LastPos: 200}
	if err := cp2.Save(tmpDir); err != nil {
		t.Fatalf("Save checkpoint failed: %v", err)
	}

	// 验证加载的是新值
	cp3 := &WALReplayCheckpoint{}
	if err := cp3.Load(tmpDir); err != nil {
		t.Fatalf("Load checkpoint failed: %v", err)
	}

	if cp3.LastSeq != 2 {
		t.Errorf("expected LastSeq 2, got %d", cp3.LastSeq)
	}
	if cp3.LastPos != 200 {
		t.Errorf("expected LastPos 200, got %d", cp3.LastPos)
	}
}

func TestParseWALSeq(t *testing.T) {
	tests := []struct {
		path     string
		expected uint64
		wantErr  bool
	}{
		{"/path/to/00000000000000000001.wal", 1, false},
		{"/path/to/00000000000000000123.wal", 123, false},
		{"/path/to/wal", 0, true},
		{"/path/to/abc.wal", 0, true},
		{"/path/to/00000000000000000001.wal.bak", 0, true},
	}

	for _, tt := range tests {
		seq, err := parseWALSeq(tt.path)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseWALSeq(%q) expected error, got nil", tt.path)
			}
		} else {
			if err != nil {
				t.Errorf("parseWALSeq(%q) unexpected error: %v", tt.path, err)
			}
			if seq != tt.expected {
				t.Errorf("parseWALSeq(%q) = %d, want %d", tt.path, seq, tt.expected)
			}
		}
	}
}

func TestReplayWALFile(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建 WAL 并写入数据
	w, err := NewWAL(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	baseTime := time.Now().UnixNano()
	points := make([]*types.Point, 5)
	for i := 0; i < 5; i++ {
		p := &types.Point{
			Database:    "testdb",
			Measurement: "test",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   baseTime + int64(i)*int64(time.Millisecond),
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(int64(i)),
			},
		}
		data, serErr := serializePoint(p)
		if serErr != nil {
			t.Fatalf("serializePoint failed: %v", serErr)
		}
		if _, wErr := w.Write(data); wErr != nil {
			t.Fatalf("Write failed: %v", wErr)
		}
		points[i] = p
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	walPath := filepath.Join(tmpDir, "00000000000000000001.wal")

	// 从偏移 0 读取全部
	readPoints, readPos, err := replayWALFile(walPath, 0)
	if err != nil {
		t.Fatalf("replayWALFile from 0 failed: %v", err)
	}
	if len(readPoints) != 5 {
		t.Errorf("expected 5 points, got %d", len(readPoints))
	}
	if readPos <= 0 {
		t.Errorf("expected positive readPos, got %d", readPos)
	}

	// 从中间偏移读取
	readPoints2, _, err := replayWALFile(walPath, readPos/2)
	if err != nil {
		t.Fatalf("replayWALFile from middle failed: %v", err)
	}
	if len(readPoints2) >= len(readPoints) {
		t.Errorf("expected fewer points from middle offset, got %d", len(readPoints2))
	}
}

func TestReplayWAL_Incremental(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建第一个 WAL 文件，写入 3 条数据
	w1, err := NewWAL(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWAL 1 failed: %v", err)
	}
	baseTime := time.Now().UnixNano()
	for i := 0; i < 3; i++ {
		p := &types.Point{
			Database:    "testdb",
			Measurement: "test",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   baseTime + int64(i)*int64(time.Millisecond),
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(int64(i)),
			},
		}
		data, serErr := serializePoint(p)
		if serErr != nil {
			t.Fatalf("serializePoint failed: %v", serErr)
		}
		if _, wErr := w1.Write(data); wErr != nil {
			t.Fatalf("Write failed: %v", wErr)
		}
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close w1 failed: %v", err)
	}

	// 全量 replay，应该有 3 条数据
	points1, err := ReplayWAL(tmpDir)
	if err != nil {
		t.Fatalf("ReplayWAL 1 failed: %v", err)
	}
	if len(points1) != 3 {
		t.Errorf("expected 3 points after first replay, got %d", len(points1))
	}

	// 加载 checkpoint 确认有记录
	cp := &WALReplayCheckpoint{}
	if err := cp.Load(tmpDir); err != nil {
		t.Fatalf("Load checkpoint failed: %v", err)
	}
	if cp.LastSeq != 1 {
		t.Errorf("expected LastSeq 1, got %d", cp.LastSeq)
	}
	if cp.LastPos <= 0 {
		t.Errorf("expected positive LastPos, got %d", cp.LastPos)
	}

	// 创建第二个 WAL 文件，写入 2 条数据
	w2, err := NewWAL(tmpDir, 2)
	if err != nil {
		t.Fatalf("NewWAL 2 failed: %v", err)
	}
	for i := 0; i < 2; i++ {
		p := &types.Point{
			Database:    "testdb",
			Measurement: "test",
			Tags:        map[string]string{"host": "server2"},
			Timestamp:   baseTime + int64(i+10)*int64(time.Millisecond),
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(int64(i + 10)),
			},
		}
		data, serErr := serializePoint(p)
		if serErr != nil {
			t.Fatalf("serializePoint failed: %v", serErr)
		}
		if _, wErr := w2.Write(data); wErr != nil {
			t.Fatalf("Write failed: %v", wErr)
		}
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close w2 failed: %v", err)
	}

	// 增量 replay，应该只读新的 2 条数据
	points2, err := ReplayWAL(tmpDir)
	if err != nil {
		t.Fatalf("ReplayWAL 2 failed: %v", err)
	}
	if len(points2) != 2 {
		t.Errorf("expected 2 points from incremental replay, got %d", len(points2))
	}

	// 验证 checkpoint 已更新
	cp2 := &WALReplayCheckpoint{}
	if err := cp2.Load(tmpDir); err != nil {
		t.Fatalf("Load checkpoint 2 failed: %v", err)
	}
	if cp2.LastSeq != 2 {
		t.Errorf("expected LastSeq 2, got %d", cp2.LastSeq)
	}
}
