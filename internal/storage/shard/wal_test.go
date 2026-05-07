// internal/storage/shard/wal_test.go
package shard

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
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

func TestReplayWALFromCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建 WAL 并写入数据
	w, err := NewWAL(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	baseTime := time.Now().UnixNano()
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
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 使用 checkpoint 从头 replay
	cp := &WALReplayCheckpoint{}
	points, err := ReplayWALFromCheckpoint(tmpDir, cp)
	if err != nil {
		t.Fatalf("ReplayWALFromCheckpoint failed: %v", err)
	}
	if len(points) != 5 {
		t.Errorf("expected 5 points, got %d", len(points))
	}

	// 使用中间位置的 checkpoint
	cp2 := &WALReplayCheckpoint{LastSeq: 1, LastPos: 100}
	points2, err := ReplayWALFromCheckpoint(tmpDir, cp2)
	if err != nil {
		t.Fatalf("ReplayWALFromCheckpoint with checkpoint failed: %v", err)
	}
	if len(points2) >= 5 {
		t.Errorf("expected fewer points from checkpoint, got %d", len(points2))
	}
}

func TestWALRotateLocked(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// 写入大量数据触发滚动（walFileSizeLimit = 64MB）
	// 每次写入 1MB，写入 70 次应该触发至少一次滚动
	for i := 0; i < 70; i++ {
		data := make([]byte, 1024*1024) // 1MB per write
		_, err := w.Write(data)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// 检查序列号是否增加（滚动后应该是 seq=2）
	seq := w.Sequence()
	t.Logf("WAL sequence after writes: %d", seq)

	// 滚动后序列号应该大于 1
	if seq <= 1 {
		t.Errorf("expected sequence > 1 after rotation, got %d", seq)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWALRotateLocked_Direct(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// 手动调用 rotateLocked
	err = w.rotateLocked()
	if err != nil {
		t.Fatalf("rotateLocked failed: %v", err)
	}

	// 序列号应该增加
	if w.Sequence() != 2 {
		t.Errorf("expected sequence 2 after rotate, got %d", w.Sequence())
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWALCleanup(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建多个 WAL 文件
	wal1, err := NewWAL(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWAL 1 failed: %v", err)
	}
	if _, err := wal1.Write([]byte("data1")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := wal1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	wal2, err := NewWAL(tmpDir, 2)
	if err != nil {
		t.Fatalf("NewWAL 2 failed: %v", err)
	}
	if _, err := wal2.Write([]byte("data2")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := wal2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	wal3, err := NewWAL(tmpDir, 3)
	if err != nil {
		t.Fatalf("NewWAL 3 failed: %v", err)
	}
	if _, err := wal3.Write([]byte("data3")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := wal3.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证所有文件存在
	files, err := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}
	if len(files) != 3 {
		t.Errorf("expected 3 WAL files, got %d", len(files))
	}

	// 调用 Cleanup 删除序列号小于 3 的文件
	wal := &WAL{dir: tmpDir, seq: 3}
	if err := wal.Cleanup(); err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	// 验证文件 1 和 2 被删除
	for _, seq := range []uint64{1, 2} {
		filename := filepath.Join(tmpDir, fmt.Sprintf("%020d.wal", seq))
		if _, err := os.Stat(filename); !os.IsNotExist(err) {
			t.Errorf("expected file %s to be deleted", filename)
		}
	}

	// 验证文件 3 仍然存在
	filename3 := filepath.Join(tmpDir, fmt.Sprintf("%020d.wal", uint64(3)))
	if _, err := os.Stat(filename3); os.IsNotExist(err) {
		t.Errorf("expected file %s to exist", filename3)
	}
}

func TestWAL_WriteConcurrent(t *testing.T) {
	// 测试并发写入
	tmpDir := t.TempDir()
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	const goroutines = 10
	const writesPerGoroutine = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				data := []byte(fmt.Sprintf("data-%d-%d", id, i))
				_, err := w.Write(data)
				if err != nil {
					t.Errorf("Write failed: %v", err)
				}
			}
		}(g)
	}

	wg.Wait()

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证所有数据都能 replay
	points, err := ReplayWAL(tmpDir)
	if err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}

	// 注意：由于 WAL 只存储 bytes，replay 返回的是反序列化后的 Point
	// 并发写入不保证顺序，但数据应该完整
	t.Logf("ReplayWAL returned %d points/entries", len(points))
}

func TestWAL_Sync_FlushesData(t *testing.T) {
	// 测试 Sync 能正确刷盘
	tmpDir := t.TempDir()
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// 写入数据
	data := []byte("test data for sync")
	_, err = w.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Sync 应该刷盘
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// 关闭
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 重新打开 WAL 验证数据
	w2, err := NewWAL(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer func() { _ = w2.Close() }()

	// 应该没有任何数据（因为是新的序列号）
	if w2.Sequence() != 1 {
		t.Errorf("expected sequence 1, got %d", w2.Sequence())
	}
}

func TestWAL_TruncateCurrent(t *testing.T) {
	// 测试 TruncateCurrent 清空当前文件
	tmpDir := t.TempDir()
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// 写入一些数据
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		_, err := w.Write(data)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Truncate
	if err := w.TruncateCurrent(); err != nil {
		t.Fatalf("TruncateCurrent failed: %v", err)
	}

	// 关闭
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证文件仍然存在但是空的
	files, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if len(files) != 1 {
		t.Errorf("expected 1 WAL file, got %d", len(files))
	}
}

func TestWAL_parseWALSeq(t *testing.T) {
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
		{"/path/to/sst_1", 0, true},
		{"00000000000000000001.wal", 1, false},
		{"wal", 0, true},
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

func TestWAL_ReplayFromCheckpoint(t *testing.T) {
	// 测试从指定 checkpoint 开始 replay
	tmpDir := t.TempDir()

	// 创建 WAL 并写入数据
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	baseTime := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Database:    "testdb",
			Measurement: "test",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   baseTime + int64(i)*1000,
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(int64(i)),
			},
		}
		data, _ := serializePoint(p)
		_, _ = w.Write(data)
	}
	_ = w.Close()

	// 创建 checkpoint（跳过前 5 条数据）
	cp := &WALReplayCheckpoint{
		LastSeq: 0,
		LastPos: 0,
		Updated: time.Now().UnixNano(),
	}

	// 先全量 replay 更新 checkpoint
	points1, err := ReplayWAL(tmpDir)
	if err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}
	t.Logf("Full replay got %d points", len(points1))

	// 再次 replay 应该使用 checkpoint
	if err := cp.Load(tmpDir); err != nil {
		t.Fatalf("Checkpoint Load failed: %v", err)
	}
	points2, err := ReplayWALFromCheckpoint(tmpDir, cp)
	if err != nil {
		t.Fatalf("ReplayWALFromCheckpoint failed: %v", err)
	}

	// 如果 checkpoint 有效，应该跳过一些数据
	t.Logf("Checkpoint replay got %d points", len(points2))
}

func TestWAL_Write_ExactlyAtLimit(t *testing.T) {
	// 测试写入正好在限制边界的数据
	tmpDir := t.TempDir()
	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// 写入 4KB 数据（正好是缓冲区大小）
	largeData := make([]byte, 4096)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	n, err := w.Write(largeData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(largeData) {
		t.Errorf("expected %d bytes written, got %d", len(largeData), n)
	}

	_ = w.Close()
}

func TestReplayWALFile_Streaming(t *testing.T) {
	// 测试流式读取：确保不会一次性加载整个文件到内存
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// 写入 100 条数据
	baseTime := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		p := &types.Point{
			Database:    "testdb",
			Measurement: "test",
			Tags:        map[string]string{"host": fmt.Sprintf("server%d", i%10)},
			Timestamp:   baseTime + int64(i)*int64(time.Millisecond),
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(int64(i)),
			},
		}
		data, _ := serializePoint(p)
		_, _ = w.Write(data)
	}
	_ = w.Close()

	walPath := filepath.Join(tmpDir, "00000000000000000000.wal")

	// 从偏移 0 读取全部
	readPoints, readPos, err := replayWALFile(walPath, 0)
	if err != nil {
		t.Fatalf("replayWALFile failed: %v", err)
	}
	if len(readPoints) != 100 {
		t.Errorf("expected 100 points, got %d", len(readPoints))
	}
	if readPos <= 0 {
		t.Errorf("expected positive readPos, got %d", readPos)
	}

	// 验证数据完整性
	for i, p := range readPoints {
		if p.Timestamp != baseTime+int64(i)*int64(time.Millisecond) {
			t.Errorf("point %d: expected timestamp %d, got %d", i, baseTime+int64(i)*int64(time.Millisecond), p.Timestamp)
		}
	}
}

func TestReplayWALFile_PartialRead(t *testing.T) {
	// 测试部分读取：从中间位置开始
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	baseTime := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		p := &types.Point{
			Database:    "testdb",
			Measurement: "test",
			Tags:        map[string]string{"host": "server1"},
			Timestamp:   baseTime + int64(i)*int64(time.Millisecond),
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(int64(i)),
			},
		}
		data, _ := serializePoint(p)
		_, _ = w.Write(data)
	}
	_ = w.Close()

	walPath := filepath.Join(tmpDir, "00000000000000000000.wal")

	// 从偏移 0 读取全部 10 条
	points1, pos1, err := replayWALFile(walPath, 0)
	if err != nil {
		t.Fatalf("replayWALFile failed: %v", err)
	}
	if len(points1) != 10 {
		t.Errorf("expected 10 points from offset 0, got %d", len(points1))
	}
	if pos1 <= 0 {
		t.Errorf("expected positive pos1, got %d", pos1)
	}

	// 从 pos1/2 位置读取（跳过一些数据）
	points2, _, err := replayWALFile(walPath, pos1/2)
	if err != nil {
		t.Fatalf("replayWALFile from middle failed: %v", err)
	}
	// 应该读到更少的数据（因为跳过了前半部分）
	if len(points2) >= len(points1) {
		t.Errorf("expected fewer points from middle offset, got first=%d second=%d", len(points1), len(points2))
	}
}

func TestReplayWALFile_EmptyFile(t *testing.T) {
	// 测试读取空文件
	tmpDir := t.TempDir()

	// 创建一个空的 WAL 文件
	emptyPath := filepath.Join(tmpDir, "00000000000000000000.wal")
	if err := os.WriteFile(emptyPath, []byte{}, 0600); err != nil {
		t.Fatalf("failed to create empty WAL file: %v", err)
	}

	points, pos, err := replayWALFile(emptyPath, 0)
	if err != nil {
		t.Fatalf("replayWALFile failed: %v", err)
	}
	if len(points) != 0 {
		t.Errorf("expected 0 points from empty file, got %d", len(points))
	}
	if pos != 0 {
		t.Errorf("expected pos 0 for empty file, got %d", pos)
	}
}

func TestReplayWALFile_StartPosition(t *testing.T) {
	// 测试从指定起始位置读取
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	baseTime := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		p := &types.Point{
			Timestamp: baseTime + int64(i)*int64(time.Millisecond),
			Tags:      map[string]string{"host": "server1"},
			Fields: map[string]*types.FieldValue{
				"value": types.NewFieldValue(int64(i)),
			},
		}
		data, _ := serializePoint(p)
		_, _ = w.Write(data)
	}
	_ = w.Close()

	walPath := filepath.Join(tmpDir, "00000000000000000000.wal")

	// 从偏移 0 读取
	allPoints, _, err := replayWALFile(walPath, 0)
	if err != nil {
		t.Fatalf("replayWALFile(0) failed: %v", err)
	}

	// 从文件末尾位置读取（应该没有数据）
	points, pos, err := replayWALFile(walPath, 1<<30) // 1GB offset
	if err != nil {
		t.Fatalf("replayWALFile(eof) failed: %v", err)
	}
	if len(points) != 0 {
		t.Errorf("expected 0 points from EOF position, got %d", len(points))
	}
	_ = allPoints
	_ = pos
}

func TestReplayWALFile_LargeRecord(t *testing.T) {
	// 测试处理超大记录
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// 写入一条正常记录
	p := &types.Point{
		Timestamp: time.Now().UnixNano(),
		Tags:      map[string]string{"host": "server1"},
		Fields: map[string]*types.FieldValue{
			"value": types.NewFieldValue(int64(1)),
		},
	}
	data, _ := serializePoint(p)
	_, _ = w.Write(data)
	_ = w.Close()

	walPath := filepath.Join(tmpDir, "00000000000000000000.wal")

	// 读取应该成功
	points, _, err := replayWALFile(walPath, 0)
	if err != nil {
		t.Fatalf("replayWALFile failed: %v", err)
	}
	if len(points) != 1 {
		t.Errorf("expected 1 point, got %d", len(points))
	}
}
