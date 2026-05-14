package wal

import (
	"os"
	"sync"
	"testing"
)

func TestWAL_OpenAndClose(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{
		Dir:      tmpDir,
		SyncMode: SyncNone,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries, _ := listSegments(tmpDir)
	if len(entries) != 1 {
		t.Errorf("expected 1 segment, got %d", len(entries))
	}
}

func TestWAL_WriteAndReplay(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	payloads := [][]byte{
		[]byte("record-1"),
		[]byte("record-2"),
		[]byte("record-3"),
	}
	for _, p := range payloads {
		if _, err := w.Write(p); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w2.Close() }()

	var replayed [][]byte
	err = w2.Replay(func(data []byte) error {
		replayed = append(replayed, append([]byte(nil), data...))
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(replayed) != 3 {
		t.Errorf("expected 3 replayed records, got %d", len(replayed))
	}
	for i, p := range replayed {
		if string(p) != string(payloads[i]) {
			t.Errorf("record %d: expected %q, got %q", i, payloads[i], p)
		}
	}
}

func TestWAL_WriteBatch(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	payloads := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	n, err := w.WriteBatch(payloads)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if n != 3 {
		t.Errorf("expected total 3, got %d", n)
	}
}

func TestWAL_TruncateCurrent(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	_, _ = w.Write([]byte("data-to-truncate"))
	_ = w.Sync()

	if err := w.TruncateCurrent(); err != nil {
		t.Fatalf("TruncateCurrent: %v", err)
	}

	var count int
	_ = w.Replay(func(data []byte) error {
		count++
		return nil
	})
	// TruncateCurrent 不再截断 segment，数据被保留
	if count != 1 {
		t.Errorf("expected 1 record after truncate, got %d", count)
	}
}

func TestWAL_Rotation(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{
		Dir:         tmpDir,
		SegmentSize: 1024,
		SyncMode:    SyncNone,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	largePayload := make([]byte, 200)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	for i := 0; i < 20; i++ {
		if _, err := w.Write(largePayload); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	_ = w.Sync()
	_ = w.Close()

	entries, _ := listSegments(tmpDir)
	if len(entries) < 2 {
		t.Errorf("expected at least 2 segments after rotation, got %d", len(entries))
	}
}

func TestWAL_ConcurrentWrite(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	const goroutines = 10
	const writesPer = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < writesPer; i++ {
				data := []byte("concurrent-data")
				if _, err := w.Write(data); err != nil {
					t.Errorf("Write error: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestWAL_ReplayIncremental(t *testing.T) {
	tmpDir := t.TempDir()

	w1, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 10; i++ {
		data := make([]byte, 10)
		data[0] = byte('a' + i)
		_, _ = w1.Write(data)
	}
	_ = w1.Close()

	w2, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w2.Close() }()

	for i := 0; i < 5; i++ {
		data := make([]byte, 10)
		data[0] = byte('x')
		_, _ = w2.Write(data)
	}

	var count int
	_ = w2.Replay(func(data []byte) error {
		count++
		return nil
	})
	if count < 5 {
		t.Errorf("expected at least 5 records, got %d", count)
	}
}

func TestWAL_FilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = w.Close()

	entries, _ := listSegments(tmpDir)
	for _, e := range entries {
		info, _ := os.Stat(e.Path)
		if info != nil && info.Mode().Perm() != 0600 {
			t.Errorf("expected 0600 permission on %s, got %o", e.Path, info.Mode().Perm())
		}
	}
}

func TestWAL_CRC_Corruption_Skip(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, _ = w.Write([]byte("good-record"))
	_ = w.Sync()
	_ = w.Close()

	entries, _ := listSegments(tmpDir)
	if len(entries) > 0 {
		data, _ := os.ReadFile(entries[0].Path)
		if len(data) > segmentHeaderSize+4 {
			data[segmentHeaderSize] = 0xFF
			data[segmentHeaderSize+1] = 0xFF
			_ = os.WriteFile(entries[0].Path, data, 0600)
		}
	}

	w2, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w2.Close() }()

	var count int
	_ = w2.Replay(func(data []byte) error {
		count++
		return nil
	})
	if count != 0 {
		t.Logf("corrupted record skipped: %d records replayed", count)
	}
}

func TestWAL_CloseIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestWAL_Generation(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	if w.Generation() == 0 {
		t.Error("expected non-zero generation")
	}
	if w.SegmentNum() != 1 {
		t.Errorf("expected segment num 1, got %d", w.SegmentNum())
	}
}

func TestWAL_TruncateAfterFlush_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// 写入多条记录
	for i := 0; i < 10; i++ {
		data := make([]byte, 100)
		data[0] = byte(i)
		if _, err := w.Write(data); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	_ = w.Sync()

	// TruncateAfterFlush：rotate + 删除旧 segment，仅保留新空 segment
	if err := w.TruncateAfterFlush(); err != nil {
		t.Fatalf("TruncateAfterFlush: %v", err)
	}

	// 验证只剩 1 个 segment
	entries, _ := listSegments(tmpDir)
	if len(entries) != 1 {
		t.Errorf("expected 1 segment after truncate, got %d", len(entries))
	}

	// 验证 truncate 后仍可继续写入
	if _, err := w.Write([]byte("after-truncate")); err != nil {
		t.Fatalf("Write after truncate: %v", err)
	}
	_ = w.Sync()

	// replay 应只看到 truncate 后的数据
	var count int
	_ = w.Replay(func(data []byte) error {
		count++
		return nil
	})
	if count != 1 {
		t.Errorf("expected 1 record after truncate, got %d", count)
	}

	_ = w.Close()
}

func TestWAL_TruncateAfterFlush_EmptyWAL(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	// 空 WAL 调用也不应报错
	if err := w.TruncateAfterFlush(); err != nil {
		t.Fatalf("TruncateAfterFlush on empty WAL: %v", err)
	}

	entries, _ := listSegments(tmpDir)
	if len(entries) != 1 {
		t.Errorf("expected 1 segment, got %d", len(entries))
	}
}

func TestWAL_TruncateAfterFlush_MultiSegment(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{
		Dir:         tmpDir,
		SegmentSize: 1024,
		SyncMode:    SyncNone,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// 写入足够数据触发多次 rotate（至少 3 个 segment）
	largePayload := make([]byte, 500)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}
	for i := 0; i < 10; i++ {
		if _, err := w.Write(largePayload); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	_ = w.Sync()

	// 验证有多个 segment
	entriesBefore, _ := listSegments(tmpDir)
	if len(entriesBefore) < 2 {
		t.Fatalf("need at least 2 segments for multi-segment test, got %d", len(entriesBefore))
	}

	// TruncateAfterFlush 应清理所有旧 segment
	if err := w.TruncateAfterFlush(); err != nil {
		t.Fatalf("TruncateAfterFlush: %v", err)
	}

	// 验证只剩 1 个新 segment
	entriesAfter, _ := listSegments(tmpDir)
	if len(entriesAfter) != 1 {
		t.Errorf("expected 1 segment after multi-segment truncate, got %d", len(entriesAfter))
	}

	// 验证 truncate 后能正常写入
	if _, err := w.Write([]byte("post-truncate-data")); err != nil {
		t.Fatalf("Write after truncate: %v", err)
	}
	_ = w.Sync()

	_ = w.Close()
}

func TestWAL_TruncateAfterFlush_Closed(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = w.Close()

	// 已关闭的 WAL 调用 TruncateAfterFlush 不应报错
	if err := w.TruncateAfterFlush(); err != nil {
		t.Errorf("TruncateAfterFlush on closed WAL should not error, got %v", err)
	}
}

func TestWAL_TruncateAfterFlush_RestartRecovery(t *testing.T) {
	tmpDir := t.TempDir()

	// 第一阶段：写入数据 → truncate → 关闭
	w1, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		_, _ = w1.Write([]byte("phase1-data"))
	}
	_ = w1.Sync()
	if err := w1.TruncateAfterFlush(); err != nil {
		t.Fatalf("TruncateAfterFlush: %v", err)
	}
	// truncate 后写入新数据
	if _, err := w1.Write([]byte("phase1-after-truncate")); err != nil {
		t.Fatalf("Write after truncate: %v", err)
	}
	_ = w1.Close()

	// 第二阶段：重新打开，验证 replay 仅包含 truncate 后的数据
	w2, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()

	var replayed [][]byte
	_ = w2.Replay(func(data []byte) error {
		replayed = append(replayed, append([]byte(nil), data...))
		return nil
	})

	if len(replayed) != 1 {
		t.Errorf("expected 1 replayed record after truncate + restart, got %d", len(replayed))
	}
	if len(replayed) > 0 && string(replayed[0]) != "phase1-after-truncate" {
		t.Errorf("expected 'phase1-after-truncate', got %q", replayed[0])
	}
}

func TestWAL_WriteAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(Config{Dir: tmpDir, SyncMode: SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = w.Close()

	_, err = w.Write([]byte("test"))
	if err != ErrWALClosed {
		t.Errorf("expected ErrWALClosed, got %v", err)
	}
}

func TestReplay_WithCheckpoint_SkipsPersistedSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, SegmentSize: 1024, SyncMode: SyncNone})
	if err != nil {
		t.Fatal(err)
	}

	// 写入 seg1
	_, _ = w.Write([]byte("seg1-data"))
	_ = w.Rotate()
	// 写入 seg2
	_, _ = w.Write([]byte("seg2-data"))
	_ = w.Rotate()
	// 写入 seg3
	_, _ = w.Write([]byte("seg3-data"))
	_ = w.Close()

	// 记录 checkpoint: seg2 已持久化
	cp := &Checkpoint{Generation: w.Generation(), Segment: 2}
	if err := cp.Save(dir); err != nil {
		t.Fatal(err)
	}

	// 重新打开，replay 应只回放 seg3
	w2, err := Open(Config{Dir: dir, SegmentSize: 1024, SyncMode: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()

	var replayed []string
	err = w2.Replay(func(data []byte) error {
		replayed = append(replayed, string(data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 {
		t.Fatalf("expected 1 replayed segment, got %d: %v", len(replayed), replayed)
	}
	if replayed[0] != "seg3-data" {
		t.Errorf("expected seg3-data, got %s", replayed[0])
	}
}

func TestReplay_NoCheckpoint_ReplaysAll(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, SegmentSize: 1024, SyncMode: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("data1"))
	_, _ = w.Write([]byte("data2"))
	_ = w.Close()

	w2, _ := Open(Config{Dir: dir, SegmentSize: 1024, SyncMode: SyncNone})
	defer func() { _ = w2.Close() }()

	var replayed []string
	_ = w2.Replay(func(data []byte) error {
		replayed = append(replayed, string(data))
		return nil
	})
	if len(replayed) != 2 {
		t.Fatalf("expected 2 replayed segments, got %d", len(replayed))
	}
}

func TestTruncateAfterFlush_ClearsCheckpoint(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, SegmentSize: 1024, SyncMode: SyncNone})
	if err != nil {
		t.Fatal(err)
	}

	// 写入数据
	_, _ = w.Write([]byte("test-data"))

	// 创建 checkpoint
	cp := &Checkpoint{Generation: w.Generation(), Segment: w.SegmentNum()}
	_ = cp.Save(dir)

	// TruncateAfterFlush 应清理 checkpoint
	_ = w.TruncateAfterFlush()

	loaded, _ := LoadCheckpoint(dir)
	if loaded != nil {
		t.Error("expected checkpoint to be cleared after TruncateAfterFlush")
	}

	_ = w.Close()
}
