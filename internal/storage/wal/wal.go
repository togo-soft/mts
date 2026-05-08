package wal

import (
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage"
)

// ErrWALClosed 表示 WAL 已关闭。
var ErrWALClosed = errors.New("wal closed")

// SyncMode 定义同步模式。
type SyncMode int

const (
	SyncNone     SyncMode = iota // 不主动 fsync
	SyncPeriodic                 // 定时 fsync（默认）
	SyncEvery                    // 每次写入 fsync
)

// Config 是 WAL 实例的配置。
type Config struct {
	Dir          string
	SegmentSize  int64 // 默认 64MB
	MaxSegments  int   // 0 = 无限制
	SyncMode     SyncMode
	SyncInterval time.Duration // SyncPeriodic 的间隔，默认 1 秒
	Logger       *slog.Logger
}

func (c *Config) normalize() {
	if c.SegmentSize <= 0 {
		c.SegmentSize = 64 * 1024 * 1024
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.SyncInterval <= 0 {
		c.SyncInterval = time.Second
	}
}

// WAL 是 Write-Ahead Log 实例。
type WAL struct {
	dir      string
	gen      uint64 // 当前世代
	segNum   uint64 // 当前 segment 序号
	seg      *segment
	mu       sync.Mutex
	buf      []byte // 聚合写缓冲
	bufPos   int
	cfg      Config
	closed   atomic.Bool
	syncDone chan struct{} // 停止周期性同步
}

// Open 打开或创建 WAL。
func Open(cfg Config) (*WAL, error) {
	cfg.normalize()

	if err := storage.SafeMkdirAll(cfg.Dir, 0700); err != nil {
		return nil, err
	}

	w := &WAL{
		dir:      cfg.Dir,
		buf:      make([]byte, 64*1024), // 64KB 写缓冲
		cfg:      cfg,
		syncDone: make(chan struct{}),
	}

	// 发现现有 segment，确定 generation 和 segment 号
	entries, err := listSegments(cfg.Dir)
	if err != nil {
		return nil, err
	}

	if len(entries) > 0 {
		last := entries[len(entries)-1]
		w.gen = last.Gen
		w.segNum = last.Num + 1
	} else {
		w.gen = uint64(time.Now().Unix())
		w.segNum = 1
	}

	seg, err := openSegment(cfg.Dir, w.gen, w.segNum)
	if err != nil {
		return nil, err
	}
	w.seg = seg

	if cfg.MaxSegments > 0 && len(entries) >= cfg.MaxSegments {
		cfg.Logger.Warn("WAL segment count at limit", "count", len(entries)+1, "max", cfg.MaxSegments)
	}

	if cfg.SyncMode == SyncPeriodic {
		w.startPeriodicSync()
	}

	return w, nil
}

// Write 写入一条记录到 WAL。返回写入的 payload 长度。
func (w *WAL) Write(data []byte) (int, error) {
	if w.closed.Load() {
		return 0, ErrWALClosed
	}

	recordSize := RecordSize(len(data))
	record := make([]byte, recordSize)
	record = EncodeRecord(record, TypePointData, data)

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.seg.size+int64(w.bufPos)+int64(len(record)) > w.cfg.SegmentSize {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
	}

	if len(record) >= len(w.buf) {
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
		if _, err := w.seg.Write(record); err != nil {
			return 0, err
		}
	} else {
		if len(w.buf)-w.bufPos < len(record) {
			if err := w.flushLocked(); err != nil {
				return 0, err
			}
		}
		copy(w.buf[w.bufPos:], record)
		w.bufPos += len(record)
	}

	if w.cfg.SyncMode == SyncEvery {
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
		if err := w.seg.Sync(); err != nil {
			return 0, err
		}
	}

	return len(data), nil
}

// WriteBatch 批量写入多条记录，一次获取锁。
func (w *WAL) WriteBatch(data [][]byte) (int, error) {
	if w.closed.Load() {
		return 0, ErrWALClosed
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	var total int
	for _, d := range data {
		recordSize := RecordSize(len(d))
		record := make([]byte, recordSize)
		record = EncodeRecord(record, TypePointData, d)

		if w.seg.size+int64(w.bufPos)+int64(len(record)) > w.cfg.SegmentSize {
			if err := w.rotateLocked(); err != nil {
				return total, err
			}
		}

		if len(record) >= len(w.buf) {
			if err := w.flushLocked(); err != nil {
				return total, err
			}
			if _, err := w.seg.Write(record); err != nil {
				return total, err
			}
		} else {
			if len(w.buf)-w.bufPos < len(record) {
				if err := w.flushLocked(); err != nil {
					return total, err
				}
			}
			copy(w.buf[w.bufPos:], record)
			w.bufPos += len(record)
		}
		total += len(d)
	}

	if w.cfg.SyncMode == SyncEvery {
		if err := w.flushLocked(); err != nil {
			return total, err
		}
		if err := w.seg.Sync(); err != nil {
			return total, err
		}
	}

	return total, nil
}

// Sync 强制刷盘。
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}
	return w.seg.Sync()
}

// TruncateCurrent 截断当前 segment（flush 后调用）。
func (w *WAL) TruncateCurrent() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}

	if err := Cleanup(w.dir, w.gen); err != nil {
		w.cfg.Logger.Warn("WAL cleanup failed", "error", err)
	}

	return w.seg.Truncate()
}

// Close 关闭 WAL。
func (w *WAL) Close() error {
	if w.closed.Swap(true) {
		return nil
	}

	if w.cfg.SyncMode == SyncPeriodic && w.syncDone != nil {
		close(w.syncDone)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}
	if err := w.seg.Sync(); err != nil {
		return err
	}
	return w.seg.Close()
}

// Generation 返回当前世代号。
func (w *WAL) Generation() uint64 {
	return w.gen
}

// SegmentNum 返回当前 segment 序号。
func (w *WAL) SegmentNum() uint64 {
	return w.segNum
}

// Replay 流式回放所有 WAL segment。
func (w *WAL) Replay(fn func(payload []byte) error) error {
	entries, err := listSegments(w.dir)
	if err != nil {
		return err
	}

	cp, err := loadCheckpoint(w.dir)
	if err != nil {
		w.cfg.Logger.Warn("failed to load WAL checkpoint", "error", err)
		cp = &Checkpoint{}
	}

	var count int64
	for _, e := range entries {
		if e.Gen < cp.Generation {
			continue
		}
		if e.Gen == cp.Generation && e.Num < cp.Segment {
			continue
		}

		startPos := int64(segmentHeaderSize)
		if e.Gen == cp.Generation && e.Num == cp.Segment && cp.Position > startPos {
			startPos = cp.Position
		}

		file, err := os.Open(e.Path)
		if err != nil {
			w.cfg.Logger.Warn("failed to open WAL segment for replay", "path", e.Path, "error", err)
			continue
		}

		if startPos == int64(segmentHeaderSize) {
			if _, err := file.Seek(0, 0); err != nil {
				_ = file.Close()
				return err
			}
			if _, _, err := readSegmentHeader(file); err != nil {
				_ = file.Close()
				w.cfg.Logger.Warn("failed to read WAL segment header", "path", e.Path, "error", err)
				continue
			}
		}

		pos, err := readRecords(file, startPos, fn)
		_ = file.Close()
		if err != nil {
			w.cfg.Logger.Warn("WAL replay encountered error", "path", e.Path, "error", err)
		}

		cp = &Checkpoint{Generation: e.Gen, Segment: e.Num, Position: pos}
		count++
		if count%1000 == 0 {
			if err := saveCheckpoint(w.dir, cp); err != nil {
				w.cfg.Logger.Warn("failed to save WAL checkpoint", "error", err)
			}
		}
	}

	if err := saveCheckpoint(w.dir, cp); err != nil {
		w.cfg.Logger.Warn("failed to save WAL checkpoint", "error", err)
	}
	return nil
}

// rotateLocked 轮转到新 segment（需持有 w.mu）。
func (w *WAL) rotateLocked() error {
	if err := w.flushLocked(); err != nil {
		return err
	}
	if err := w.seg.Sync(); err != nil {
		return err
	}
	if err := w.seg.Close(); err != nil {
		return err
	}

	w.segNum++
	seg, err := openSegment(w.dir, w.gen, w.segNum)
	if err != nil {
		return err
	}
	w.seg = seg
	return nil
}

// flushLocked 刷写缓冲（需持有 w.mu）。
func (w *WAL) flushLocked() error {
	if w.bufPos == 0 {
		return nil
	}
	n, err := w.seg.Write(w.buf[:w.bufPos])
	if err != nil {
		return err
	}
	if n != w.bufPos {
		return ErrShortWrite
	}
	w.bufPos = 0
	return nil
}

// startPeriodicSync 启动周期性 fsync goroutine。
func (w *WAL) startPeriodicSync() {
	go func() {
		ticker := time.NewTicker(w.cfg.SyncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := w.Sync(); err != nil {
					w.cfg.Logger.Error("wal periodic sync failed", "error", err)
				}
			case <-w.syncDone:
				return
			}
		}
	}()
}
