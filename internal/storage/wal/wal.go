package wal

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage"
)

const (
	defaultSegmentSize  = 64 * 1024 * 1024 // 默认 segment 大小 64MB
	writeBufSize        = 1 * 1024 * 1024  // 写缓冲大小 1MB，减少慢 I/O 平台上 file.Write 频率
	defaultSyncInterval = time.Second      // 默认同步间隔
)

// ErrWALClosed 表示 WAL 已关闭。
var ErrWALClosed = errors.New("wal closed")

// GlobalDir 返回全局 WAL 目录路径
func GlobalDir(dataDir string) string {
	return filepath.Join(dataDir, "wal")
}

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
	Compressed   bool          // 是否启用 LZ4 压缩，默认 true
	Logger       *slog.Logger
}

func (c *Config) normalize() {
	if c.SegmentSize <= 0 {
		c.SegmentSize = defaultSegmentSize
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.SyncInterval <= 0 {
		c.SyncInterval = defaultSyncInterval
	}
	// 默认启用压缩
	if !c.Compressed {
		c.Compressed = true
	}
}

// WAL 是 Write-Ahead Log 实例。
type WAL struct {
	dir            string
	gen            uint64 // 当前世代
	segNum         uint64 // 当前 segment 序号
	seg            *segment
	mu             sync.Mutex
	buf            []byte // 聚合写缓冲
	bufPos         int
	cfg            Config
	closed         atomic.Bool
	segClosed      atomic.Bool   // segment 是否已关闭
	syncDone       chan struct{} // 停止周期性同步
	syncDoneClosed atomic.Bool   // syncDone 是否已关闭
	replayedSegs   int           // replay 过程中发现的 segment 数量
	compressed     bool          // 是否启用压缩
}

// Open 打开或创建 WAL。
func Open(cfg Config) (*WAL, error) {
	cfg.normalize()

	slog.Debug("wal.Open: creating dir", "dir", cfg.Dir)
	if err := storage.SafeMkdirAll(cfg.Dir, 0700); err != nil {
		return nil, err
	}

	w := &WAL{
		dir:        cfg.Dir,
		buf:        make([]byte, writeBufSize), // 64KB 写缓冲
		cfg:        cfg,
		syncDone:   make(chan struct{}),
		compressed: cfg.Compressed,
	}

	// 发现现有 segment，确定 generation 和 segment 号
	entries, err := ListSegments(cfg.Dir)
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

	seg, err := openSegment(cfg.Dir, w.gen, w.segNum, w.compressed)
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

	// 压缩 payload
	var payload []byte
	var releasePayload func()
	if w.compressed {
		var err error
		payload, releasePayload, err = CompressPayload(data)
		if err != nil {
			return 0, err
		}
	}
	if payload == nil {
		payload = data
	}

	// 编码记录（池化缓冲区避免分配）
	recordSize := RecordSize(len(payload))
	recordBuf := getBuf(recordSize)
	if cap(recordBuf) < recordSize {
		recordBuf = make([]byte, recordSize)
	}
	record := EncodeRecord(recordBuf[:recordSize], TypePointData, payload)

	// 编码完成后释放压缩缓冲区
	if releasePayload != nil {
		releasePayload()
	}

	w.mu.Lock()
	defer func() {
		w.mu.Unlock()
		putBuf(recordBuf)
	}()

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
	var recordBuf []byte // 跨迭代复用，避免每次分配
	for _, d := range data {
		// 压缩 payload
		var payload []byte
		var releasePayload func()
		if w.compressed {
			var err error
			payload, releasePayload, err = CompressPayload(d)
			if err != nil {
				return total, err
			}
		}
		if payload == nil {
			payload = d
		}

		// 编码记录（复用 recordBuf，仅在容量不足时扩容）
		recordSize := RecordSize(len(payload))
		if cap(recordBuf) < recordSize {
			recordBuf = make([]byte, recordSize)
		}
		record := EncodeRecord(recordBuf[:recordSize], TypePointData, payload)

		// 编码完成后释放压缩缓冲区
		if releasePayload != nil {
			releasePayload()
		}

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
	if w.seg == nil {
		return nil
	}
	return w.seg.Sync()
}

// TruncateCurrent 清理当前 segment（flush 后调用）。
// 清理当前 generation 的旧 segment 文件，但保留当前 segment。
func (w *WAL) TruncateCurrent() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}

	// 当前 segment 已 sync，数据安全
	// 删除当前 generation 的所有旧 segment（segNum < w.segNum）
	entries, err := ListSegments(w.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Gen == w.gen && e.Num < w.segNum {
			if rmErr := os.Remove(e.Path); rmErr != nil {
				w.cfg.Logger.Warn("failed to remove old WAL segment", "path", e.Path, "error", rmErr)
			}
		}
	}
	return nil
}

// TruncateAfterFlush 在 MemTable flush 到 SSTable 后清理 WAL segment。
//
// 安全性：flush 期间 Shard 持有写锁，保证无并发写入。
// flush 后所有 WAL 数据已持久化到 SSTable，全部 segment 可安全删除。
//
// 操作：flush buffer → rotate（创建新空 segment）→ 删除所有旧 segment。
// 调用后 WAL 保持打开状态，新写入进入全新的空 segment。
func (w *WAL) TruncateAfterFlush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed.Load() {
		return nil
	}

	// 先 rotate：flush buffer → sync → close 当前 → 创建新空 segment
	if err := w.rotateLocked(); err != nil {
		return err
	}

	// 删除所有旧 segment（仅保留 rotateLocked 刚创建的新空 segment）
	entries, err := ListSegments(w.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Gen == w.gen && e.Num == w.segNum {
			continue
		}
		if rmErr := os.Remove(e.Path); rmErr != nil {
			w.cfg.Logger.Warn("failed to remove old WAL segment", "path", e.Path, "error", rmErr)
		}
	}

	_ = ClearCheckpoint(w.dir)

	return nil
}

// TruncateBefore 删除编号小于 seq 的所有 WAL 段（flush 完成后调用）
func (w *WAL) TruncateBefore(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, err := ListSegments(w.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Gen == w.gen && e.Num < seq {
			if err := os.Remove(e.Path); err != nil && !os.IsNotExist(err) {
				if w.cfg.Logger != nil {
					w.cfg.Logger.Warn("failed to remove WAL segment", "path", e.Path, "error", err)
				}
			}
		}
	}
	return nil
}

// Purge 删除当前 WAL 的所有 segment 文件。
// 调用前应确保数据已 flush 到 SSTable。
//
// Purge 会先关闭当前 WAL（flush buffer + sync + close），
// 然后删除所有 segment 文件，重置 WAL 状态。
// 调用后 WAL 处于关闭状态，不能继续使用。
//
// 注意：即使 WAL 已经关闭（closed=true），Purge 仍会删除 segment 文件。
func (w *WAL) Purge() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 如果 segment 存在，先将 buffer 中的数据刷写到 segment 并 sync
	// 确保所有缓冲数据都已持久化到磁盘，然后再删除文件
	if w.seg != nil {
		// 如果 WAL 未关闭，执行 flush 和 sync
		if !w.closed.Load() {
			if err := w.flushLocked(); err != nil {
				return err
			}
			if err := w.seg.Sync(); err != nil {
				return err
			}
		}
		// 关闭 segment（仅当未关闭时，避免 double-close）
		if !w.segClosed.Load() {
			if err := w.seg.Close(); err != nil {
				// 记录错误但继续执行删除
				w.cfg.Logger.Warn("failed to close WAL segment", "error", err)
			}
			w.segClosed.Store(true)
		}
		w.seg = nil
	}

	// 删除所有 segment 文件
	entries, err := ListSegments(w.dir)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if err := os.Remove(e.Path); err != nil {
			w.cfg.Logger.Warn("failed to remove WAL segment", "path", e.Path, "error", err)
		}
	}

	// 标记 WAL 为关闭状态（如果尚未关闭）
	w.closed.Store(true)

	// 停止周期性 sync goroutine（如果尚未停止）
	if w.cfg.SyncMode == SyncPeriodic && w.syncDone != nil {
		if w.syncDoneClosed.CompareAndSwap(false, true) {
			close(w.syncDone)
		}
	}

	return nil
}

// Close 关闭 WAL。
func (w *WAL) Close() error {
	if w.closed.Swap(true) {
		return nil
	}

	if w.cfg.SyncMode == SyncPeriodic && w.syncDone != nil {
		if w.syncDoneClosed.CompareAndSwap(false, true) {
			close(w.syncDone)
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}
	if w.seg == nil {
		return nil
	}
	if err := w.seg.Sync(); err != nil {
		return err
	}
	if err := w.seg.Close(); err != nil {
		return err
	}
	w.segClosed.Store(true)
	return nil
}

// Dir 返回 WAL 目录路径
func (w *WAL) Dir() string {
	return w.dir
}

// Generation 返回当前世代号。
func (w *WAL) Generation() uint64 {
	return w.gen
}

// SegmentNum 返回当前 segment 序号。
func (w *WAL) SegmentNum() uint64 {
	return w.segNum
}

// SetSegmentNum 设置当前 segment 序号。
// 用于在 replay 完成后更新，以便 TruncateCurrent 知道哪些 segment 已处理。
func (w *WAL) SetSegmentNum(segNum uint64) {
	w.segNum = segNum
}

// ReplayedSegments 返回 replay 过程中发现的 segment 数量。
func (w *WAL) ReplayedSegments() int {
	return w.replayedSegs
}

// Replay 流式回放 WAL segment。
//
// 通过 checkpoint 跳过已持久化到 SSTable 的旧 segment，减少重启恢复时间。
// 去重由上层（memtable）通过 timestamp + tags 处理。
func (w *WAL) Replay(fn func(payload []byte) error) error {
	entries, err := ListSegments(w.dir)
	if err != nil {
		return err
	}

	// 加载 checkpoint，跳过已完成 flush 的旧 segment
	cp, _ := LoadCheckpoint(w.dir)
	if cp != nil {
		w.cfg.Logger.Info("WAL checkpoint found, skipping persisted segments",
			"checkpoint_gen", cp.Generation,
			"checkpoint_seg", cp.Segment)
	}

	var toReplay []segmentEntry
	if cp != nil {
		for _, e := range entries {
			if e.Gen == cp.Generation && e.Num <= cp.Segment {
				continue
			}
			toReplay = append(toReplay, e)
		}
	} else {
		toReplay = entries
	}

	w.replayedSegs = len(toReplay)

	for _, e := range toReplay {
		file, err := os.Open(e.Path)
		if err != nil {
			w.cfg.Logger.Warn("failed to open WAL segment for replay", "path", e.Path, "error", err)
			continue
		}

		// 跳过 segment header，从数据部分开始读取
		if _, err := file.Seek(0, 0); err != nil {
			_ = file.Close()
			return err
		}
		_, _, compressed, err := readSegmentHeader(file)
		if err != nil {
			_ = file.Close()
			w.cfg.Logger.Warn("failed to read WAL segment header", "path", e.Path, "error", err)
			continue
		}

		_, err = readRecords(file, int64(segmentHeaderSize), fn, compressed)
		_ = file.Close()
		if err != nil {
			w.cfg.Logger.Warn("WAL replay encountered error", "path", e.Path, "error", err)
		}
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
	seg, err := openSegment(w.dir, w.gen, w.segNum, w.compressed)
	if err != nil {
		return err
	}
	w.seg = seg
	return nil
}

// Rotate 轮转到新 segment。
func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateLocked()
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
