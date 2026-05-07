package shard

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage"
)

// WAL 文件大小阈值，达到后滚动到新文件。
const walFileSizeLimit = 64 * 1024 * 1024 // 64MB

// WAL Write-Ahead Log 实现。
type WAL struct {
	dir      string
	seq      uint64
	file     *os.File
	mu       sync.Mutex
	buf      []byte
	pos      int
	logger   *slog.Logger
	fileSize int64
}

// WALReplayCheckpoint 记录 WAL replay 的进度，用于增量 replay。
type WALReplayCheckpoint struct {
	LastSeq uint64
	LastPos int64
	Updated int64
}

func (c *WALReplayCheckpoint) filePath(walDir string) string {
	return filepath.Join(walDir, "_replay_checkpoint.json")
}

// Save 保存 checkpoint 到文件。
func (c *WALReplayCheckpoint) Save(walDir string) error {
	c.Updated = time.Now().UnixNano()
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	path := c.filePath(walDir)
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	return nil
}

// Load 从文件加载 checkpoint。
func (c *WALReplayCheckpoint) Load(walDir string) error {
	path := c.filePath(walDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read checkpoint: %w", err)
	}
	if err := json.Unmarshal(data, c); err != nil {
		return fmt.Errorf("unmarshal checkpoint: %w", err)
	}
	return nil
}

// NewWAL 创建新的 WAL 实例。
func NewWAL(dir string, seq uint64) (*WAL, error) {
	return NewWALWithLogger(dir, seq, slog.Default())
}

// NewWALWithLogger 创建 WAL 并指定自定义日志记录器。
func NewWALWithLogger(dir string, seq uint64, logger *slog.Logger) (*WAL, error) {
	if err := storage.SafeMkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	filename := filepath.Join(dir, padSeq(seq)+".wal")
	f, err := storage.SafeOpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	return &WAL{
		dir:    dir,
		seq:    seq,
		file:   f,
		buf:    make([]byte, 4096),
		logger: logger,
	}, nil
}

func padSeq(seq uint64) string {
	return fmt.Sprintf("%020d", seq)
}

// Write 写入数据到 WAL。
func (w *WAL) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	need := 4 + len(data)

	if int64(need) >= walFileSizeLimit-w.fileSize {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
	}

	if len(data) >= len(w.buf) {
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
		var lengthBuf [4]byte
		binary.BigEndian.PutUint32(lengthBuf[:], uint32(len(data)))
		if _, err := w.file.Write(lengthBuf[:]); err != nil {
			return 0, err
		}
		w.fileSize += 4
		if _, err := w.file.Write(data); err != nil {
			return 0, err
		}
		w.fileSize += int64(len(data))
		return len(data), nil
	}

	if len(w.buf)-w.pos < need {
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
	}

	binary.BigEndian.PutUint32(w.buf[w.pos:], uint32(len(data)))
	w.pos += 4

	copy(w.buf[w.pos:], data)
	w.pos += len(data)

	return len(data), nil
}

// rotateLocked 滚动到新的 WAL 文件（需持有 w.mu）。
func (w *WAL) rotateLocked() error {
	if err := w.flushLocked(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}

	w.seq++
	filename := filepath.Join(w.dir, padSeq(w.seq)+".wal")
	f, err := storage.SafeOpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	w.file = f
	w.fileSize = 0
	return nil
}

// ErrShortWrite 表示写入的字节数少于预期。
var ErrShortWrite = fmt.Errorf("short write")

func (w *WAL) flushLocked() error {
	if w.pos == 0 {
		return nil
	}

	n, err := w.file.Write(w.buf[:w.pos])
	if err != nil {
		return err
	}
	if n != w.pos {
		return ErrShortWrite
	}
	w.fileSize += int64(w.pos)
	w.pos = 0
	return nil
}

// Sync 将缓冲数据刷盘。
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}
	return w.file.Sync()
}
