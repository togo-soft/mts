// internal/storage/shard/wal.go
package shard

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"micro-ts/internal/storage"
)

// WAL Write-Ahead Log
type WAL struct {
	dir  string
	seq  uint64
	file *os.File
	mu   sync.Mutex
	buf  []byte
	pos  int
}

// NewWAL 创建 WAL
func NewWAL(dir string, seq uint64) (*WAL, error) {
	if err := storage.SafeMkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	filename := filepath.Join(dir, padSeq(seq)+".wal")
	f, err := storage.SafeOpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	return &WAL{
		dir:  dir,
		seq:  seq,
		file: f,
		buf:  make([]byte, 4096),
	}, nil
}

func padSeq(seq uint64) string {
	return fmt.Sprintf("%020d", seq)
}

// Write 写入数据
func (w *WAL) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 计算需要的空间：4 bytes length + data
	need := 4 + len(data)
	if len(w.buf)-w.pos < need {
		// 刷新到文件
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
	}

	// 写入长度
	binary.BigEndian.PutUint32(w.buf[w.pos:], uint32(len(data)))
	w.pos += 4

	// 写入数据
	copy(w.buf[w.pos:], data)
	w.pos += len(data)

	return len(data), nil
}

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
	w.pos = 0
	return nil
}

// Sync 刷盘
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Sequence 返回当前序列号
func (w *WAL) Sequence() uint64 {
	return w.seq
}

// Close 关闭
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}
