// internal/storage/shard/wal.go
package shard

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"micro-ts/internal/storage"
	"micro-ts/internal/types"
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

// StartPeriodicSync 启动定期 Sync goroutine
func (w *WAL) StartPeriodicSync(interval time.Duration, done <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := w.Sync(); err != nil {
					// log error but don't stop
					fmt.Printf("WAL Sync error: %v\n", err)
				}
			case <-done:
				return
			}
		}
	}()
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

// Point 序列化格式:
// [8 bytes: timestamp][4 bytes: tag_len][N bytes: tags][4 bytes: field_count]
// field: [4 bytes: key_len][N bytes: key][1 byte: type][N bytes: value]

func serializePoint(p *types.Point) ([]byte, error) {
	var buf []byte

	// timestamp
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(p.Timestamp))
	buf = append(buf, ts[:]...)

	// tags 简化处理：key\x00value\x00key\x00value...
	var tagData []byte
	for k, v := range p.Tags {
		tagData = append(tagData, []byte(k)...)
		tagData = append(tagData, 0)
		tagData = append(tagData, []byte(v)...)
		tagData = append(tagData, 0)
	}
	var tagLen [4]byte
	binary.BigEndian.PutUint32(tagLen[:], uint32(len(tagData)))
	buf = append(buf, tagLen[:]...)
	buf = append(buf, tagData...)

	// fields
	var fieldCount [4]byte
	binary.BigEndian.PutUint32(fieldCount[:], uint32(len(p.Fields)))
	buf = append(buf, fieldCount[:]...)

	for k, v := range p.Fields {
		// key
		keyBytes := []byte(k)
		var keyLen [4]byte
		binary.BigEndian.PutUint32(keyLen[:], uint32(len(keyBytes)))
		buf = append(buf, keyLen[:]...)
		buf = append(buf, keyBytes...)

		// value
		switch val := v.(type) {
		case float64:
			buf = append(buf, 0) // type: float64
			var vbits [8]byte
			binary.BigEndian.PutUint64(vbits[:], math.Float64bits(val))
			buf = append(buf, vbits[:]...)
		case int64:
			buf = append(buf, 1) // type: int64
			var vbits [8]byte
			binary.BigEndian.PutUint64(vbits[:], uint64(val))
			buf = append(buf, vbits[:]...)
		case string:
			buf = append(buf, 2) // type: string
			var vLen [4]byte
			binary.BigEndian.PutUint32(vLen[:], uint32(len(val)))
			buf = append(buf, vLen[:]...)
			buf = append(buf, val...)
		case bool:
			buf = append(buf, 3) // type: bool
			if val {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		}
	}

	return buf, nil
}

func deserializePoint(data []byte) (*types.Point, error) {
	pos := 0

	// timestamp
	ts := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8

	// tags
	tagLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	tags := make(map[string]string)
	if tagLen > 0 {
		tagData := data[pos : pos+tagLen]
		pos += tagLen
		// 解析 key\x00value\x00...
		parts := bytesSplit(tagData, 0)
		for i := 0; i+1 < len(parts); i += 2 {
			tags[string(parts[i])] = string(parts[i+1])
		}
	}

	// fields
	fieldCount := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	fields := make(map[string]any)
	for i := 0; i < fieldCount; i++ {
		// key
		keyLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		key := string(data[pos : pos+keyLen])
		pos += keyLen

		// value
		typ := data[pos]
		pos++

		switch typ {
		case 0: // float64
			val := math.Float64frombits(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			fields[key] = val
		case 1: // int64
			val := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			fields[key] = val
		case 2: // string
			valLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			val := string(data[pos : pos+valLen])
			pos += valLen
			fields[key] = val
		case 3: // bool
			val := data[pos] == 1
			pos++
			fields[key] = val
		}
	}

	return &types.Point{
		Timestamp: ts,
		Tags:      tags,
		Fields:    fields,
	}, nil
}

func bytesSplit(data []byte, sep byte) [][]byte {
	var result [][]byte
	var start int
	for i := 0; i < len(data); i++ {
		if data[i] == sep {
			result = append(result, data[start:i])
			start = i + 1
		}
	}
	result = append(result, data[start:])
	return result
}

// ReplayWAL 重放 WAL 文件
func ReplayWAL(walDir string) ([]*types.Point, error) {
	files, err := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if err != nil {
		return nil, err
	}

	var points []*types.Point
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}

		pos := 0
		for pos < len(data) {
			if pos+4 > len(data) {
				break
			}
			size := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4

			if pos+size > len(data) {
				break
			}
			p, err := deserializePoint(data[pos : pos+size])
			if err != nil {
				pos += size
				continue
			}
			points = append(points, p)
			pos += size
		}
	}

	return points, nil
}
