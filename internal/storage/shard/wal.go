// internal/storage/shard/wal.go
package shard

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage"
	"codeberg.org/micro-ts/mts/types"
)

// WAL Write-Ahead Log 实现。
//
// 功能：
//
//   - 提供先写日志的持久化保证
//   - 支持缓冲写入提高性能
//   - 支持定期同步和显式同步
//
// 数据格式：
//
//	[length:4 bytes][data:N bytes]
//	length 为 uint32 大端序
//
// 字段说明：
//
//   - dir:    WAL 文件所在目录
//   - seq:    文件序列号
//   - file:   底层文件句柄
//   - mu:     保护并发访问的锁
//   - buf:    4KB 写缓冲区
//   - pos:    缓冲区当前位置
//   - logger: 日志记录器
//
// 使用模式：
//
//	wal.Write(data)
//	wal.StartPeriodicSync(5*time.Second, done)
//	// ...
//	wal.Close()
//
// 恢复：
//
//	使用 ReplayWAL 函数在启动时恢复数据点。
type WAL struct {
	dir    string
	seq    uint64
	file   *os.File
	mu     sync.Mutex
	buf    []byte
	pos    int
	logger *slog.Logger
}

// NewWAL 创建新的 WAL 实例。
//
// 参数：
//   - dir: WAL 存储目录
//   - seq: 序列号（用于生成文件名）
//
// 返回：
//   - *WAL: 初始化的 WAL
//   - error: 创建失败时返回错误
//
// 默认行为：
//
//	使用 slog.Default() 作为日志记录器。
//	如果需要自定义日志，使用 NewWALWithLogger。
func NewWAL(dir string, seq uint64) (*WAL, error) {
	return NewWALWithLogger(dir, seq, slog.Default())
}

// NewWALWithLogger 创建 WAL 并指定自定义日志记录器。
//
// 参数：
//   - dir:    WAL 存储目录
//   - seq:    序列号
//   - logger: 日志记录器
//
// 返回：
//   - *WAL:   WAL 实例
//   - error:  创建失败时返回错误
//
// 文件创建：
//
//	目录：dir（自动创建，权限 0700）
//	文件：{seq:020d}.wal（如 00000000000000000001.wal）
//	权限：0600
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
//
// 参数：
//   - data: 要写入的数据
//
// 返回：
//   - int:   成功写入的数据字节数（不含长度前缀）
//   - error: 写入失败时返回错误
//
// 写入格式：
//
//	[length:4][data:N]
//	length 为 uint32 大端序，表示跟随的数据长度
//
// 缓冲策略：
//
//	数据先写入 4KB 缓冲区，缓冲区满时刷到文件。
//	应用程序应定期调用 Sync() 或启动 StartPeriodicSync 确保持久化。
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

// ErrShortWrite 表示写入的字节数少于预期。
//
// 通常意味着磁盘满或 IO 错误。
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

// Sync 将缓冲数据刷盘。
//
// 返回：
//   - error: 刷盘失败时返回错误
//
// 保证：
//
//   - Sync 返回后，之前的所有 Write 数据都已持久化到磁盘。
//   - 操作系统崩溃不会导致数据丢失（fsync 语义）。
//
// 性能：
//
//	同步写入会显著降低吞吐量。
//	建议使用 StartPeriodicSync 进行定期同步，平衡性能和数据安全。
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushLocked(); err != nil {
		return err
	}
	return w.file.Sync()
}

// StartPeriodicSync 启动定期同步的 goroutine。
//
// 参数：
//   - interval: 同步间隔
//   - done:     关闭信号通道，接收到信号后停止同步
//
// 使用场景：
//
//	后台定期刷盘，避免每次写入都同步的性能损失。
//	提供折中的数据安全性：最坏情况下丢失一个间隔内的数据。
//
// 使用示例：
//
//	done := make(chan struct{})
//	wal.StartPeriodicSync(5*time.Second, done)
//	...
//	close(done) // 停止同步 goroutine
//
// 错误处理：
//
//	sync 失败会记录 Error 日志，不会中断同步循环。
func (w *WAL) StartPeriodicSync(interval time.Duration, done <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := w.Sync(); err != nil {
					w.logger.Error("wal sync failed", slog.Any("error", err))
				}
			case <-done:
				return
			}
		}
	}()
}

// Sequence 返回当前 WAL 的序列号。
//
// 返回：
//   - uint64: WAL 序列号，用于日志轮转或标识
func (w *WAL) Sequence() uint64 {
	return w.seq
}

// Close 关闭 WAL，释放资源。
//
// 返回：
//   - error: 关闭失败时返回错误
//
// 关闭流程：
//
//  1. 刷出缓冲区中的所有数据
//  2. 执行 fsync 确保持久化
//  3. 关闭文件句柄
//
// 注意：
//
//	关闭后 WAL 不可再使用。
//	关闭后会丢失未同步的数据。
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

// estimateSerializedSize 预估序列化后的大小
func estimateSerializedSize(p *types.Point) int {
	size := 8 + 4 + 4 // timestamp + tag_len + field_count

	// tags: key\x00value\x00...
	for k, v := range p.Tags {
		size += len(k) + len(v) + 2
	}

	// fields: [4 bytes: key_len][N bytes: key][1 byte: type][N bytes: value]
	for k, v := range p.Fields {
		size += 4 + len(k) + 1 // key_len + key + type
		switch v.(type) {
		case float64, int64:
			size += 8
		case string:
			size += 4 + len(v.(string))
		case bool:
			size += 1
		}
	}

	return size
}

func serializePoint(p *types.Point) ([]byte, error) {
	// 预分配精确大小的 buffer，避免动态增长
	buf := make([]byte, 0, estimateSerializedSize(p))

	// timestamp
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(p.Timestamp))
	buf = append(buf, ts[:]...)

	// tags: [4 bytes: tag_len][N bytes: tag_data]
	// 先计算 tag_data 长度
	tagLen := 0
	for k, v := range p.Tags {
		tagLen += len(k) + len(v) + 2
	}
	var tl [4]byte
	binary.BigEndian.PutUint32(tl[:], uint32(tagLen))
	buf = append(buf, tl[:]...)

	// 直接写入 tag_data，避免中间切片
	for k, v := range p.Tags {
		buf = append(buf, k...)
		buf = append(buf, 0)
		buf = append(buf, v...)
		buf = append(buf, 0)
	}

	// fields: [4 bytes: field_count]
	var fc [4]byte
	binary.BigEndian.PutUint32(fc[:], uint32(len(p.Fields)))
	buf = append(buf, fc[:]...)

	// field entries
	for k, v := range p.Fields {
		// key: [4 bytes: key_len][N bytes: key]
		var kl [4]byte
		binary.BigEndian.PutUint32(kl[:], uint32(len(k)))
		buf = append(buf, kl[:]...)
		buf = append(buf, k...)

		// value: [1 byte: type][N bytes: value]
		switch val := v.(type) {
		case float64:
			buf = append(buf, 0)
			var vb [8]byte
			binary.BigEndian.PutUint64(vb[:], math.Float64bits(val))
			buf = append(buf, vb[:]...)
		case int64:
			buf = append(buf, 1)
			var vb [8]byte
			binary.BigEndian.PutUint64(vb[:], uint64(val))
			buf = append(buf, vb[:]...)
		case string:
			buf = append(buf, 2)
			var vl [4]byte
			binary.BigEndian.PutUint32(vl[:], uint32(len(val)))
			buf = append(buf, vl[:]...)
			buf = append(buf, val...)
		case bool:
			buf = append(buf, 3)
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
	if len(data) < 16 {
		return nil, fmt.Errorf("data too short: %d bytes", len(data))
	}

	pos := 0

	// timestamp
	ts := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8

	// tags
	tagLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	// 预分配 map 容量（估算：平均每个 tag 约 10+10=20 bytes，所以 tagLen/20 个 entries）
	estimatedTags := tagLen / 20
	if estimatedTags < 1 {
		estimatedTags = 1
	}
	tags := make(map[string]string, estimatedTags)

	if tagLen > 0 {
		if pos+tagLen > len(data) {
			return nil, fmt.Errorf("invalid tag length: %d", tagLen)
		}
		tagData := data[pos : pos+tagLen]
		pos += tagLen

		// 内联解析，避免 bytesSplit 分配
		start := 0
		var key string
		for i := 0; i <= len(tagData); i++ {
			if i == len(tagData) || tagData[i] == 0 {
				if key == "" {
					// 这是 key
					key = string(tagData[start:i])
				} else {
					// 这是 value
					tags[key] = string(tagData[start:i])
					key = ""
				}
				start = i + 1
			}
		}
	}

	// fields
	if pos+4 > len(data) {
		return nil, fmt.Errorf("data too short for field count")
	}
	fieldCount := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	fields := make(map[string]any, fieldCount)

	for i := 0; i < fieldCount; i++ {
		if pos+4 > len(data) {
			return nil, fmt.Errorf("data too short for field key length")
		}
		keyLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4

		if pos+keyLen+1 > len(data) {
			return nil, fmt.Errorf("data too short for field key")
		}
		key := string(data[pos : pos+keyLen])
		pos += keyLen

		typ := data[pos]
		pos++

		switch typ {
		case 0: // float64
			if pos+8 > len(data) {
				return nil, fmt.Errorf("data too short for float64 value")
			}
			val := math.Float64frombits(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			fields[key] = val
		case 1: // int64
			if pos+8 > len(data) {
				return nil, fmt.Errorf("data too short for int64 value")
			}
			val := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			fields[key] = val
		case 2: // string
			if pos+4 > len(data) {
				return nil, fmt.Errorf("data too short for string length")
			}
			valLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if pos+valLen > len(data) {
				return nil, fmt.Errorf("data too short for string value")
			}
			val := string(data[pos : pos+valLen])
			pos += valLen
			fields[key] = val
		case 3: // bool
			if pos+1 > len(data) {
				return nil, fmt.Errorf("data too short for bool value")
			}
			val := data[pos] == 1
			pos++
			fields[key] = val
		default:
			return nil, fmt.Errorf("unknown field type: %d", typ)
		}
	}

	return &types.Point{
		Timestamp: ts,
		Tags:      tags,
		Fields:    fields,
	}, nil
}

// ReplayWAL 重放 WAL 文件，恢复数据点。
//
// 参数：
//   - walDir: WAL 目录路径
//
// 返回：
//   - []*types.Point: 恢复的数据点
//   - error:          重放失败时返回错误
//
// 重放过程：
//
//  1. 扫描目录下所有 .wal 文件
//  2. 按顺序读取每个文件
//  3. 解析 length-prefixed 数据
//  4. 反序列化为 Point
//
// 错误处理：
//
//	文件读取错误会跳过该文件。
//	数据解析错误会跳过该条目。
//	尽可能恢复有效数据。
//
// 使用场景：
//
//	数据库启动时调用，恢复崩溃前的未刷盘数据。
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
