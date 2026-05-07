// internal/storage/shard/wal.go
package shard

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"codeberg.org/micro-ts/mts/internal/storage"
	"codeberg.org/micro-ts/mts/types"
)

// WAL 文件大小阈值，达到后滚动到新文件。
const walFileSizeLimit = 64 * 1024 * 1024 // 64MB

// WAL Write-Ahead Log 实现。
//
// 功能：
//
//   - 提供先写日志的持久化保证
//   - 支持缓冲写入提高性能
//   - 支持定期同步和显式同步
//   - 支持文件滚动，避免单个文件过大
//
// 数据格式：
//
//	[length:4 bytes][data:N bytes]
//	length 为 uint32 大端序
//
// 字段说明：
//
//   - dir:       WAL 文件所在目录
//   - seq:       文件序列号
//   - file:      底层文件句柄
//   - mu:        保护并发访问的锁
//   - buf:       4KB 写缓冲区
//   - pos:       缓冲区当前位置
//   - logger:    日志记录器
//   - fileSize:  当前文件已写入的字节数（不含缓冲区）
//
// 使用模式：
//
//	wal.Write(data)
//	wal.StartPeriodicSync(5*time.Second, done)
//	// ...
//	wal.Close()
//
// 文件滚动：
//
//	当文件大小超过 walFileSizeLimit (64MB) 时，
//	自动创建新的 WAL 文件继续写入。
//	通过 Sequence() 获取当前 WAL 的序列号。
//
// 恢复：
//
//	使用 ReplayWAL 函数在启动时恢复数据点。
type WAL struct {
	dir      string
	seq      uint64
	file     *os.File
	mu       sync.Mutex
	buf      []byte
	pos      int
	logger   *slog.Logger
	fileSize int64 // 当前文件已写入的字节数
}

// WALReplayCheckpoint 记录 WAL replay 的进度，用于增量 replay。
type WALReplayCheckpoint struct {
	// LastSeq 是最后处理的 WAL 文件序列号
	LastSeq uint64
	// LastPos 是在 LastSeq 文件中已处理的字节偏移
	LastPos int64
	// Updated 是 checkpoint 更新时间戳
	Updated int64
}

// checkpoint 文件路径
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
//
// 文件滚动：
//
//	当文件大小超过 64MB 时，自动创建新的 WAL 文件继续写入。
func (w *WAL) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 计算需要的空间：4 bytes length + data
	need := 4 + len(data)

	// 检查是否需要滚动到新文件
	// 当剩余空间不足以容纳当前写入时，触发滚动
	if int64(need) >= walFileSizeLimit-w.fileSize {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
	}

	// 如果单次写入数据大于等于缓冲区大小，直接刷出缓冲区并写入文件
	if len(data) >= len(w.buf) {
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
		// 写入长度前缀
		var lengthBuf [4]byte
		binary.BigEndian.PutUint32(lengthBuf[:], uint32(len(data)))
		if _, err := w.file.Write(lengthBuf[:]); err != nil {
			return 0, err
		}
		w.fileSize += 4
		// 写入数据
		if _, err := w.file.Write(data); err != nil {
			return 0, err
		}
		w.fileSize += int64(len(data))
		return len(data), nil
	}

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

// rotateLocked 滚动到新的 WAL 文件。
//
// 需要持有 w.mu 锁。
func (w *WAL) rotateLocked() error {
	// 先刷出当前缓冲区
	if err := w.flushLocked(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}

	// 创建新文件
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
	w.fileSize += int64(w.pos)
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
		for {
			select {
			case <-ticker.C:
				if err := w.Sync(); err != nil {
					w.logger.Error("wal sync failed", slog.Any("error", err))
				}
			case <-done:
				ticker.Stop()
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

// Cleanup 删除旧的 WAL 文件。
//
// 调用时机：
//
//	在 MemTable 成功刷盘到 SSTable 后调用。
//	此时旧 WAL 文件中的数据已经持久化到 SSTable，不再需要。
//
// 删除策略：
//
//	删除所有序列号小于当前序列号的 WAL 文件。
//	当前 WAL 文件不会被删除（由 TruncateCurrent 处理）。
func (w *WAL) Cleanup() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	pattern := filepath.Join(w.dir, "*.wal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	for _, path := range matches {
		filename := filepath.Base(path)
		seqStr := filename[:len(filename)-4] // 去掉 ".wal" 后缀
		var seq uint64
		if _, err := fmt.Sscanf(seqStr, "%020d", &seq); err != nil {
			continue // 跳过无法解析的文件名
		}
		if seq < w.seq {
			if err := os.Remove(path); err != nil {
				w.logger.Warn("failed to remove old WAL file", "path", path, "error", err)
			}
		}
	}
	return nil
}

// TruncateCurrent 清空当前 WAL 文件。
//
// 调用时机：
//
//	在 MemTable 成功刷盘到 SSTable 后调用。
//	此时数据已在 SSTable 中，不再需要 WAL 中的数据。
//
// 实现：
//
//	先刷盘和 sync，然后截断文件到 0。
//	文件句柄保持打开状态，无需重建。
func (w *WAL) TruncateCurrent() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 先刷出缓冲区
	if err := w.flushLocked(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}

	// 截断文件到 0
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	// 重新定位到文件开头
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	w.fileSize = 0
	return nil
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
		switch v.GetValue().(type) {
		case *types.FieldValue_FloatValue, *types.FieldValue_IntValue:
			size += 8
		case *types.FieldValue_StringValue:
			size += 4 + len(v.GetValue().(*types.FieldValue_StringValue).StringValue)
		case *types.FieldValue_BoolValue:
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
		switch val := v.GetValue().(type) {
		case *types.FieldValue_FloatValue:
			buf = append(buf, 0)
			var vb [8]byte
			binary.BigEndian.PutUint64(vb[:], math.Float64bits(val.FloatValue))
			buf = append(buf, vb[:]...)
		case *types.FieldValue_IntValue:
			buf = append(buf, 1)
			var vb [8]byte
			binary.BigEndian.PutUint64(vb[:], uint64(val.IntValue))
			buf = append(buf, vb[:]...)
		case *types.FieldValue_StringValue:
			buf = append(buf, 2)
			var vl [4]byte
			binary.BigEndian.PutUint32(vl[:], uint32(len(val.StringValue)))
			buf = append(buf, vl[:]...)
			buf = append(buf, val.StringValue...)
		case *types.FieldValue_BoolValue:
			buf = append(buf, 3)
			if val.BoolValue {
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

	fields := make(map[string]*types.FieldValue, fieldCount)

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
			fields[key] = types.NewFieldValue(val)
		case 1: // int64
			if pos+8 > len(data) {
				return nil, fmt.Errorf("data too short for int64 value")
			}
			val := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			fields[key] = types.NewFieldValue(val)
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
			fields[key] = types.NewFieldValue(val)
		case 3: // bool
			if pos+1 > len(data) {
				return nil, fmt.Errorf("data too short for bool value")
			}
			val := data[pos] == 1
			pos++
			fields[key] = types.NewFieldValue(val)
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
//
// 增量 replay：
//
//	如果存在 checkpoint 文件，会从上次中断的位置继续 replay。
//	这避免了对已处理数据的重复读取。
func ReplayWAL(walDir string) ([]*types.Point, error) {
	// 加载 checkpoint
	var cp WALReplayCheckpoint
	if err := cp.Load(walDir); err != nil {
		slog.Warn("failed to load WAL checkpoint, doing full replay", "walDir", walDir, "error", err)
	}

	// 获取所有 WAL 文件
	files, err := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if err != nil {
		return nil, err
	}

	// 解析文件序列号并排序
	type walFile struct {
		seq  uint64
		path string
	}
	var walFiles []walFile
	for _, f := range files {
		seq, err := parseWALSeq(f)
		if err != nil {
			continue
		}
		walFiles = append(walFiles, walFile{seq: seq, path: f})
	}
	sort.Slice(walFiles, func(i, j int) bool {
		return walFiles[i].seq < walFiles[j].seq
	})

	var points []*types.Point
	for _, wf := range walFiles {
		// 跳过完全处理过的文件
		if wf.seq < cp.LastSeq {
			continue
		}

		// 确定起始偏移
		startPos := int64(0)
		if wf.seq == cp.LastSeq {
			startPos = cp.LastPos
		}

		// 读取并解析文件
		readPoints, readPos, err := replayWALFile(wf.path, startPos)
		if err != nil {
			slog.Warn("failed to replay WAL file, skipping", "path", wf.path, "error", err)
			continue
		}
		points = append(points, readPoints...)

		// 更新 checkpoint
		cp.LastSeq = wf.seq
		cp.LastPos = readPos
		if err := cp.Save(walDir); err != nil {
			slog.Warn("failed to save WAL checkpoint", "error", err)
		}
	}

	return points, nil
}

// parseWALSeq 从 WAL 文件路径解析序列号。
func parseWALSeq(path string) (uint64, error) {
	filename := filepath.Base(path)
	if len(filename) < 4 || filename[len(filename)-4:] != ".wal" {
		return 0, fmt.Errorf("invalid WAL filename: %s", filename)
	}
	seqStr := filename[:len(filename)-4]
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse WAL seq: %w", err)
	}
	return seq, nil
}

// replayWALFile 从指定偏移读取单个 WAL 文件。
// 使用流式读取，避免将整个文件加载到内存。
func replayWALFile(path string, startPos int64) ([]*types.Point, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}

	// Seek 到起始位置
	if _, err := file.Seek(startPos, 0); err != nil {
		_ = file.Close()
		return nil, 0, err
	}

	var points []*types.Point
	pos := startPos
	buf := make([]byte, 4096) // 4KB 读取缓冲区

	for {
		// 读取长度字段 (4 bytes)
		lengthBuf := make([]byte, 4)
		n, err := file.Read(lengthBuf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			_ = file.Close()
			return points, pos, err
		}
		if n != 4 {
			_ = file.Close()
			break
		}
		pos += 4

		size := int(binary.BigEndian.Uint32(lengthBuf))
		if size > 1024*1024*1024 { // 超过 1GB 的单条记录，可能是损坏
			slog.Warn("WAL record too large, stopping replay", "size", size, "path", path)
			_ = file.Close()
			break
		}

		if size > len(buf) {
			buf = make([]byte, size)
		}

		// 读取数据
		data := buf[:size]
		read := 0
		for read < size {
			n, err := file.Read(data[read:])
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				_ = file.Close()
				return points, pos, err
			}
			read += n
			pos += int64(n)
		}

		if read != size {
			_ = file.Close()
			break
		}

		p, err := deserializePoint(data)
		if err != nil {
			continue
		}
		points = append(points, p)
	}

	_ = file.Close()
	return points, pos, nil
}

// ReplayWALFromCheckpoint 从指定 checkpoint 开始 replay。
// 用于测试或强制全量 replay。
func ReplayWALFromCheckpoint(walDir string, checkpoint *WALReplayCheckpoint) ([]*types.Point, error) {
	files, err := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if err != nil {
		return nil, err
	}

	type walFile struct {
		seq  uint64
		path string
	}
	var walFiles []walFile
	for _, f := range files {
		seq, err := parseWALSeq(f)
		if err != nil {
			continue
		}
		walFiles = append(walFiles, walFile{seq: seq, path: f})
	}
	sort.Slice(walFiles, func(i, j int) bool {
		return walFiles[i].seq < walFiles[j].seq
	})

	var points []*types.Point
	for _, wf := range walFiles {
		if wf.seq < checkpoint.LastSeq {
			continue
		}

		startPos := int64(0)
		if wf.seq == checkpoint.LastSeq {
			startPos = checkpoint.LastPos
		}

		readPoints, _, err := replayWALFile(wf.path, startPos)
		if err != nil {
			slog.Warn("failed to replay WAL file, skipping", "path", wf.path, "error", err)
			continue
		}
		points = append(points, readPoints...)
	}

	return points, nil
}
