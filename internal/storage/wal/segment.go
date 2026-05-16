package wal

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"codeberg.org/micro-ts/mts/internal/storage"
)

// segment 表示一个 WAL 文件。
type segment struct {
	file          *os.File
	gen           uint64 // 世代号
	num           uint64 // segment 序号
	size          int64  // 当前文件大小
	headerWritten bool
	compressed    bool // 是否使用压缩
}

// openSegment 打开或创建指定世代和序号的 WAL segment。
func openSegment(dir string, gen uint64, num uint64, compressed bool) (*segment, error) {
	filename := segmentPath(dir, gen, num)
	slog.Debug("openSegment: opening file", "filename", filename)
	f, err := storage.SafeOpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	seg := &segment{
		file:          f,
		gen:           gen,
		num:           num,
		size:          info.Size(),
		headerWritten: info.Size() >= segmentHeaderSize,
		compressed:    compressed,
	}

	if !seg.headerWritten {
		if err := seg.writeHeader(); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	return seg, nil
}

// segmentPath 生成 segment 文件路径。
func segmentPath(dir string, gen uint64, num uint64) string {
	return filepath.Join(dir, segmentName(gen, num))
}

// segmentName 生成文件名（不含路径）。
func segmentName(gen uint64, num uint64) string {
	return formatHex16(gen) + "_" + formatHex8(num) + ".wal"
}

func formatHex16(n uint64) string {
	return fmt.Sprintf("%016x", n)
}

func formatHex8(n uint64) string {
	return fmt.Sprintf("%08x", n)
}

// writeHeader 写入文件头。
func (s *segment) writeHeader() error {
	var buf [segmentHeaderSize]byte
	flags := uint16(0)
	if s.compressed {
		flags = FlagCompressed
	}
	encodeSegmentHeader(buf[:], uint32(s.num), flags)
	n, err := s.file.Write(buf[:])
	if err != nil {
		return err
	}
	if n != segmentHeaderSize {
		return ErrShortWrite
	}
	s.size = segmentHeaderSize
	s.headerWritten = true
	return nil
}

// Write 追加数据到 segment 文件。
func (s *segment) Write(data []byte) (int, error) {
	n, err := s.file.Write(data)
	if err != nil {
		return 0, err
	}
	s.size += int64(n)
	return n, nil
}

// Sync 刷盘。
func (s *segment) Sync() error {
	return s.file.Sync()
}

// Truncate 截断文件到 0，重新写 header。
func (s *segment) Truncate() error {
	if err := s.file.Truncate(0); err != nil {
		return err
	}
	if _, err := s.file.Seek(0, 0); err != nil {
		return err
	}
	s.size = 0
	s.headerWritten = false
	return s.writeHeader()
}

// Close 关闭 segment 文件。
func (s *segment) Close() error {
	return s.file.Close()
}

// parseSegmentName 从文件名解析 (generation, segment)。
func parseSegmentName(filename string) (gen uint64, num uint64, err error) {
	base := filepath.Base(filename)
	if !strings.HasSuffix(base, ".wal") {
		return 0, 0, &FormatError{Reason: "not a .wal file: " + base}
	}
	core := base[:len(base)-4]
	parts := strings.Split(core, "_")
	if len(parts) != 2 || len(parts[0]) != 16 || len(parts[1]) != 8 {
		return 0, 0, &FormatError{Reason: "invalid segment name: " + base}
	}
	gen, err = strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return 0, 0, &FormatError{Reason: "invalid generation: " + parts[0]}
	}
	num, err = strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return 0, 0, &FormatError{Reason: "invalid segment number: " + parts[1]}
	}
	return gen, num, nil
}

// segmentEntry 表示一个已发现的 segment。
type segmentEntry struct {
	Gen  uint64
	Num  uint64
	Path string
}

// listSegments 列出目录中所有 WAL segment，按 (gen, num) 排序。
func listSegments(dir string) ([]segmentEntry, error) {
	pattern := filepath.Join(dir, "*.wal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	type seg struct {
		gen  uint64
		num  uint64
		path string
	}
	var segs []seg
	for _, m := range matches {
		g, n, e := parseSegmentName(m)
		if e != nil {
			continue
		}
		segs = append(segs, seg{gen: g, num: n, path: m})
	}
	for i := 0; i < len(segs)-1; i++ {
		for j := i + 1; j < len(segs); j++ {
			if segs[i].gen > segs[j].gen ||
				(segs[i].gen == segs[j].gen && segs[i].num > segs[j].num) {
				segs[i], segs[j] = segs[j], segs[i]
			}
		}
	}

	entries := make([]segmentEntry, len(segs))
	for i, s := range segs {
		entries[i] = segmentEntry{Gen: s.gen, Num: s.num, Path: s.path}
	}
	return entries, nil
}
