package shard

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"codeberg.org/micro-ts/mts/types"
)

// ReplayWAL 重放 WAL 文件，恢复数据点。
func ReplayWAL(walDir string) ([]*types.Point, error) {
	var cp WALReplayCheckpoint
	if err := cp.Load(walDir); err != nil {
		slog.Warn("failed to load WAL checkpoint, doing full replay", "walDir", walDir, "error", err)
	}

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
		if wf.seq < cp.LastSeq {
			continue
		}

		startPos := int64(0)
		if wf.seq == cp.LastSeq {
			startPos = cp.LastPos
		}

		readPoints, readPos, err := replayWALFile(wf.path, startPos)
		if err != nil {
			slog.Warn("failed to replay WAL file, skipping", "path", wf.path, "error", err)
			continue
		}
		points = append(points, readPoints...)

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
func replayWALFile(path string, startPos int64) ([]*types.Point, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}

	if _, err := file.Seek(startPos, 0); err != nil {
		_ = file.Close()
		return nil, 0, err
	}

	var points []*types.Point
	pos := startPos
	buf := make([]byte, 4096)

	for {
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
		if size > 1024*1024*1024 {
			slog.Warn("WAL record too large, stopping replay", "size", size, "path", path)
			_ = file.Close()
			break
		}

		if size > len(buf) {
			buf = make([]byte, size)
		}

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
