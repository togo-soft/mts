// Package unordered 管理未排序 SSTable 文件（immutable memtable 集合）。
// 目录结构: {dataDir}/unordered/{db}/{meas}/sst_{seq}.bin
package unordered

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/types"
)

const dirName = "unordered"

var globalSeq atomic.Uint64

// Dir 返回 unordered 目录路径。
func Dir(dataDir string) string {
	return filepath.Join(dataDir, dirName)
}

// EnsureDir 确保 unordered 目录存在（权限 0700）。
func EnsureDir(dataDir string) error {
	return os.MkdirAll(Dir(dataDir), 0700)
}

// NextSeq 获取全局自增序列号。
func NextSeq() uint64 {
	return globalSeq.Add(1)
}

// SetSeq 从已有文件中恢复最大序列号（启动时调用）。
func SetSeq(maxSeq uint64) {
	for {
		current := globalSeq.Load()
		if maxSeq <= current || globalSeq.CompareAndSwap(current, maxSeq) {
			break
		}
	}
}

// FilePath 返回指定 db/meas/seq 的文件路径。
func FilePath(dataDir, db, meas string, seq uint64) string {
	return filepath.Join(Dir(dataDir), db, meas, fmt.Sprintf("sst_%d.bin", seq))
}

// WriteAndPath 将 MemPoint 切片按 (db, measurement) 分组写入对应的
// {dataDir}/unordered/{db}/{meas}/sst_{seq}.bin 文件。
// 返回每个已写入文件的路径列表。
func Write(dataDir string, points []types.MemPoint, compressionAlgo types.CompressionAlgorithm) ([]string, error) {
	if len(points) == 0 {
		return nil, fmt.Errorf("unordered write: empty points")
	}

	// 按 (db, meas) 分组
	type group struct {
		db     string
		meas   string
		points []types.MemPoint
	}
	groupMap := make(map[string]*group)
	var groupOrder []string

	for _, mp := range points {
		key := mp.Database + "\x00" + mp.Measurement
		g, ok := groupMap[key]
		if !ok {
			g = &group{db: mp.Database, meas: mp.Measurement}
			groupMap[key] = g
			groupOrder = append(groupOrder, key)
		}
		g.points = append(g.points, mp)
	}

	var paths []string
	for _, key := range groupOrder {
		g := groupMap[key]
		seq := NextSeq()
		targetPath := FilePath(dataDir, g.db, g.meas, seq)

		// 确保 {dataDir}/unordered/{db}/{meas}/ 目录存在
		parentDir := filepath.Dir(targetPath)
		if err := os.MkdirAll(parentDir, 0700); err != nil {
			// 清理已写入的文件
			for _, p := range paths {
				_ = os.Remove(p)
			}
			return nil, fmt.Errorf("ordered mkdir: %w", err)
		}

		// 使用 ShardDir 为 {dataDir}/unordered/{db}/{meas}，这样 sstable.Writer
		// 会在 {dataDir}/unordered/{db}/{meas}/data/sst_{seq}.bin 创建临时文件
		w, err := sstable.NewWriter(parentDir, seq, sstable.BlockSize, compressionAlgo, sstable.FlagUnordered)
		if err != nil {
			for _, p := range paths {
				_ = os.Remove(p)
			}
			return nil, fmt.Errorf("unordered write: %w", err)
		}
		if err := w.WriteMemPoints(g.points); err != nil {
			_ = w.Close()
			_ = os.Remove(targetPath)
			for _, p := range paths {
				_ = os.Remove(p)
			}
			return nil, fmt.Errorf("unordered write points: %w", err)
		}
		if err := w.Close(); err != nil {
			_ = os.Remove(targetPath)
			for _, p := range paths {
				_ = os.Remove(p)
			}
			return nil, fmt.Errorf("unordered close: %w", err)
		}

		// Writer 将文件创建在 parentDir/data/sst_{seq}.bin，移动至目标路径
		writerPath := filepath.Join(parentDir, "data", fmt.Sprintf("sst_%d.bin", seq))
		if err := os.Rename(writerPath, targetPath); err != nil {
			_ = os.Remove(targetPath)
			for _, p := range paths {
				_ = os.Remove(p)
			}
			return nil, fmt.Errorf("unordered rename: %w", err)
		}
		// 清理空的 data 目录
		_ = os.Remove(filepath.Join(parentDir, "data"))

		// FieldData 由 PointToMemPoint 从 fieldSerialPool 分配，写入完成后归还 pool
		for _, mp := range g.points {
			types.ReleaseFieldData(mp.FieldData)
		}

		paths = append(paths, targetPath)
	}

	return paths, nil
}

// Remove 删除指定的 unordered 文件。
func Remove(path string) error {
	return os.Remove(path)
}

// ListFiles 递归列出 unordered 目录下所有 sst_*.bin 文件，按 seq 排序。
func ListFiles(dataDir string) ([]string, error) {
	dir := Dir(dataDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() {
			// 兼容旧版：{dataDir}/unordered/sst_{seq}.bin
			if strings.HasPrefix(e.Name(), "sst_") && strings.HasSuffix(e.Name(), ".bin") {
				files = append(files, filepath.Join(dir, e.Name()))
			}
			continue
		}
		// 递归扫描 {db}/{meas}/sst_{seq}.bin
		dbDir := filepath.Join(dir, e.Name())
		measEntries, err := os.ReadDir(dbDir)
		if err != nil {
			continue
		}
		for _, me := range measEntries {
			if !me.IsDir() {
				if strings.HasPrefix(me.Name(), "sst_") && strings.HasSuffix(me.Name(), ".bin") {
					files = append(files, filepath.Join(dbDir, me.Name()))
				}
				continue
			}
			measDir := filepath.Join(dbDir, me.Name())
			sstEntries, err := os.ReadDir(measDir)
			if err != nil {
				continue
			}
			for _, se := range sstEntries {
				if se.IsDir() || !strings.HasPrefix(se.Name(), "sst_") || !strings.HasSuffix(se.Name(), ".bin") {
					continue
				}
				files = append(files, filepath.Join(measDir, se.Name()))
			}
		}
	}

	sort.Slice(files, func(i, j int) bool {
		si, _ := parseSeq(files[i])
		sj, _ := parseSeq(files[j])
		return si < sj
	})
	return files, nil
}

// ParseFilePath 从 unordered 文件路径提取 db、meas 和 seq。
// 格式: {dataDir}/unordered/{db}/{meas}/sst_{seq}.bin
func ParseFilePath(dataDir, path string) (db, meas string, seq uint64, ok bool) {
	rel, err := filepath.Rel(Dir(dataDir), path)
	if err != nil {
		return "", "", 0, false
	}
	parts := strings.SplitN(rel, string(filepath.Separator), 3)
	if len(parts) != 3 {
		return "", "", 0, false
	}
	db = parts[0]
	meas = parts[1]
	seq, err = parseSeq(parts[2])
	if err != nil {
		return "", "", 0, false
	}
	return db, meas, seq, true
}

// RecoverSeq 从 unordered 目录恢复最大 seq（启动时调用）。
func RecoverSeq(dataDir string) error {
	files, err := ListFiles(dataDir)
	if err != nil {
		return err
	}
	var maxSeq uint64
	for _, f := range files {
		seq, err := parseSeq(f)
		if err != nil {
			continue
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	SetSeq(maxSeq)
	return nil
}

func parseSeq(path string) (uint64, error) {
	base := filepath.Base(path)
	numStr := strings.TrimPrefix(base, "sst_")
	numStr = strings.TrimSuffix(numStr, ".bin")
	n, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse seq from %q: %w", base, err)
	}
	return n, nil
}
