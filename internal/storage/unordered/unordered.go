// Package unordered 管理未排序 SSTable 文件（immutable memtable 集合）。
// 目录结构: {dataDir}/unordered/sst_{seq}.bin
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

const (
	dirName  = "unordered"
	filePerm = 0600
)

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

// FilePath 返回指定 seq 的文件路径。
func FilePath(dataDir string, seq uint64) string {
	return filepath.Join(Dir(dataDir), fmt.Sprintf("sst_%d.bin", seq))
}

// ListFiles 列出 unordered 目录下所有 sst_*.bin 文件，按 seq 排序。
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
		if e.IsDir() || !strings.HasPrefix(e.Name(), "sst_") || !strings.HasSuffix(e.Name(), ".bin") {
			continue
		}
		files = append(files, filepath.Join(dir, e.Name()))
	}
	sort.Slice(files, func(i, j int) bool {
		return parseSeq(files[i]) < parseSeq(files[j])
	})
	return files, nil
}

// Write 将 MemPoint 切片写入 unordered SSTable 文件，返回文件路径。
func Write(dataDir string, points []types.MemPoint, compressionAlgo sstable.CompressionAlgorithm) (string, error) {
	if len(points) == 0 {
		return "", fmt.Errorf("unordered write: empty points")
	}
	seq := NextSeq()
	targetPath := FilePath(dataDir, seq)

	// sstable.NewWriter 在 shardDir/data/ 下创建最终文件，因此将 unordered 目录作为 shardDir 传入，
	// 在 Close 后将文件从 data/sst_{seq}.bin 移动到目标路径。
	w, err := sstable.NewWriter(Dir(dataDir), seq, sstable.BlockSize, compressionAlgo, sstable.FlagUnordered)
	if err != nil {
		return "", fmt.Errorf("unordered write: %w", err)
	}
	if err := w.WriteMemPoints(points); err != nil {
		_ = w.Close()
		_ = os.Remove(targetPath)
		return "", fmt.Errorf("unordered write points: %w", err)
	}
	if err := w.Close(); err != nil {
		_ = os.Remove(targetPath)
		return "", fmt.Errorf("unordered close: %w", err)
	}

	// Writer 将文件创建在 Dir(dataDir)/data/sst_{seq}.bin，移动至目标路径
	writerPath := filepath.Join(Dir(dataDir), "data", fmt.Sprintf("sst_%d.bin", seq))
	if err := os.Rename(writerPath, targetPath); err != nil {
		_ = os.Remove(targetPath)
		return "", fmt.Errorf("unordered rename: %w", err)
	}
	// 清理空的 data 目录
	_ = os.Remove(filepath.Join(Dir(dataDir), "data"))

	return targetPath, nil
}

// Remove 删除指定的 unordered 文件。
func Remove(path string) error {
	return os.Remove(path)
}

func parseSeq(path string) uint64 {
	base := filepath.Base(path)
	numStr := strings.TrimPrefix(base, "sst_")
	numStr = strings.TrimSuffix(numStr, ".bin")
	n, _ := strconv.ParseUint(numStr, 10, 64)
	return n
}

// RecoverSeq 从 unordered 目录恢复最大 seq（启动时调用）。
func RecoverSeq(dataDir string) error {
	files, err := ListFiles(dataDir)
	if err != nil {
		return err
	}
	var maxSeq uint64
	for _, f := range files {
		seq := parseSeq(f)
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	SetSeq(maxSeq)
	return nil
}
