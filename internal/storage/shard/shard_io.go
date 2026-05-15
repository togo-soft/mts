package shard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// listSSTableFiles 列出 Shard 中所有可读的 SSTable 文件路径。
// SSTable 文件始终存放在 data/ 目录下（data/sst_*.bin），
// 同时也兼容 leveled 目录结构（data/L0/sst_*.bin, ...）。
func (s *Shard) listSSTableFiles() []string {
	dataDir := filepath.Join(s.dir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return nil
	}

	var files []string

	// 先扫描 data/ 下的 SSTable 文件（初始 L0 文件和 compaction 输出文件）
	entries, err := os.ReadDir(dataDir)
	if err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if !strings.HasPrefix(entry.Name(), "sst_") || !strings.HasSuffix(entry.Name(), ".bin") {
				continue
			}
			files = append(files, filepath.Join(dataDir, entry.Name()))
		}
	}

	// 当 levelCompaction 启用时，额外扫描 data/L{level}/ 目录下的文件
	if s.levelCompaction != nil {
		for level := 0; ; level++ {
			levelDir := filepath.Join(dataDir, fmt.Sprintf("L%d", level))
			entries, err := os.ReadDir(levelDir)
			if err != nil {
				break
			}
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				if !strings.HasPrefix(entry.Name(), "sst_") || !strings.HasSuffix(entry.Name(), ".bin") {
					continue
				}
				files = append(files, filepath.Join(levelDir, entry.Name()))
			}
		}
	}

	return files
}
