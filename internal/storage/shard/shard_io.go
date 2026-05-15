package shard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// listSSTableFiles 列出 Shard 中所有可读的 SSTable 文件路径。
// 自动处理 flat（data/sst_*.bin）和 leveled（data/L0/sst_*.bin, ...）两种目录结构。
func (s *Shard) listSSTableFiles() []string {
	dataDir := filepath.Join(s.dir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return nil
	}

	var files []string

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
		return files
	}

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), "sst_") || !strings.HasSuffix(entry.Name(), ".bin") {
			continue
		}
		files = append(files, filepath.Join(dataDir, entry.Name()))
	}
	return files
}
