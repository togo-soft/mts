package wal

import (
	"log/slog"
	"os"
	"path/filepath"
)

// Cleanup 删除所有世代号小于 beforeGen 的 segment 文件。
func Cleanup(dir string, beforeGen uint64) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".wal" {
			continue
		}
		gen, _, err := parseSegmentName(e.Name())
		if err != nil {
			slog.Debug("WAL cleanup: skipping file with invalid segment name", "file", e.Name(), "error", err)
			continue
		}
		if gen < beforeGen {
			path := filepath.Join(dir, e.Name())
			if err := os.Remove(path); err != nil {
				return err
			}
		}
	}
	return nil
}
