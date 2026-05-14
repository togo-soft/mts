package wal

import (
	"encoding/json"
	"os"
	"path/filepath"
)

const checkpointFileName = "wal_checkpoint"

// Checkpoint 记录已持久化到 SSTable 的 WAL 位置。
type Checkpoint struct {
	Generation uint64 `json:"generation"`
	Segment    uint64 `json:"segment"`
}

// CheckpointPath 返回 checkpoint 文件路径。
func CheckpointPath(walDir string) string {
	return filepath.Join(walDir, checkpointFileName)
}

// Save 写入 checkpoint 到 WAL 目录（原子写入：先写 tmp 再 rename）。
func (cp *Checkpoint) Save(walDir string) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	path := CheckpointPath(walDir)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// LoadCheckpoint 从 WAL 目录加载 checkpoint。
// 文件不存在时返回 nil, nil。
func LoadCheckpoint(walDir string) (*Checkpoint, error) {
	path := CheckpointPath(walDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

// ClearCheckpoint 删除 checkpoint 文件。
func ClearCheckpoint(walDir string) error {
	path := CheckpointPath(walDir)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
