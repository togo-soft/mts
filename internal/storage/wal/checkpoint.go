package wal

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Checkpoint 记录 WAL 回放进度。
type Checkpoint struct {
	Generation uint64 `json:"gen"`
	Segment    uint64 `json:"seg"`
	Position   int64  `json:"pos"`
}

func checkpointPath(dir string) string {
	return filepath.Join(dir, "_replay_checkpoint.json")
}

// saveCheckpoint 原子写入 checkpoint（先写 .tmp 再 rename）。
func saveCheckpoint(dir string, cp *Checkpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}

	path := checkpointPath(dir)
	tmpPath := path + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// loadCheckpoint 加载 checkpoint，文件不存在返回零值。
func loadCheckpoint(dir string) (*Checkpoint, error) {
	path := checkpointPath(dir)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Checkpoint{}, nil
		}
		return nil, err
	}

	cp := &Checkpoint{}
	if err := json.Unmarshal(data, cp); err != nil {
		return nil, err
	}
	return cp, nil
}
