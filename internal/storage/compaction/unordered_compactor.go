// Package compaction 实现 Level Compaction 策略。
//
// UnorderedCompactor 将 unordered 目录下的未排序 SSTable 分拣排序后
// 写入 stable/{db}/{meas}/{shard}/L0/ 目录。
package compaction

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"codeberg.org/micro-ts/mts/internal/storage/shard/sstable"
	"codeberg.org/micro-ts/mts/internal/storage/unordered"
	"codeberg.org/micro-ts/mts/types"
)

// UnorderedShardManager shard 操作接口（compaction 需要的最小接口）。
type UnorderedShardManager interface {
	L0Dir(db, measurement string, shardStart int64) (string, error)
	ShardDurationNanos() int64
}

// UnorderedCompactor 将 unordered 数据分拣排序写入 stable L0 目录。
type UnorderedCompactor struct {
	dataDir     string
	shardMgr    UnorderedShardManager
	compression types.CompressionAlgorithm
	mu          sync.Mutex
}

// NewUnorderedCompactor 创建新的 UnorderedCompactor。
func NewUnorderedCompactor(dataDir string, shardMgr UnorderedShardManager, compression types.CompressionAlgorithm) *UnorderedCompactor {
	return &UnorderedCompactor{
		dataDir:     dataDir,
		shardMgr:    shardMgr,
		compression: compression,
	}
}

// pointGroup 按 (db, meas, shard) 分组的 key 和 points。
type pointGroup struct {
	db         string
	meas       string
	shardStart int64
	points     []types.MemPoint
}

// Compact 扫描 unordered 下所有文件，逐文件处理：
// 读取 → 按 (db, measurement, shard) 分拣排序 → 写入 L0 → 删除源文件。
// 逐文件处理可避免同时持有所有 unordered 数据，控制峰值内存。
func (uc *UnorderedCompactor) Compact() error {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	files, err := unordered.ListFiles(uc.dataDir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}

	for _, file := range files {
		if err := uc.compactFile(file); err != nil {
			return err
		}
	}

	return nil
}

// compactFile 处理单个 unordered 文件：逐行迭代读取、分组、排序、写入 L0、删除。
func (uc *UnorderedCompactor) compactFile(file string) error {
	db, meas, _, ok := unordered.ParseFilePath(uc.dataDir, file)
	if !ok {
		return nil
	}

	reader, err := sstable.NewReader(file, sstable.Schema{})
	if err != nil {
		slog.Warn("skipping corrupt unordered file", "path", file, "error", err)
		return nil
	}
	defer func() { _ = reader.Close() }()

	it, err := reader.NewIterator(nil, nil)
	if err != nil {
		slog.Warn("failed to create iterator for unordered file", "path", file, "error", err)
		return nil
	}

	groupMap := make(map[string]*pointGroup)
	var groupOrder []string

	for it.Next() {
		row := it.Point()
		shardStart := (row.Timestamp / uc.shardMgr.ShardDurationNanos()) * uc.shardMgr.ShardDurationNanos()
		key := pointGroupKey(db, meas, shardStart)

		g, ok := groupMap[key]
		if !ok {
			g = &pointGroup{
				db:         db,
				meas:       meas,
				shardStart: shardStart,
			}
			groupMap[key] = g
			groupOrder = append(groupOrder, key)
		}

		mp := types.MemPoint{
			Database:    db,
			Measurement: meas,
			Timestamp:   row.Timestamp,
			Sid:         row.Sid,
			FieldData:   rowToFieldData(row.Fields),
		}
		g.points = append(g.points, mp)
	}

	// 对每组排序并写入 L0
	for _, key := range groupOrder {
		g := groupMap[key]

		sort.Slice(g.points, func(i, j int) bool {
			if g.points[i].Timestamp != g.points[j].Timestamp {
				return g.points[i].Timestamp < g.points[j].Timestamp
			}
			return g.points[i].Sid < g.points[j].Sid
		})

		l0Dir, err := uc.shardMgr.L0Dir(g.db, g.meas, g.shardStart)
		if err != nil {
			return fmt.Errorf("L0 dir for %s/%s/%d: %w", g.db, g.meas, g.shardStart, err)
		}

		seq := unordered.NextSeq()
		w, err := sstable.NewWriter(l0Dir, seq, sstable.BlockSize, uc.compression, sstable.FlagSorted)
		if err != nil {
			return fmt.Errorf("create L0 writer: %w", err)
		}
		if err := w.WriteMemPoints(g.points); err != nil {
			_ = w.Close()
			return fmt.Errorf("write L0 points: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("close L0 writer: %w", err)
		}
	}

	// 删除已处理的源文件
	if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove source file %s: %w", file, err)
	}

	// 清理空目录（递归向上清理）
	dir := filepath.Dir(file)
	for strings.Count(dir, string(filepath.Separator)) >= strings.Count(uc.dataDir, string(filepath.Separator)) {
		entries, readErr := os.ReadDir(dir)
		if readErr != nil || len(entries) > 0 {
			break
		}
		if err := os.Remove(dir); err != nil {
			slog.Warn("failed to remove empty dir during unordered cleanup", "dir", dir, "error", err)
			break
		}
		dir = filepath.Dir(dir)
	}

	return nil
}

// pointGroupKey 生成分组 key。
func pointGroupKey(db, meas string, shardStart int64) string {
	return db + "\000" + meas + "\000" + fmt.Sprintf("%d", shardStart)
}

// rowToFieldData 将 []*FieldEntry 序列化为 FieldData 字节。
func rowToFieldData(fields []*types.FieldEntry) []byte {
	if len(fields) == 0 {
		return nil
	}
	m := make(map[string]*types.FieldValue, len(fields))
	for _, f := range fields {
		m[f.Key] = f.Value
	}
	return types.AppendFieldData(nil, m)
}
