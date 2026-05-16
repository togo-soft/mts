// Package compaction 实现 Level Compaction 策略。
//
// UnorderedCompactor 将 unordered 目录下的未排序 SSTable 分拣排序后
// 写入 stable/{db}/{meas}/{shard}/L0/ 目录。
package compaction

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

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
	compression sstable.CompressionAlgorithm
}

// NewUnorderedCompactor 创建新的 UnorderedCompactor。
func NewUnorderedCompactor(dataDir string, shardMgr UnorderedShardManager, compression sstable.CompressionAlgorithm) *UnorderedCompactor {
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

// Compact 扫描 unordered 下所有文件，按 (db, measurement, shard) 分拣排序，
// 写入对应 stable/{db}/{meas}/{shardStart}_{endTime}/L0/ 目录。
func (uc *UnorderedCompactor) Compact() error {
	files, err := unordered.ListFiles(uc.dataDir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}

	// 分组: key = "db\000meas\000shardStart"
	groupMap := make(map[string]*pointGroup)
	var groupOrder []string

	for _, file := range files {
		db, meas, _, ok := unordered.ParseFilePath(uc.dataDir, file)
		if !ok {
			// 非法路径格式，可能是旧版平铺文件，跳过
			continue
		}

		reader, err := sstable.NewReader(file, sstable.Schema{})
		if err != nil {
			continue // 跳过损坏文件
		}
		rows, err := reader.ReadAll(nil)
		_ = reader.Close()
		if err != nil {
			continue
		}

		for _, row := range rows {
			shardStart := (row.Timestamp / uc.shardMgr.ShardDurationNanos()) * uc.shardMgr.ShardDurationNanos()
			key := db + "\000" + meas + "\000" + fmt.Sprintf("%d", shardStart)

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

			// 将 PointRow 转回 MemPoint
			mp := types.MemPoint{
				Database:    db,
				Measurement: meas,
				Timestamp:   row.Timestamp,
				Sid:         row.Sid,
				FieldData:   rowToFieldData(row.Fields),
			}
			g.points = append(g.points, mp)
		}
	}

	// 对每组排序并写入 L0
	for _, key := range groupOrder {
		g := groupMap[key]

		// 排序: (timestamp, sid)
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

	// 删除已处理的 unordered 文件
	for _, file := range files {
		_ = os.Remove(file)

		// 清理空目录（递归向上清理）
		dir := filepath.Dir(file)
		for strings.Count(dir, string(filepath.Separator)) >= strings.Count(uc.dataDir, string(filepath.Separator)) {
			entries, _ := os.ReadDir(dir)
			if len(entries) > 0 {
				break
			}
			_ = os.Remove(dir)
			dir = filepath.Dir(dir)
		}
	}

	return nil
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
