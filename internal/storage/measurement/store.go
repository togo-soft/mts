// internal/storage/measurement/store.go
package measurement

import (
	"context"

	"micro-ts/internal/types"
)

// MetaStore 元数据存储接口
type MetaStore interface {
	// 获取 Measurement 元信息
	GetMeta(ctx context.Context) (*types.MeasurementMeta, error)
	// 更新 Measurement 元信息
	SetMeta(ctx context.Context, meta *types.MeasurementMeta) error

	// 根据 sid 获取 tags
	GetSeries(ctx context.Context, sid uint64) ([]byte, error)
	// 添加或更新 series
	SetSeries(ctx context.Context, sid uint64, tags []byte) error
	// 获取所有 series
	GetAllSeries(ctx context.Context) (map[uint64][]byte, error)

	// 获取 tag 对应的所有 sid
	GetSidsByTag(ctx context.Context, tagKey, tagValue string) ([]uint64, error)
	// 添加 tag -> sid 映射
	AddTagIndex(ctx context.Context, tagKey, tagValue string, sid uint64) error

	// 持久化到磁盘
	Persist(ctx context.Context) error
	// 从磁盘加载
	Load(ctx context.Context) error
	// 关闭
	Close() error
}
