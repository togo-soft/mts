// internal/storage/measurement/store.go
package measurement

import (
	"context"

	"codeberg.org/micro-ts/mts/types"
)

// MetaStore 元数据存储接口
type MetaStore interface {
	// 获取 Measurement 元信息（Schema、TagKeys）。
	//
	// 返回：
	//   - *types.MeasurementMeta: Measurement Schema 和配置
	//   - error: 获取失败时返回错误
	GetMeta(ctx context.Context) (*types.MeasurementMeta, error)

	// 更新 Measurement 元信息。
	//
	// 参数：
	//   - meta: 新的 Measurement 元信息
	//
	// 返回：
	//   - error: 更新失败时返回错误
	//
	// 注意：
	//
	//	更新会标记 MetaStore 为 dirty（需要持久化）。
	SetMeta(ctx context.Context, meta *types.MeasurementMeta) error

	// GetSeries 根据 SID 获取 Series 的标签。
	//
	// 参数：
	//   - ctx: 上下文
	//   - sid: Series ID
	//
	// 返回：
	//   - []byte: 序列化后的标签数据
	//   - error:  获取失败时返回错误
	//
	// 注意：
	//
	//	返回的切片是内部数据的副本，修改不会影响存储。
	GetSeries(ctx context.Context, sid uint64) ([]byte, error)

	// SetSeries 设置或更新 Series 的标签。
	//
	// 参数：
	//   - ctx:  上下文
	//   - sid:  Series ID
	//   - tags: 序列化的标签数据
	//
	// 返回：
	//   - error: 设置失败时返回错误
	//
	// 注意：
	//
	//	会标记 MetaStore 为 dirty。
	SetSeries(ctx context.Context, sid uint64, tags []byte) error

	// GetAllSeries 获取所有 Series 的标签。
	//
	// 返回：
	//   - map[uint64][]byte: SID 到标签的映射，每个值都是独立副本
	//   - error:             获取失败时返回错误
	GetAllSeries(ctx context.Context) (map[uint64][]byte, error)

	// GetSidsByTag 查找匹配标签条件的所有 Series IDs。
	//
	// 参数：
	//   - ctx:      上下文
	//   - tagKey:   标签键
	//   - tagValue: 标签值
	//
	// 返回：
	//   - []uint64: 匹配的 Series IDs
	//   - error:    查询失败时返回错误
	//
	// 用途：
	//
	//	用于优化带标签过滤的查询，避免全表扫描。
	GetSidsByTag(ctx context.Context, tagKey, tagValue string) ([]uint64, error)

	// AddTagIndex 添加标签到 Series 的索引。
	//
	// 参数：
	//   - ctx:      上下文
	//   - tagKey:   标签键
	//   - tagValue: 标签值
	//   - sid:      Series ID
	//
	// 返回：
	//   - error: 索引失败时返回错误
	//
	// 注意：
	//
	//	会标记 MetaStore 为 dirty。
	//	相同的 (tagKey, tagValue, sid) 可能被多次添加。
	AddTagIndex(ctx context.Context, tagKey, tagValue string, sid uint64) error

	// Persist 将元数据持久化到磁盘。
	//
	// 参数：
	//   - ctx:  上下文
	//   - path: 持久化文件路径
	//
	// 返回：
	//   - error: 持久化失败时返回错误
	//
	// 格式：
	//
	//	二进制格式，包含魔数、版本、Schema、Series 和 Tag 索引。
	Persist(ctx context.Context, path string) error

	// Load 从磁盘加载元数据。
	//
	// 参数：
	//   - ctx:  上下文
	//   - path: 持久化文件路径
	//
	// 返回：
	//   - error: 加载失败时返回错误
	//
	// 注意：
	//
	//	加载会清空当前的内存数据。
	//	文件格式需匹配持久化时的版本。
	Load(ctx context.Context, path string) error

	// Close 关闭存储，释放资源。
	//
	// 返回：
	//   - error: 关闭失败时返回错误
	//
	// 说明：
	//
	//	关闭后 MetaStore 不可再使用。
	//	不会自动调用 Persist，需显式保存。
	Close() error
}
