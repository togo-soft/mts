// microts.go
package microts

import (
	"context"

	"micro-ts/internal/types"
)

// Config 配置
type Config struct {
	DataDir string
}

// DB 数据库
type DB struct {
	cfg *Config
}

// Open 打开数据库
func Open(cfg Config) (*DB, error) {
	return &DB{cfg: &cfg}, nil
}

// Write 写入单个点
func (db *DB) Write(ctx context.Context, point *types.Point) error {
	return nil
}

// WriteBatch 批量写入
func (db *DB) WriteBatch(ctx context.Context, points []*types.Point) error {
	return nil
}

// QueryRange 范围查询
func (db *DB) QueryRange(ctx context.Context, req *types.QueryRangeRequest) (*types.QueryRangeResponse, error) {
	return &types.QueryRangeResponse{}, nil
}

// ListMeasurements 列出 Measurement
func (db *DB) ListMeasurements(ctx context.Context, database string) ([]string, error) {
	return []string{}, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	return nil
}
