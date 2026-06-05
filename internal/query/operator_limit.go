package query

import (
	"context"

	"codeberg.org/micro-ts/mts/types"
)

// LimitOperator 跳过 Offset 行后限制输出行数。
type LimitOperator struct {
	upstream Operator
	offset   int64
	limit    int64
	skipped  int64
	consumed int64
}

// NewLimitOperator 创建截断算子。
func NewLimitOperator(upstream Operator, offset, limit int64) *LimitOperator {
	return &LimitOperator{upstream: upstream, offset: offset, limit: limit}
}

// Open 初始化上游算子。
func (l *LimitOperator) Open(ctx context.Context) error {
	return l.upstream.Open(ctx)
}

// Next 返回经过 Offset/Limit 截断后的行。
func (l *LimitOperator) Next() (*types.PointRow, error) {
	for {
		if l.limit > 0 && l.consumed >= l.limit {
			return nil, nil
		}
		row, err := l.upstream.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil
		}
		if l.skipped < l.offset {
			l.skipped++
			continue
		}
		l.consumed++
		return row, nil
	}
}

// Close 关闭上游算子。
func (l *LimitOperator) Close() error {
	return l.upstream.Close()
}
