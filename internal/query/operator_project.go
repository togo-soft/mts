package query

import (
	"context"

	"codeberg.org/micro-ts/mts/types"
)

// ProjectOperator 选择输出指定字段。
type ProjectOperator struct {
	upstream   Operator
	fields     []string
	currentRow *types.PointRow
}

// NewProjectOperator 创建投影算子。
func NewProjectOperator(upstream Operator, fields []string) *ProjectOperator {
	return &ProjectOperator{upstream: upstream, fields: fields}
}

// Open 初始化上游算子。
func (p *ProjectOperator) Open(ctx context.Context) error {
	return p.upstream.Open(ctx)
}

// Next 返回只包含指定字段的行。
func (p *ProjectOperator) Next() (*types.PointRow, error) {
	row, err := p.upstream.Next()
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	if len(p.fields) == 0 || len(row.Fields) == len(p.fields) {
		return row, nil
	}

	fieldSet := make(map[string]bool, len(p.fields))
	for _, name := range p.fields {
		fieldSet[name] = true
	}
	filtered := make([]*types.FieldEntry, 0, len(p.fields))
	for _, f := range row.Fields {
		if fieldSet[f.Key] {
			filtered = append(filtered, f)
		}
	}
	p.currentRow = &types.PointRow{
		Sid:       row.Sid,
		Timestamp: row.Timestamp,
		Tags:      row.Tags,
		Fields:    filtered,
	}
	return p.currentRow, nil
}

// Close 关闭上游算子。
func (p *ProjectOperator) Close() error {
	return p.upstream.Close()
}
