// Package types 查询相关类型
package types

type QueryRangeRequest struct {
	Database    string
	Measurement string
	StartTime   int64
	EndTime     int64
	Fields      []string
	Tags        map[string]string
	Offset      int64
	Limit       int64
}

// QueryRangeResponse 返回查询结果
type QueryRangeResponse struct {
	Database    string
	Measurement string
	StartTime   int64
	EndTime     int64
	TotalCount  int64
	HasMore     bool
	Rows        []PointRow
}
