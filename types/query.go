// Package types 提供查询相关的类型定义。
//
// 查询类型支持灵活的范围查询、字段投影、标签过滤和分页。
// 所有时间戳都使用纳秒级 Unix 时间戳。
package types

// QueryRangeRequest 定义范围查询的请求参数。
//
// 所有字段都是可选的（都有合理的默认值），但为了获得有意义的查询结果，
// 通常需要指定 Database、Measurement 和至少一个时间边界。
//
// 字段说明：
//
//   - Database:    数据库名称，用于隔离不同业务的数据
//   - Measurement: 测量名称，类似关系数据库的表名
//   - StartTime:   查询起始时间（包含），纳秒 Unix 时间戳，0 表示不限制
//   - EndTime:     查询结束时间（不包含），纳秒 Unix 时间戳，0 表示不限制
//   - Fields:      字段列表，为空表示返回所有字段
//   - Tags:        标签过滤器，所有指定的标签必须匹配
//   - Offset:      分页偏移量，跳过前 N 行
//   - Limit:       最大返回行数，0 表示不限制
//
// 使用示例：
//
//	req := &types.QueryRangeRequest{
//	    Database:    "metrics",
//	    Measurement: "cpu_usage",
//	    StartTime:   time.Now().Add(-1*time.Hour).UnixNano(),
//	    EndTime:     time.Now().UnixNano(),
//	    Fields:      []string{"usage", "temperature"},
//	    Tags:        map[string]string{"host": "server1"},
//	    Limit:       1000,
//	}
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

// QueryRangeResponse 返回范围查询的结果。
//
// 响应包含查询元数据和实际的行数据，支持结果集分页。
//
// 字段说明：
//
//   - Database:    查询的数据库名称
//   - Measurement: 查询的测量名称
//   - StartTime:   实际的查询起始时间
//   - EndTime:     实际的查询结束时间
//   - TotalCount:  返回的总行数（当前页）
//   - HasMore:     是否还有更多数据可用
//   - Rows:        查询结果行
//
// 分页处理：
//
//	当 HasMore 为 true 时，可以通过增加 Offset 来获取下一页。
//	例如：Offset = 当前已获取的行数，Limit 保持一致。
//
// 使用示例：
//
//	if resp.HasMore {
//	    req.Offset = int64(len(resp.Rows)) // 跳过已获取的行
//	    nextResp, err := db.QueryRange(ctx, req)
//	    // 处理 nextResp...
//	}
type QueryRangeResponse struct {
	Database    string
	Measurement string
	StartTime   int64
	EndTime     int64
	TotalCount  int64
	HasMore     bool
	Rows        []PointRow
}
