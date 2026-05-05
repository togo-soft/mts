// Package types 公共类型定义
package types

// Point 是写入的基本单位
type Point struct {
	Database    string
	Measurement string
	Tags        map[string]string
	Timestamp   int64 // 纳秒
	Fields      map[string]any
}

// PointRow 是查询结果的一行
type PointRow struct {
	SID       uint64 // Series ID
	Timestamp int64
	Tags      map[string]string
	Fields    map[string]any
}
