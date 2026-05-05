// Package types 定义了微时序数据库的公共数据类型。
//
// 这些类型是数据库 API 的核心，被用于写入、查询和数据交换。
// 类型设计遵循时序数据库的惯例，兼容 InfluxDB 等主流时序数据库的概念。
//
// 基本概念：
//
//   - Point:  时序数据库的基本写入单元，包含时间戳、标签和字段
//   - Series: 唯一标签组合定义的时间序列
//   - Measurement: 类似关系数据库中的表，一组相关 Series 的集合
//
// 数据类型定义原则：
//
//   - 时间戳使用纳秒级 Unix 时间戳（int64）
//   - 标签使用字符串键值对，用于 Series 唯一标识
//   - 字段支持多种数据类型：float64, int64, string, bool
package types

// Point 是写入时序数据库的基本数据单元。
//
// 每个 Point 代表时间序列中的一个数据点，必须包含：
//   - 时间戳：纳秒级 Unix 时间戳，标识数据点的时间
//   - Database 和 Measurement：标识数据归属
//   - Tags：标签键值对，用于标识时间序列（建议使用 host, region 等维度）
//   - Fields：字段值，存储实际的数据
//
// 标签与字段的区别：
//
//	标签用于索引和分组，会被存储在 Series Key 中，查询效率高但数量有限。
//	字段存储实际数据值，不会被索引，但可以存储大量不同的值。
//
// 时间戳使用建议：
//
//	推荐使用时间.Now().UnixNano() 获取当前时间的纳秒戳。
//	数据库支持乱序写入，但顺序写入性能更好。
//
// 使用示例：
//
//	point := &types.Point{
//	    Database:    "metrics",
//	    Measurement: "cpu_usage",
//	    Tags: map[string]string{
//	        "host":   "server1",
//	        "region": "us-east-1",
//	    },
//	    Timestamp: time.Now().UnixNano(),
//	    Fields: map[string]any{
//	        "value":       45.2,
//	        "temperature": 65.0,
//	    },
//	}
type Point struct {
	Database    string
	Measurement string
	Tags        map[string]string
	Timestamp   int64 // 纳秒
	Fields      map[string]any
}

// PointRow 是查询结果的单行数据。
//
// 包含完整的 Time-Series-Data 信息：时间戳、标签和字段值。
// SID（Series ID）是内部使用的序列标识符，用户通常不需要直接使用。
//
// PointRow 结构用于查询返回，相比写入时的 Point 结构更扁平化：
//
//   - SID:       Series ID，用于内部标识唯一的标签组合
//   - Timestamp: 数据点的时间戳
//   - Tags:      标签键值对
//   - Fields:    字段值
//
// 使用场景：
//
//	查询结果使用 PointRow 而非 Point，原因是：
//	1. 返回的是 Series 的一部分信息，完整的 Point 可能需要更多上下文
//	2. 更扁平的结构便于遍历和处理
//
// 使用示例：
//
//	for _, row := range resp.Rows {
//	    fmt.Printf("时间: %d, 标签: %v, 值: %v\n",
//	        time.Unix(0, row.Timestamp), row.Tags, row.Fields)
//	}
type PointRow struct {
	SID       uint64 // Series ID
	Timestamp int64
	Tags      map[string]string
	Fields    map[string]any
}
