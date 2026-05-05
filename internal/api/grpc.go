// Package api 实现 gRPC API 服务。
//
// 提供完整的时序数据库远程访问接口，兼容常见的时序数据库操作语义。
//
// 服务列表：
//
//	Write:             单点写入
//	WriteBatch:        批量写入
//	QueryRange:        范围查询
//	ListMeasurements:  枚举 Measurement
//	CreateMeasurement: 创建 Measurement
//	DropMeasurement:   删除 Measurement
//	ListDatabases:     枚举数据库
//	CreateDatabase:    创建数据库
//	DropDatabase:      删除数据库
//	Health:            健康检查
//
// 状态码：
//
//	成功返回时 Success=true/Healthy=true
//	失败返回包含具体错误信息
//
// 使用方法：
//
//	// 服务端
//	service := api.New(engine)
//	grpcServer := grpc.NewServer()
//	pb.RegisterMicroTSServer(grpcServer, service)
//
//	// 客户端 (使用生成的 pb 包)
//	client := pb.NewMicroTSClient(conn)
//	resp, err := client.Write(ctx, &pb.WriteRequest{...})
package api

import (
	"context"

	"codeberg.org/micro-ts/mts/internal/api/pb"
	"codeberg.org/micro-ts/mts/types"
)

// MicroTSService 实现 gRPC MicroTS 服务。
//
// 提供时序数据库的核心操作接口，包括写入、查询和管理功能。
//
// 字段说明：
//
//   - UnimplementedMicroTSServer: 嵌入 gRPC 生成的未实现桩，确保向前兼容
//   - engine:                     存储引擎实例，any 类型以适应接口演进
//
// 并发安全：
//
//	gRPC 框架保证每个请求在独立 goroutine 中处理。
//	service 的无状态设计确保并发安全。
//
// 使用示例：
//
//	engine, _ := engine.New(&engine.Config{...})
//	service := api.New(engine)
//
//	grpcServer := grpc.NewServer()
//	pb.RegisterMicroTSServer(grpcServer, service)
//	listener, _ := net.Listen("tcp", ":50051")
//	grpcServer.Serve(listener)
type MicroTSService struct {
	pb.UnimplementedMicroTSServer
	engine any
}

// New 创建 gRPC 服务实例。
//
// 参数：
//   - engine: 存储引擎实例，类型为 engine.Engine（使用 any 以减轻耦合）
//
// 返回：
//   - *MicroTSService: 服务实例
//
// 说明：
//
//	当前 engine 使用 any 类型注入，实际运行时需要断言为 *engine.Engine。
//	这种设计允许服务层和引擎层独立演进。
func New(engine any) *MicroTSService {
	return &MicroTSService{
		engine: engine,
	}
}

// Write 处理单点写入请求。
//
// 参数：
//   - ctx: gRPC 上下文
//   - req: 写入请求，包含 DataPoint
//
// 返回：
//   - *pb.WriteResponse: 写入响应，Success=true 表示成功
//   - error: 处理失败时返回 gRPC 错误
//
// 注意：
//
//	当前为桩实现，总是返回 Success=true。
//	完整实现需要将 pb.WriteRequest 转换为 *types.Point 并调用引擎。
func (s *MicroTSService) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	return &pb.WriteResponse{
		Success: true,
	}, nil
}

// WriteBatch 处理批量写入请求。
//
// 参数：
//   - ctx: gRPC 上下文
//   - req: 批量写入请求，包含多个 DataPoint
//
// 返回：
//   - *pb.WriteBatchResponse: 批量写入响应
//   - error: 处理失败时返回 gRPC 错误
//
// 响应字段：
//
//   - Success: 是否全部成功
//   - Count:   实际处理的点数
//
// 注意：
//
//	当前为桩实现，返回 Success=true 和点数统计。
//	完整实现需要批量转换并调用 engine.WriteBatch。
func (s *MicroTSService) WriteBatch(ctx context.Context, req *pb.WriteBatchRequest) (*pb.WriteBatchResponse, error) {
	return &pb.WriteBatchResponse{
		Success: true,
		Count:   int32(len(req.Points)),
	}, nil
}

// QueryRange 处理范围查询请求。
//
// 参数：
//   - ctx: gRPC 上下文
//   - req: 查询请求，包含时间范围和过滤条件
//
// 返回：
//   - *pb.QueryRangeResponse: 查询结果
//   - error: 查询失败时返回 gRPC 错误
//
// 注意：
//
//	当前为桩实现，返回空结果。
//	完整实现需要将请求转换为 QueryRangeRequest，
//	调用引擎查询，并将结果转换为 pb.Row 列表。
func (s *MicroTSService) QueryRange(ctx context.Context, req *pb.QueryRangeRequest) (*pb.QueryRangeResponse, error) {
	return &pb.QueryRangeResponse{
		Database:    req.Database,
		Measurement: req.Measurement,
		StartTime:   req.StartTime,
		EndTime:     req.EndTime,
		TotalCount:  0,
		HasMore:     false,
		Rows:        []*pb.Row{},
	}, nil
}

// ListMeasurements 处理列出 Measurement 请求。
//
// 参数：
//   - ctx: gRPC 上下文
//   - req: 列表请求，包含数据库名称
//
// 返回：
//   - *pb.ListMeasurementsResponse: Measurement 列表
//   - error: 查询失败时返回 gRPC 错误
//
// 注意：
//
//	当前为桩实现，返回空列表。
//	完整实现需要从元数据存储中读取 Measurement 列表。
func (s *MicroTSService) ListMeasurements(ctx context.Context, req *pb.ListMeasurementsRequest) (*pb.ListMeasurementsResponse, error) {
	return &pb.ListMeasurementsResponse{
		Measurements: []string{},
	}, nil
}

// CreateMeasurement 处理创建 Measurement 请求。
//
// 参数：
//   - ctx: gRPC 上下文
//   - req: 创建请求，包含数据库和 Measurement 名称
//
// 返回：
//   - *pb.CreateMeasurementResponse: 创建结果
//   - error: 创建失败时返回 gRPC 错误
//
// 注意：
//
//	当前为桩实现，总是返回 Success=true。
//	完整实现需要调用引擎创建 Measurement 元数据。
func (s *MicroTSService) CreateMeasurement(ctx context.Context, req *pb.CreateMeasurementRequest) (*pb.CreateMeasurementResponse, error) {
	return &pb.CreateMeasurementResponse{
		Success: true,
	}, nil
}

// DropMeasurement 处理删除 Measurement 请求。
//
// 参数：
//   - ctx: gRPC 上下文
//   - req: 删除请求，包含数据库和 Measurement 名称
//
// 返回：
//   - *pb.DropMeasurementResponse: 删除结果
//   - error: 删除失败时返回 gRPC 错误
//
// 注意：
//
//	当前为桩实现，总是返回 Success=true。
//	完整实现需要调用引擎删除 Measurement 元数据。
func (s *MicroTSService) DropMeasurement(ctx context.Context, req *pb.DropMeasurementRequest) (*pb.DropMeasurementResponse, error) {
	return &pb.DropMeasurementResponse{
		Success: true,
	}, nil
}

// ListDatabases 处理列出数据库请求。
//
// 参数：
//   - ctx: gRPC 上下文
//   - req: 列表请求（当前为空）
//
// 返回：
//   - *pb.ListDatabasesResponse: 数据库列表
//   - error: 查询失败时返回 gRPC 错误
//
// 注意：
//
//	当前为桩实现，返回空列表。
//	完整实现需要从元数据存储中读取数据库列表。
func (s *MicroTSService) ListDatabases(ctx context.Context, req *pb.ListDatabasesRequest) (*pb.ListDatabasesResponse, error) {
	return &pb.ListDatabasesResponse{
		Databases: []string{},
	}, nil
}

// CreateDatabase 处理创建数据库请求。
//
// 参数：
//   - ctx: gRPC 上下文
//   - req: 创建请求，包含数据库名称
//
// 返回：
//   - *pb.CreateDatabaseResponse: 创建结果
//   - error: 创建失败时返回 gRPC 错误
//
// 注意：
//
//	当前为桩实现，总是返回 Success=true。
//	完整实现需要调用引擎创建数据库元数据。
func (s *MicroTSService) CreateDatabase(ctx context.Context, req *pb.CreateDatabaseRequest) (*pb.CreateDatabaseResponse, error) {
	return &pb.CreateDatabaseResponse{
		Success: true,
	}, nil
}

// DropDatabase 处理删除数据库请求。
//
// 参数：
//   - ctx: gRPC 上下文
//   - req: 删除请求，包含数据库名称
//
// 返回：
//   - *pb.DropDatabaseResponse: 删除结果
//   - error: 删除失败时返回 gRPC 错误
//
// 注意：
//
//	当前为桩实现，总是返回 Success=true。
//	完整实现需要调用引擎删除数据库元数据。
func (s *MicroTSService) DropDatabase(ctx context.Context, req *pb.DropDatabaseRequest) (*pb.DropDatabaseResponse, error) {
	return &pb.DropDatabaseResponse{
		Success: true,
	}, nil
}

// Health 处理健康检查请求。
//
// 参数：
//   - ctx: gRPC 上下文
//   - req: 健康检查请求（当前为空）
//
// 返回：
//   - *pb.HealthResponse: 健康状态响应
//   - error: 处理失败时返回 gRPC 错误
//
// 响应字段：
//
//   - Healthy: 服务是否健康
//   - Version: 服务版本号
//
// 典型用途：
//
//	用于负载均衡器健康检查、Kubernetes liveness/readiness 探针。
func (s *MicroTSService) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{
		Healthy: true,
		Version: "1.0.0",
	}, nil
}

// ToProtoPointRow 将 types.PointRow 转换为 pb.Row。
//
// 参数：
//   - row: 内部 PointRow 结构，可以为 nil
//
// 返回：
//   - *pb.Row: 转换后的 gRPC Row 结构
//
// 说明：
//
//	如果输入为 nil，返回 nil。
//	当前实现仅转换时间戳和标签，字段转换待完善。
func ToProtoPointRow(row *types.PointRow) *pb.Row {
	if row == nil {
		return nil
	}
	return &pb.Row{
		Timestamp: row.Timestamp,
		Tags:      row.Tags,
		Fields:    nil,
	}
}
