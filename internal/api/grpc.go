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
	"fmt"

	"codeberg.org/micro-ts/mts/internal/api/pb"
	"codeberg.org/micro-ts/mts/internal/engine"
	"codeberg.org/micro-ts/mts/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MicroTSService 实现 gRPC MicroTS 服务。
//
// 提供时序数据库的核心操作接口，包括写入、查询和管理功能。
//
// 字段说明：
//
//   - UnimplementedMicroTSServer: 嵌入 gRPC 生成的未实现桩，确保向前兼容
//   - engine:                     存储引擎实例
//
// 并发安全：
//
//	gRPC 框架保证每个请求在独立 goroutine 中处理。
//	service 的无状态设计确保并发安全。
//
// 使用示例：
//
//	eng, _ := engine.New(&engine.Config{...})
//	service := api.New(eng)
//
//	grpcServer := grpc.NewServer()
//	pb.RegisterMicroTSServer(grpcServer, service)
//	listener, _ := net.Listen("tcp", ":50051")
//	grpcServer.Serve(listener)
type MicroTSService struct {
	pb.UnimplementedMicroTSServer
	engine *engine.Engine
}

// New 创建 gRPC 服务实例。
//
// 参数：
//   - eng: 存储引擎实例
//
// 返回：
//   - *MicroTSService: 服务实例
func New(eng *engine.Engine) *MicroTSService {
	return &MicroTSService{
		engine: eng,
	}
}

// fieldValueToAny 将 pb.FieldValue 转换为 interface{}。
func fieldValueToAny(fv *pb.FieldValue) (any, error) {
	switch v := fv.Value.(type) {
	case *pb.FieldValue_IntValue:
		return v.IntValue, nil
	case *pb.FieldValue_FloatValue:
		return v.FloatValue, nil
	case *pb.FieldValue_StringValue:
		return v.StringValue, nil
	case *pb.FieldValue_BoolValue:
		return v.BoolValue, nil
	default:
		return nil, fmt.Errorf("unknown field value type")
	}
}

// anyToFieldValue 将 interface{} 转换为 pb.FieldValue。
func anyToFieldValue(v any) (*pb.FieldValue, error) {
	switch val := v.(type) {
	case int64:
		return &pb.FieldValue{Value: &pb.FieldValue_IntValue{IntValue: val}}, nil
	case float64:
		return &pb.FieldValue{Value: &pb.FieldValue_FloatValue{FloatValue: val}}, nil
	case string:
		return &pb.FieldValue{Value: &pb.FieldValue_StringValue{StringValue: val}}, nil
	case bool:
		return &pb.FieldValue{Value: &pb.FieldValue_BoolValue{BoolValue: val}}, nil
	default:
		return nil, fmt.Errorf("unsupported field type: %T", v)
	}
}

// writeRequestToPoint 将 pb.WriteRequest 转换为 types.Point。
func writeRequestToPoint(req *pb.WriteRequest) (*types.Point, error) {
	fields := make(map[string]any, len(req.Fields))
	for name, fv := range req.Fields {
		val, err := fieldValueToAny(fv)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", name, err)
		}
		fields[name] = val
	}

	return &types.Point{
		Database:    req.Database,
		Measurement: req.Measurement,
		Tags:        req.Tags,
		Timestamp:   req.Timestamp,
		Fields:      fields,
	}, nil
}

// pointRowToProto 将 types.PointRow 转换为 pb.Row。
func pointRowToProto(row *types.PointRow) (*pb.Row, error) {
	if row == nil {
		return nil, nil
	}

	fields := make(map[string]*pb.FieldValue, len(row.Fields))
	for name, v := range row.Fields {
		fv, err := anyToFieldValue(v)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", name, err)
		}
		fields[name] = fv
	}

	return &pb.Row{
		Timestamp: row.Timestamp,
		Tags:      row.Tags,
		Fields:    fields,
	}, nil
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
func (s *MicroTSService) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	point, err := writeRequestToPoint(req)
	if err != nil {
		return &pb.WriteResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	if err := s.engine.Write(ctx, point); err != nil {
		return &pb.WriteResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

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
func (s *MicroTSService) WriteBatch(ctx context.Context, req *pb.WriteBatchRequest) (*pb.WriteBatchResponse, error) {
	points := make([]*types.Point, 0, len(req.Points))
	for i, p := range req.Points {
		point, err := writeRequestToPoint(p)
		if err != nil {
			return &pb.WriteBatchResponse{
				Success: false,
				Error:   fmt.Sprintf("point %d: %v", i, err),
			}, nil
		}
		points = append(points, point)
	}

	if err := s.engine.WriteBatch(ctx, points); err != nil {
		return &pb.WriteBatchResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.WriteBatchResponse{
		Success: true,
		Count:   int32(len(points)),
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
func (s *MicroTSService) QueryRange(ctx context.Context, req *pb.QueryRangeRequest) (*pb.QueryRangeResponse, error) {
	queryReq := &types.QueryRangeRequest{
		Database:    req.Database,
		Measurement: req.Measurement,
		StartTime:   req.StartTime,
		EndTime:     req.EndTime,
		Fields:      req.Fields,
		Tags:        req.Tags,
		Offset:      req.Offset,
		Limit:       req.Limit,
	}

	resp, err := s.engine.Query(ctx, queryReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query failed: %v", err)
	}

	rows := make([]*pb.Row, 0, len(resp.Rows))
	for i := range resp.Rows {
		row, err := pointRowToProto(&resp.Rows[i])
		if err != nil {
			return nil, status.Errorf(codes.Internal, "convert row %d: %v", i, err)
		}
		rows = append(rows, row)
	}

	return &pb.QueryRangeResponse{
		Database:    resp.Database,
		Measurement: resp.Measurement,
		StartTime:   resp.StartTime,
		EndTime:     resp.EndTime,
		TotalCount:  resp.TotalCount,
		HasMore:     resp.HasMore,
		Rows:        rows,
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
func (s *MicroTSService) ListMeasurements(ctx context.Context, req *pb.ListMeasurementsRequest) (*pb.ListMeasurementsResponse, error) {
	measurements, found := s.engine.ListMeasurements(req.Database)
	if !found {
		return &pb.ListMeasurementsResponse{
			Measurements: []string{},
		}, nil
	}
	return &pb.ListMeasurementsResponse{
		Measurements: measurements,
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
func (s *MicroTSService) CreateMeasurement(ctx context.Context, req *pb.CreateMeasurementRequest) (*pb.CreateMeasurementResponse, error) {
	_, err := s.engine.CreateMeasurement(req.Database, req.Measurement)
	if err != nil {
		return &pb.CreateMeasurementResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
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
func (s *MicroTSService) DropMeasurement(ctx context.Context, req *pb.DropMeasurementRequest) (*pb.DropMeasurementResponse, error) {
	found, err := s.engine.DropMeasurement(req.Database, req.Measurement)
	if err != nil {
		return &pb.DropMeasurementResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	if !found {
		return &pb.DropMeasurementResponse{
			Success: false,
			Error:   fmt.Sprintf("measurement not found: %s/%s", req.Database, req.Measurement),
		}, nil
	}
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
func (s *MicroTSService) ListDatabases(ctx context.Context, req *pb.ListDatabasesRequest) (*pb.ListDatabasesResponse, error) {
	databases := s.engine.ListDatabases()
	return &pb.ListDatabasesResponse{
		Databases: databases,
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
func (s *MicroTSService) CreateDatabase(ctx context.Context, req *pb.CreateDatabaseRequest) (*pb.CreateDatabaseResponse, error) {
	_ = s.engine.CreateDatabase(req.Database)
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
func (s *MicroTSService) DropDatabase(ctx context.Context, req *pb.DropDatabaseRequest) (*pb.DropDatabaseResponse, error) {
	found := s.engine.DropDatabase(req.Database)
	if !found {
		return &pb.DropDatabaseResponse{
			Success: false,
			Error:   fmt.Sprintf("database not found: %s", req.Database),
		}, nil
	}
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
