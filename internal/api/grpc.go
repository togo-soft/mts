package api

import (
	"context"

	"micro-ts/internal/api/pb"
	"micro-ts/types"
)

// MicroTSService gRPC 服务实现
type MicroTSService struct {
	pb.UnimplementedMicroTSServer
	engine interface{}
}

// New 创建服务
func New(engine interface{}) *MicroTSService {
	return &MicroTSService{
		engine: engine,
	}
}

// Write 处理写入请求
func (s *MicroTSService) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	return &pb.WriteResponse{
		Success: true,
	}, nil
}

// WriteBatch 处理批量写入请求
func (s *MicroTSService) WriteBatch(ctx context.Context, req *pb.WriteBatchRequest) (*pb.WriteBatchResponse, error) {
	return &pb.WriteBatchResponse{
		Success: true,
		Count:   int32(len(req.Points)),
	}, nil
}

// QueryRange 处理范围查询请求
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

// ListMeasurements 处理列出 Measurement 请求
func (s *MicroTSService) ListMeasurements(ctx context.Context, req *pb.ListMeasurementsRequest) (*pb.ListMeasurementsResponse, error) {
	return &pb.ListMeasurementsResponse{
		Measurements: []string{},
	}, nil
}

// Health 处理健康检查请求
func (s *MicroTSService) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{
		Healthy: true,
		Version: "1.0.0",
	}, nil
}

// ToProtoPointRow converts a types.PointRow to pb.Row
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
