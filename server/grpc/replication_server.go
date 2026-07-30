package grpc

import (
	"context"
	"kafka-clone/server/datatypes"
	gen "kafka-clone/server/datatypes/proto-generated"
	"log/slog"
)

type ReplicationGrpcServer struct {
	gen.UnimplementedReplicationServiceServer
	broker datatypes.IBroker // Ссылка на пул топиков
	log    *slog.Logger
}

func NewReplicationGrpcServer(broker datatypes.IBroker, log *slog.Logger) *ReplicationGrpcServer {
	return &ReplicationGrpcServer{
		broker: broker,
		log:    log.With("component", "InboundReplicationServer"),
	}
}

// AppendEntries обрабатывает входящий поток репликации, когда данный брокер является Слейвом
func (s *ReplicationGrpcServer) AppendEntries(ctx context.Context, req *gen.AppendEntriesRequest) (*gen.AppendEntriesResponse, error) {
	// Находим топик и партицию на этой локальной ноде
	resp, err := s.broker.ReplicationLogHandler(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// FetchLog обрабатывает запрос отстающей реплики на выкачивание диапазона лога.
// Этот брокер выступает источником (обычно лидером партиции).
func (s *ReplicationGrpcServer) FetchLog(ctx context.Context, req *gen.FetchLogRequest) (*gen.FetchLogResponse, error) {
	return s.broker.FetchLogHandler(ctx, req)
}
