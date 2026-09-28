package grpc

import (
	"context"
	"kafka-clone/server/datatypes/broker"
	gen "kafka-clone/server/datatypes/proto-generated"
	"log/slog"
)

type ReplicationGrpcServer struct {
	gen.UnimplementedReplicationServiceServer
	broker broker.IReplicationBroker // Ссылка на пул топиков
	log    *slog.Logger
}

func NewReplicationGrpcServer(broker broker.IReplicationBroker, log *slog.Logger) *ReplicationGrpcServer {
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

// InvalidateLastOffset обрабатывает команду лидера откатить последнюю запись
// лога этой реплики: лидер разослал запись, но не смог закоммитить её у себя,
// поэтому запись должна исчезнуть и здесь.
func (s *ReplicationGrpcServer) InvalidateLastOffset(ctx context.Context, req *gen.InvalidateRequest) (*gen.InvalidateAck, error) {
	return s.broker.InvalidateLogHandler(ctx, req)
}
