package grpc

import (
	"context"
	"kafka-clone/server/datatypes"
	"log/slog"
)

type ReplicationGrpcServer struct {
	datatypes.UnimplementedReplicationServiceServer
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
func (s *ReplicationGrpcServer) AppendEntries(ctx context.Context, req *datatypes.AppendEntriesRequest) (*datatypes.AppendEntriesResponse, error) {
	// Находим топик и партицию на этой локальной ноде

	handler, err := s.broker.ReplicationLogHandler(ctx, req)
	if err != nil {
		return nil, err
	}
	return handler, nil
}
