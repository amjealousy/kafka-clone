package grpc

import (
	"context"
	"kafka-clone/server/datatypes"
	gen "kafka-clone/server/datatypes/proto-generated"
	"log/slog"
)

// ControlGrpcServer обслуживает административный control-plane API кластера
// (DescribeTopic/CreateTopic). Работает на ОТДЕЛЬНОМ gRPC-порту, независимом
// от ReplicationGrpcServer (порт репликации лога) и от TCP-порта клиентов
// produce/consume.
type ControlGrpcServer struct {
	gen.UnimplementedControlServiceServer
	controller datatypes.IController
	log        *slog.Logger
}

func NewControlGrpcServer(controller datatypes.IController, log *slog.Logger) *ControlGrpcServer {
	return &ControlGrpcServer{
		controller: controller,
		log:        log.With("component", "ControlAPI"),
	}
}

// DescribeTopic делегирует запрос в NodeCoordinator: может ответить любая нода,
// т.к. ответ строится из локально видимого состояния etcd.
func (s *ControlGrpcServer) DescribeTopic(ctx context.Context, req *gen.DescribeTopicRequest) (*gen.DescribeTopicResponse, error) {
	return s.controller.DescribeTopic(ctx, req)
}

// CreateTopic делегирует запрос в NodeCoordinator. Если данная нода не
// является текущим контроллером кластера, NodeCoordinator сам вернёт
// not_controller=true и адрес актуального контроллера.
func (s *ControlGrpcServer) CreateTopic(ctx context.Context, req *gen.CreateTopicRequest) (*gen.CreateTopicResponse, error) {
	return s.controller.CreateTopic(ctx, req)
}
