package grpc

import (
	"context"
	"log/slog"

	"kafka-clone/server/datatypes"
	gen "kafka-clone/server/datatypes/proto-generated"
)

// ControlGrpcServer обслуживает административный control-plane API кластера
// (DescribeTopic/CreateTopic). Работает на ОТДЕЛЬНОМ gRPC-порту, независимом
// от ReplicationGrpcServer (порт репликации лога) и от TCP-порта клиентов
// produce/consume.
type ControlGrpcServer struct {
	gen.UnimplementedControlServiceServer
	controller datatypes.ITopicManager
	log        *slog.Logger
}

func NewControlGrpcServer(controller datatypes.ITopicManager, log *slog.Logger) *ControlGrpcServer {
	return &ControlGrpcServer{
		controller: controller,
		log:        log.With("component", "ControlAPI"),
	}
}

// DescribeTopic преобразует protobuf-запрос и транспорт-независимый ответ
// контроллера. Ответить может любая нода, т.к. ответ строится из локально
// видимого состояния etcd.
func (s *ControlGrpcServer) DescribeTopic(ctx context.Context, req *gen.DescribeTopicRequest) (*gen.DescribeTopicResponse, error) {
	response, err := s.controller.DescribeTopic(ctx, datatypes.DescribeTopicRequest{
		TopicName: req.GetTopicName(),
	})
	if err != nil {
		return nil, err
	}

	partitions := make([]*gen.PartitionInfo, 0, len(response.Partitions))
	for _, partition := range response.Partitions {
		replicas := make([]*gen.ReplicaInfo, 0, len(partition.Replicas))
		for _, replica := range partition.Replicas {
			replicas = append(replicas, &gen.ReplicaInfo{
				NodeId:  replica.NodeID,
				Address: replica.Address,
				InSync:  replica.InSync,
			})
		}
		partitions = append(partitions, &gen.PartitionInfo{
			PartitionId:   partition.PartitionID,
			LeaderNodeId:  partition.LeaderNodeID,
			LeaderAddress: partition.LeaderAddress,
			Replicas:      replicas,
		})
	}

	return &gen.DescribeTopicResponse{
		Found:      response.Found,
		Error:      errorMessage(response.Error),
		TopicName:  response.TopicName,
		Partitions: partitions,
	}, nil
}

// CreateTopic преобразует protobuf-запрос и транспорт-независимый ответ
// контроллера. Если данная нода не является текущим контроллером кластера,
// NodeCoordinator вернёт not_controller=true и адрес актуального контроллера.
func (s *ControlGrpcServer) CreateTopic(ctx context.Context, req *gen.CreateTopicRequest) (*gen.CreateTopicResponse, error) {
	response, err := s.controller.CreateTopic(ctx, datatypes.CreateTopicRequest{
		TopicName:         req.GetTopicName(),
		NumPartitions:     int(req.GetNumPartitions()),
		ReplicationFactor: int(req.GetReplicationFactor()),
	})
	if err != nil {
		return nil, err
	}

	return &gen.CreateTopicResponse{
		Success:           response.Success,
		Error:             errorMessage(response.Error),
		NotController:     response.NotController,
		ControllerAddress: response.ControllerAddress,
	}, nil
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
