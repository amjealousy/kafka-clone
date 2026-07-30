package datatypes

import "context"
import gen "kafka-clone/server/datatypes/proto-generated"

// IController — интерфейс, который реализует cluster.NodeCoordinator для
// обслуживания ControlService (административный gRPC control-plane API:
// обнаружение топологии топиков и создание новых топиков).
type IController interface {
	DescribeTopic(ctx context.Context, req *gen.DescribeTopicRequest) (*gen.DescribeTopicResponse, error)
	CreateTopic(ctx context.Context, req *gen.CreateTopicRequest) (*gen.CreateTopicResponse, error)
}
