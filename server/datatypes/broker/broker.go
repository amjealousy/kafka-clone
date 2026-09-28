package broker

import (
	"context"

	"kafka-clone/server/datatypes/broker/dto"
	"kafka-clone/server/datatypes/encode"
	gen "kafka-clone/server/datatypes/proto-generated"

	"google.golang.org/protobuf/proto"
)

// CommandContext задаёт транспорт для broker data-plane API. TCP использует
// protobuf frames, HTTP может декодировать protobuf JSON и отправлять JSON/SSE.
type CommandContext interface {
	Context() context.Context
	CommandType() encode.Command
	Decode(body []byte, message proto.Message) error
	Respond(message proto.Message) error
}

type IReplicationBroker interface {
	ReplicationLogHandler(ctx context.Context, req *gen.AppendEntriesRequest) (*gen.AppendEntriesResponse, error)
	FetchLogHandler(ctx context.Context, req *gen.FetchLogRequest) (*gen.FetchLogResponse, error)
	InvalidateLogHandler(ctx context.Context, req *gen.InvalidateRequest) (*gen.InvalidateAck, error)
}

type ClusterRole string

const (
	Controller ClusterRole = "Controller" // only cluster can manage topics settings
	Follower   ClusterRole = "Follower"
	Unroled    ClusterRole = "Unroled"
)

type BrokerProducer interface {
	Produce(ctx CommandContext, request dto.ProduceRequest) error
}
type BrokerConsumer interface {
	Consume(ctx CommandContext, request dto.ConsumeRequest) error
}

// APIBroker содержит только операции, необходимые HTTP data-plane API.
type APIBroker interface {
	BrokerProducer
	BrokerConsumer
}
