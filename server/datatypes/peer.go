package datatypes

import (
	gen "kafka-clone/server/datatypes/proto-generated"

	"google.golang.org/grpc"
)

type Peer struct {
	ID                int
	Addr              string
	GrpcConn          *grpc.ClientConn
	ReplicationClient gen.ReplicationServiceClient
}
