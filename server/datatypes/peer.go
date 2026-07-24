package datatypes

import (
	"google.golang.org/grpc"
)

type Peer struct {
	ID                int
	Addr              string
	GrpcConn          *grpc.ClientConn
	ReplicationClient ReplicationServiceClient
}
