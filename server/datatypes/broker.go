package datatypes

import (
	"context"
	gen "kafka-clone/server/datatypes/proto-generated"
)

type IBroker interface {
	ReplicationLogHandler(ctx context.Context, req *gen.AppendEntriesRequest) (*gen.AppendEntriesResponse, error)
	FetchLogHandler(ctx context.Context, req *gen.FetchLogRequest) (*gen.FetchLogResponse, error)
}
type ClusterRole string

const (
	Controller ClusterRole = "Controller" // only cluster can manage topics settings
	Follower   ClusterRole = "Follower"
	Unroled    ClusterRole = "Unroled"
)
