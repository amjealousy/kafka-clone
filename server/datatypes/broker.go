package datatypes

import "context"

type IBroker interface {
	ReplicationLogHandler(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
}
type ClusterRole string

const (
	Controller ClusterRole = "Controller" // only cluster can manage topics settings
	Follower   ClusterRole = "Follower"
	Unroled    ClusterRole = "Unroled"
)
