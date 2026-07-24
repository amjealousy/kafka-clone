package topic

import "time"

type Partition struct {
	Id          int
	Replicas    []Replica
	Retention   time.Duration
	StartOffset uint64
}

func (p *Partition) AddReplica(replica Replica) {
	p.Replicas = append(p.Replicas, replica)
}

type Replica struct {
	Id       int
	BrokerID string
	InSync   bool
}
