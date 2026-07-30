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
	// InSync == false означает "просто реплика": нода принимает новые сообщения
	// от лидера, но её локальный лог ещё не восстановлен до актуального оффсета.
	// InSync == true — лог догнан, реплика является in-sync (ISR).
	InSync bool
}
