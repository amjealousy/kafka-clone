package datatypes

import (
	"context"
	"errors"
	"kafka-clone/server/datatypes/broker"
)

// Доменные ошибки control-plane. Живут здесь, а не в пакете cluster, чтобы
// транспортные слои (HTTP/gRPC) могли сопоставлять их через errors.Is, не
// завися от конкретной реализации координатора.
var (
	ErrTopicNotFound        = errors.New("NodeController - topic not found")
	ErrTopicCredential      = errors.New("NodeController - topic credential error")
	ErrTopicAlreadyExists   = errors.New("NodeController - topic already exists")
	ErrNotClusterController = errors.New("this node is not the cluster controller")
	ErrNotEnoughAliveNodes  = errors.New("there are not enough alive nodes")
	ErrRaceCondition        = errors.New("preventing race condition error, try later")
	// ErrControllerUnavailable — контроллер не избран: предыдущий умер, его
	// аренда истекла, новый ещё не выиграл выборы. Состояние временное.
	ErrControllerUnavailable = errors.New("cluster controller is not elected yet")
	// ErrNodeEvictedFromCluster — нода читает etcd, но её собственного ключа в
	// /kafka/nodes/ уже нет: аренда истекла и кластер её исключил. Данные она
	// отдала бы корректные, но точкой входа для клиента быть не может.
	ErrNodeEvictedFromCluster = errors.New("this node is no longer a member of the cluster")
)

// ITopicManager — интерфейс, который реализует cluster.NodeCoordinator для
// обнаружения топологии топиков и создания новых топиков. Интерфейс не зависит
// от транспортного протокола: gRPC- и HTTP-слои преобразуют свои DTO сами.
type ITopicManager interface {
	DescribeTopic(ctx context.Context, req DescribeTopicRequest) (DescribeTopicResponse, error)
	CreateTopic(ctx context.Context, req CreateTopicRequest) (CreateTopicResponse, error)
}

// IClusterDiscovery — чтение топологии кластера. Все реализации обязаны читать
// данные напрямую из etcd на момент вызова: нода, отвалившаяся от кластера,
// должна вернуть ошибку, а не отдать правдоподобный устаревший снимок.
type IClusterDiscovery interface {
	// GetClusterOverview возвращает ноды, контроллера и топики одним
	// согласованным снимком (одна ревизия etcd).
	GetClusterOverview(ctx context.Context) (broker.ClusterOverview, error)
	GetClusterNodeList(ctx context.Context) (broker.ClusterSnapshot, error)
	GetClusterTopicsList(ctx context.Context) ([]broker.ClusterTopic, error)
	// GetClusterTopic возвращает один топик; found=false, если его нет.
	GetClusterTopic(ctx context.Context, name string) (topic broker.ClusterTopic, found bool, err error)
	// FindControllerAddress возвращает control-plane gRPC-адрес действующего
	// контроллера. Пустая строка без ошибки означает, что контроллер сейчас не
	// избран (идут выборы) — это штатное состояние, а не сбой.
	FindControllerAddress(ctx context.Context) (string, error)
	// GetNodeReadiness — данные для /readyz. Ошибку не возвращает: у пробы не
	// должно быть неоднозначных состояний, причина неготовности кодируется
	// в самом ответе.
	GetNodeReadiness(ctx context.Context) broker.NodeReadiness
}

// ITopicRouter — мутации, которые обязан выполнять контроллер. Реализация сама
// находит контроллера в etcd и проксирует запрос ему, поэтому вызывающей
// стороне (HTTP-слою, UI) не нужно знать топологию кластера.
type ITopicRouter interface {
	CreateTopicRouted(ctx context.Context, req CreateTopicRequest) (CreateTopicResponse, error)
}

// IClusterAdmin — полный control-plane контракт, который потребляет HTTP-слой.
type IClusterAdmin interface {
	ITopicManager
	IClusterDiscovery
	ITopicRouter
}

type DescribeTopicRequest struct {
	TopicName string `json:"topic_name"`
}

type ReplicaInfo struct {
	NodeID  int64  `json:"node_id"`
	Address string `json:"address"`
	InSync  bool   `json:"in_sync"`
}

type PartitionInfo struct {
	PartitionID   int64         `json:"partition_id"`
	LeaderNodeID  int64         `json:"leader_node_id"`
	LeaderAddress string        `json:"leader_address"`
	Replicas      []ReplicaInfo `json:"replicas"`
}

type DescribeTopicResponse struct {
	Found      bool            `json:"found"`
	Error      error           `json:"error,omitempty"`
	TopicName  string          `json:"topic_name"`
	Partitions []PartitionInfo `json:"partitions"`
}

type CreateTopicRequest struct {
	TopicName         string `json:"topic_name"`
	NumPartitions     int    `json:"num_partitions"`
	ReplicationFactor int    `json:"replication_factor"`
}

type CreateTopicResponse struct {
	Success           bool   `json:"success"`
	Error             error  `json:"error,omitempty"`
	NotController     bool   `json:"not_controller"`
	ControllerAddress string `json:"controller_address,omitempty"`
}
