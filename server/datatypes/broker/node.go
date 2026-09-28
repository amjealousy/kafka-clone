package broker

// ClusterNode — описание одной ноды кластера в том виде, в каком его отдаёт
// control-plane HTTP API. Все адреса здесь advertised, то есть те, по которым
// к ноде реально можно обратиться извне её процесса (см. cluster.NodeAddresses).
type ClusterNode struct {
	ID   int64  `json:"id,string"`
	Role string `json:"role"`
	// IsController — авторитетный признак: нода указана лидером в
	// /kafka/controller/state И её ключ с lease жив. Полю Role доверять для
	// этого нельзя: оно пишется в /kafka/nodes/<id> асинхронно и во время
	// failover может показывать ноль или два контроллера сразу.
	IsController       bool   `json:"is_controller"`
	TCPAddress         string `json:"tcp_address"`
	HTTPAddress        string `json:"http_address"`
	ControlAddress     string `json:"control_address"`
	ReplicationAddress string `json:"replication_address"`
	StartTime          string `json:"start_time"`
}

// ClusterSnapshot — состав кластера на момент одного чтения etcd.
type ClusterSnapshot struct {
	Nodes []ClusterNode `json:"nodes"`
	// Controller == nil означает, что контроллер сейчас не избран (идут
	// выборы либо предыдущий контроллер умер, а его аренда уже истекла).
	Controller *ClusterNode `json:"controller"`
	// ServedBy — id ноды, ответившей на запрос. Нужен клиенту, чтобы понимать,
	// с кем он сейчас говорит, и логировать это при разборе инцидентов.
	ServedBy int64 `json:"served_by,string"`
}

// NodeReadiness — ответ /readyz. Нода готова обслуживать клиента, только если
// она прочитала etcd И видит себя в составе кластера. Второе условие важнее,
// чем кажется: нода может сохранять соединение с etcd для чтения, но уже быть
// исключённой из кластера по истёкшей аренде — её партиции переназначены, а
// сама она отвечает на запросы как ни в чём не бывало.
type NodeReadiness struct {
	NodeID       int64 `json:"node_id,string"`
	Ready        bool  `json:"ready"`
	InCluster    bool  `json:"in_cluster"`
	IsController bool  `json:"is_controller"`
	ClusterSize  int   `json:"cluster_size"`
	ControllerID int64 `json:"controller_id,string"`
	// Reason заполняется только когда Ready == false.
	Reason string `json:"reason,omitempty"`
}

// ClusterReplica — одна реплика партиции.
type ClusterReplica struct {
	NodeID int64 `json:"node_id,string"`
	InSync bool  `json:"in_sync"`
	// Alive — нода реплики присутствует в составе кластера в ЭТОМ ЖЕ снимке.
	Alive bool `json:"alive"`
}

// ClusterPartition — партиция топика с уже разрезолвленными адресами лидера.
type ClusterPartition struct {
	PartitionID  int64 `json:"partition_id,string"`
	LeaderNodeID int64 `json:"leader_node_id,string"`
	// Адреса лидера разрезолвлены из того же снимка, что и список нод,
	// поэтому они не могут "разъехаться" со списком нод из-за гонки.
	LeaderTCPAddress  string `json:"leader_tcp_address"`
	LeaderHTTPAddress string `json:"leader_http_address"`
	// LeaderAlive == false — партиция без живого лидера, писать в неё нельзя
	// до завершения реконфигурации контроллером.
	LeaderAlive bool             `json:"leader_alive"`
	Replicas    []ClusterReplica `json:"replicas"`
}

// ClusterTopic — топик кластера в представлении control-plane API.
type ClusterTopic struct {
	Name              string             `json:"name"`
	ReplicationFactor int                `json:"replication_factor"`
	Partitions        []ClusterPartition `json:"partitions"`
}

// ClusterOverview — единый согласованный снимок кластера: состав нод,
// действующий контроллер и все топики. Все три части читаются из etcd ОДНОЙ
// транзакцией, то есть соответствуют одной ревизии. Благодаря этому
// LeaderNodeID любой партиции гарантированно разрешается в ноду из Nodes.
type ClusterOverview struct {
	Nodes           []ClusterNode  `json:"nodes"`
	Controller      *ClusterNode   `json:"controller"`
	ControllerEpoch int64          `json:"controller_epoch,string"`
	Topics          []ClusterTopic `json:"topics"`
	// ServedBy — id ноды, ответившей на запрос.
	ServedBy int64 `json:"served_by,string"`
}

type ClusterEvent struct {
	Revision int64        `json:"revision,string"`
	Type     string       `json:"type"`
	Node     *ClusterNode `json:"node,omitempty"`
	NodeID   int64        `json:"node_id,omitempty,string"`
}
