package cluster

import (
	"cmp"
	brokertypes "kafka-clone/server/datatypes/broker"
	"slices"
)

// mapClusterNode переводит внутреннее состояние ноды в DTO control-plane API.
// controllerID передаётся отдельно, потому что признак "это контроллер" берётся
// из /kafka/controller/state, а не из поля Role (оно обновляется асинхронно).
func mapClusterNode(state NodeState, controllerID int64) brokertypes.ClusterNode {
	return brokertypes.ClusterNode{
		ID:                 state.ID,
		Role:               string(state.Role),
		IsController:       controllerID != 0 && state.ID == controllerID,
		TCPAddress:         state.TcpAddress,
		HTTPAddress:        state.HttpAddress,
		ControlAddress:     state.ControlAddress,
		ReplicationAddress: state.Address,
		StartTime:          state.StartTime,
	}
}

func controllerID(view *clusterView) int64 {
	if view.controller == nil {
		return 0
	}
	return view.controller.ID
}

func mapClusterNodes(view *clusterView) []brokertypes.ClusterNode {
	id := controllerID(view)
	nodes := make([]brokertypes.ClusterNode, 0, len(view.nodes))
	for _, state := range view.nodes {
		nodes = append(nodes, mapClusterNode(state, id))
	}
	slices.SortFunc(nodes, func(a, b brokertypes.ClusterNode) int {
		return cmp.Compare(a.ID, b.ID)
	})
	return nodes
}

func mapClusterController(view *clusterView) *brokertypes.ClusterNode {
	if view.controller == nil {
		return nil
	}
	node := mapClusterNode(*view.controller, view.controller.ID)
	return &node
}

func mapClusterSnapshot(view *clusterView, servedBy int64) brokertypes.ClusterSnapshot {
	return brokertypes.ClusterSnapshot{
		Nodes:      mapClusterNodes(view),
		Controller: mapClusterController(view),
		ServedBy:   servedBy,
	}
}

// mapClusterTopic разрешает лидеров и реплики партиций по составу нод ИЗ ТОГО
// ЖЕ снимка. Поэтому leader_alive/alive — не догадка, а факт на одной ревизии
// etcd: адрес лидера либо есть в этом же ответе, либо лидер мёртв и партиция
// ждёт реконфигурации контроллером.
func mapClusterTopic(view *clusterView, params TopicParams) brokertypes.ClusterTopic {
	partitions := make([]brokertypes.ClusterPartition, 0, len(params.Partitions))
	for _, part := range params.Partitions {
		leader, leaderAlive := view.nodes[part.LeaderNodeId]

		replicas := make([]brokertypes.ClusterReplica, 0, len(part.ReplicasNodeId))
		for _, id := range part.ReplicasNodeId {
			_, alive := view.nodes[id]
			replicas = append(replicas, brokertypes.ClusterReplica{
				NodeID: id,
				InSync: part.IsInSync(id),
				Alive:  alive,
			})
		}

		partitions = append(partitions, brokertypes.ClusterPartition{
			PartitionID:       part.PartitionId,
			LeaderNodeID:      part.LeaderNodeId,
			LeaderTCPAddress:  leader.TcpAddress,
			LeaderHTTPAddress: leader.HttpAddress,
			LeaderAlive:       leaderAlive,
			Replicas:          replicas,
		})
	}
	slices.SortFunc(partitions, func(a, b brokertypes.ClusterPartition) int {
		return cmp.Compare(a.PartitionID, b.PartitionID)
	})

	return brokertypes.ClusterTopic{
		Name:              params.Name,
		ReplicationFactor: params.DesiredReplicationFactor,
		Partitions:        partitions,
	}
}

func mapClusterTopics(view *clusterView) []brokertypes.ClusterTopic {
	topics := make([]brokertypes.ClusterTopic, 0, len(view.topics))
	for _, params := range view.topics {
		topics = append(topics, mapClusterTopic(view, params))
	}
	return topics
}

func mapClusterOverview(view *clusterView, servedBy int64) brokertypes.ClusterOverview {
	return brokertypes.ClusterOverview{
		Nodes:           mapClusterNodes(view),
		Controller:      mapClusterController(view),
		ControllerEpoch: view.epoch,
		Topics:          mapClusterTopics(view),
		ServedBy:        servedBy,
	}
}
