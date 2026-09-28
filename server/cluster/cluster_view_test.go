package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	brokertypes "kafka-clone/server/datatypes/broker"
	"strings"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

func nodeKV(t *testing.T, state NodeState) *mvccpb.KeyValue {
	t.Helper()
	value, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal node state: %v", err)
	}
	return &mvccpb.KeyValue{
		Key:   fmt.Appendf(nil, "%s%d", NodesDiscoveryPath, state.ID),
		Value: value,
	}
}

func stateKV(t *testing.T, leaderID, epoch int64) *mvccpb.KeyValue {
	t.Helper()
	value, err := json.Marshal(ControllerState{LeaderId: leaderID, LeaderEpoch: epoch})
	if err != nil {
		t.Fatalf("marshal controller state: %v", err)
	}
	return &mvccpb.KeyValue{Key: []byte(StatePath), Value: value}
}

func topicKV(t *testing.T, params TopicParams) *mvccpb.KeyValue {
	t.Helper()
	value, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal topic params: %v", err)
	}
	return &mvccpb.KeyValue{Key: fmt.Appendf(nil, "%s%s", TopicPath, params.Name), Value: value}
}

func testNodes(t *testing.T) []*mvccpb.KeyValue {
	t.Helper()
	return []*mvccpb.KeyValue{
		nodeKV(t, NodeState{
			ID: 2, Address: "10.0.0.2:6090", TcpAddress: "10.0.0.2:5090",
			ControlAddress: "10.0.0.2:7090", HttpAddress: "10.0.0.2:8090",
			Role: brokertypes.Follower,
		}),
		nodeKV(t, NodeState{
			ID: 1, Address: "10.0.0.1:6090", TcpAddress: "10.0.0.1:5090",
			ControlAddress: "10.0.0.1:7090", HttpAddress: "10.0.0.1:8090",
			Role: brokertypes.Controller,
		}),
	}
}

func TestBuildClusterViewResolvesLiveController(t *testing.T) {
	view := buildClusterView(testNodes(t), []*mvccpb.KeyValue{stateKV(t, 1, 7)}, nil)

	if view.controller == nil {
		t.Fatal("controller is nil, want node 1")
	}
	if view.controller.ID != 1 || view.controller.ControlAddress != "10.0.0.1:7090" {
		t.Fatalf("unexpected controller: %+v", view.controller)
	}
	if view.epoch != 7 {
		t.Fatalf("epoch = %d, want 7", view.epoch)
	}
}

// Ключ /kafka/controller/state живёт без аренды, поэтому после смерти
// контроллера в нём остаётся id покойника. Признавать такого контроллера
// нельзя: клиент отправил бы ему запись, которую некому обработать.
func TestBuildClusterViewRejectsControllerWithoutLiveLease(t *testing.T) {
	view := buildClusterView(testNodes(t), []*mvccpb.KeyValue{stateKV(t, 42, 9)}, nil)

	if view.controller != nil {
		t.Fatalf("controller = %+v, want nil for expired lease", view.controller)
	}
	if view.epoch != 9 {
		t.Fatalf("epoch = %d, want 9 (эпоха известна даже без живого лидера)", view.epoch)
	}
}

func TestBuildClusterViewWithoutControllerStateKey(t *testing.T) {
	view := buildClusterView(testNodes(t), nil, nil)

	if view.controller != nil {
		t.Fatalf("controller = %+v, want nil", view.controller)
	}
	if len(view.nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(view.nodes))
	}
}

// Роль в /kafka/nodes/<id> публикуется асинхронно и во время failover может
// показывать двух контроллеров сразу. Авторитетным должен остаться только тот,
// кого называет /kafka/controller/state.
func TestMapClusterOverviewTrustsStateKeyNotRoleField(t *testing.T) {
	nodes := []*mvccpb.KeyValue{
		nodeKV(t, NodeState{ID: 1, Role: brokertypes.Controller, ControlAddress: "10.0.0.1:7090"}),
		nodeKV(t, NodeState{ID: 2, Role: brokertypes.Controller, ControlAddress: "10.0.0.2:7090"}),
	}
	overview := mapClusterOverview(buildClusterView(nodes, []*mvccpb.KeyValue{stateKV(t, 2, 3)}, nil), 1)

	if overview.Controller == nil || overview.Controller.ID != 2 {
		t.Fatalf("controller = %+v, want node 2", overview.Controller)
	}
	if len(overview.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(overview.Nodes))
	}
	if overview.Nodes[0].ID != 1 || overview.Nodes[0].IsController {
		t.Fatalf("node 1 must not be marked as controller: %+v", overview.Nodes[0])
	}
	if !overview.Nodes[1].IsController {
		t.Fatalf("node 2 must be marked as controller: %+v", overview.Nodes[1])
	}
}

func TestMapClusterOverviewResolvesPartitionLeaders(t *testing.T) {
	topics := []*mvccpb.KeyValue{topicKV(t, TopicParams{
		Name:                     "orders",
		DesiredReplicationFactor: 3,
		Partitions: []PartitionsParams{
			{PartitionId: 1, LeaderNodeId: 2, ReplicasNodeId: []int64{2, 1}, IsrNodeId: []int64{2}},
			{PartitionId: 0, LeaderNodeId: 1, ReplicasNodeId: []int64{1, 2}, IsrNodeId: []int64{1, 2}},
		},
	})}
	overview := mapClusterOverview(buildClusterView(testNodes(t), []*mvccpb.KeyValue{stateKV(t, 1, 1)}, topics), 1)

	if len(overview.Topics) != 1 || overview.Topics[0].Name != "orders" {
		t.Fatalf("unexpected topics: %+v", overview.Topics)
	}
	partitions := overview.Topics[0].Partitions
	if len(partitions) != 2 {
		t.Fatalf("partitions = %d, want 2", len(partitions))
	}
	if partitions[0].PartitionID != 0 || partitions[1].PartitionID != 1 {
		t.Fatalf("partitions must be sorted by id: %+v", partitions)
	}

	// Адрес лидера разрешается из того же снимка, что и список нод.
	if partitions[0].LeaderNodeID != 1 || partitions[0].LeaderTCPAddress != "10.0.0.1:5090" {
		t.Fatalf("unexpected leader for partition 0: %+v", partitions[0])
	}
	if partitions[0].LeaderHTTPAddress != "10.0.0.1:8090" || !partitions[0].LeaderAlive {
		t.Fatalf("unexpected leader addresses for partition 0: %+v", partitions[0])
	}
	if partitions[1].LeaderNodeID != 2 || partitions[1].LeaderTCPAddress != "10.0.0.2:5090" {
		t.Fatalf("unexpected leader for partition 1: %+v", partitions[1])
	}

	replicas := partitions[1].Replicas
	if len(replicas) != 2 || replicas[0].NodeID != 2 || !replicas[0].InSync || !replicas[0].Alive {
		t.Fatalf("unexpected replicas: %+v", replicas)
	}
	if replicas[1].NodeID != 1 || replicas[1].InSync {
		t.Fatalf("replica 1 must not be in sync: %+v", replicas[1])
	}
}

// Если лидер партиции уже выбыл из кластера, это должно быть видно в ответе, а
// не маскироваться пустым адресом, который UI примет за рабочую партицию.
func TestMapClusterOverviewMarksDeadPartitionLeader(t *testing.T) {
	topics := []*mvccpb.KeyValue{topicKV(t, TopicParams{
		Name:       "orders",
		Partitions: []PartitionsParams{{PartitionId: 0, LeaderNodeId: 99, ReplicasNodeId: []int64{99, 1}}},
	})}
	overview := mapClusterOverview(buildClusterView(testNodes(t), []*mvccpb.KeyValue{stateKV(t, 1, 1)}, topics), 1)

	partition := overview.Topics[0].Partitions[0]
	if partition.LeaderAlive {
		t.Fatalf("leader_alive = true for node outside the cluster: %+v", partition)
	}
	if partition.Replicas[0].Alive {
		t.Fatalf("replica 99 must be marked as dead: %+v", partition.Replicas[0])
	}
	if !partition.Replicas[1].Alive {
		t.Fatalf("replica 1 must be marked as alive: %+v", partition.Replicas[1])
	}
}

// Значение ключа ноды может быть записано частично заполненной структурой
// (см. electionLoop, где сериализуется пустой NodeState при первом проходе),
// поэтому id обязан восстанавливаться из имени ключа.
func TestBuildClusterViewRecoversNodeIDFromKey(t *testing.T) {
	value, err := json.Marshal(NodeState{Role: brokertypes.Controller})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	kv := &mvccpb.KeyValue{Key: []byte(NodesDiscoveryPath + "5"), Value: value}

	view := buildClusterView([]*mvccpb.KeyValue{kv}, []*mvccpb.KeyValue{stateKV(t, 5, 1)}, nil)

	if _, ok := view.nodes[5]; !ok {
		t.Fatalf("node id must be recovered from key: %+v", view.nodes)
	}
	if view.controller == nil || view.controller.ID != 5 {
		t.Fatalf("controller = %+v, want node 5", view.controller)
	}
}

func TestMapClusterOverviewReportsServingNode(t *testing.T) {
	overview := mapClusterOverview(buildClusterView(testNodes(t), []*mvccpb.KeyValue{stateKV(t, 1, 1)}, nil), 2)

	if overview.ServedBy != 2 {
		t.Fatalf("served_by = %d, want 2", overview.ServedBy)
	}
}

// Нода может сохранять соединение с etcd для чтения и при этом уже быть
// исключённой из кластера по истёкшей аренде. Данные она отдала бы корректные,
// но клиента обслуживать не имеет права — иначе он закрепится за нодой, у
// которой партиции уже переназначены.
func TestEnsureMemberRejectsEvictedNode(t *testing.T) {
	view := buildClusterView(testNodes(t), []*mvccpb.KeyValue{stateKV(t, 1, 1)}, nil)

	if err := ensureMember(view, 1); err != nil {
		t.Fatalf("node 1 is a member, got error: %v", err)
	}
	err := ensureMember(view, 42)
	if !errors.Is(err, ErrNodeEvictedFromCluster) {
		t.Fatalf("err = %v, want ErrNodeEvictedFromCluster", err)
	}
	if !strings.Contains(err.Error(), "42") {
		t.Fatalf("err must name the node: %v", err)
	}
}

func TestBuildNodeReadiness(t *testing.T) {
	view := buildClusterView(testNodes(t), []*mvccpb.KeyValue{stateKV(t, 1, 1)}, nil)

	ready := buildNodeReadiness(view, 1)
	if !ready.Ready || !ready.InCluster || !ready.IsController {
		t.Fatalf("unexpected readiness for controller node: %+v", ready)
	}
	if ready.ClusterSize != 2 || ready.ControllerID != 1 {
		t.Fatalf("unexpected readiness payload: %+v", ready)
	}

	follower := buildNodeReadiness(view, 2)
	if !follower.Ready || follower.IsController {
		t.Fatalf("follower must be ready but not controller: %+v", follower)
	}

	evicted := buildNodeReadiness(view, 42)
	if evicted.Ready || evicted.InCluster || evicted.Reason == "" {
		t.Fatalf("evicted node must not be ready: %+v", evicted)
	}
}

// Когда контроллер не избран, ControllerID == 0, и это не должно случайно
// сделать контроллером ноду с нулевым id.
func TestBuildNodeReadinessWithoutController(t *testing.T) {
	view := buildClusterView(testNodes(t), nil, nil)

	ready := buildNodeReadiness(view, 1)
	if ready.IsController || ready.ControllerID != 0 {
		t.Fatalf("unexpected readiness without controller: %+v", ready)
	}
	if !ready.Ready {
		t.Fatalf("нода остаётся готовой во время выборов: %+v", ready)
	}
}

func TestBuildClusterViewSkipsCorruptedValues(t *testing.T) {
	corrupted := &mvccpb.KeyValue{Key: []byte(NodesDiscoveryPath + "3"), Value: []byte("{not-json")}
	nodes := append(testNodes(t), corrupted)

	view := buildClusterView(nodes, nil, []*mvccpb.KeyValue{{Key: []byte(TopicPath + "bad"), Value: []byte("oops")}})

	if len(view.nodes) != 2 {
		t.Fatalf("nodes = %d, want 2 (битая запись должна игнорироваться)", len(view.nodes))
	}
	if len(view.topics) != 0 {
		t.Fatalf("topics = %d, want 0", len(view.topics))
	}
}
