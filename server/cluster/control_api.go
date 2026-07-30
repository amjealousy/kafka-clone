package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"kafka-clone/server/datatypes"
	gen "kafka-clone/server/datatypes/proto-generated"
)

// DescribeTopic реализует datatypes.IController — обслуживает
// ControlService.DescribeTopic. Может быть вызван на ЛЮБОЙ ноде кластера:
// ответ строится из локально видимого состояния etcd (topicMeta), а адреса
// нод берутся из членства кластера (members), которое поддерживается
// WatchNodes независимо на каждой ноде. Специально обращаться именно к
// контроллеру для чтения метаданных не требуется.
func (n *NodeCoordinator) DescribeTopic(ctx context.Context, req *gen.DescribeTopicRequest) (*gen.DescribeTopicResponse, error) {
	params, _, err := n.topicMeta.GetTopic(ctx, req.TopicName)
	if err != nil {
		return nil, err
	}
	if params == nil {
		return &gen.DescribeTopicResponse{
			Found:     false,
			Error:     fmt.Sprintf("topic %q not found", req.TopicName),
			TopicName: req.TopicName,
		}, nil
	}

	n.membersMx.RLock()
	members := make(map[int64]NodeState, len(n.members))
	for id, st := range n.members {
		members[id] = st
	}
	n.membersMx.RUnlock()

	partitions := make([]*gen.PartitionInfo, 0, len(params.Partitions))
	for _, p := range params.Partitions {
		replicas := make([]*gen.ReplicaInfo, 0, len(p.ReplicasNodeId))
		for _, rid := range p.ReplicasNodeId {
			replicas = append(replicas, &gen.ReplicaInfo{
				NodeId:  rid,
				Address: members[rid].TcpAddress,
				InSync:  p.IsInSync(rid),
			})
		}
		partitions = append(partitions, &gen.PartitionInfo{
			PartitionId:   p.PartitionId,
			LeaderNodeId:  p.LeaderNodeId,
			LeaderAddress: members[p.LeaderNodeId].TcpAddress,
			Replicas:      replicas,
		})
	}

	return &gen.DescribeTopicResponse{
		Found:      true,
		TopicName:  params.Name,
		Partitions: partitions,
	}, nil
}

// CreateTopic реализует datatypes.IController — обслуживает
// ControlService.CreateTopic. В отличие от DescribeTopic, выполняется ТОЛЬКО
// на актуальном контроллере кластера (единственном, кому разрешено менять
// параметры топиков — см. datatypes.Controller). Если запрос попал на другую
// ноду, она честно отвечает not_controller=true с адресом контроллера, чтобы
// клиент мог повторить запрос в нужном месте, а не гадать.
//
// Алгоритм назначения реплик/лидеров (см. также computeNodeLoad):
//  1. Для каждой создаваемой партиции выбираем replication_factor нод с
//     НАИМЕНЬШИМ текущим числом уже назначенных им партиций по всему кластеру
//     (partitionCount) — это не даёт всем партициям осесть на одной ноде.
//  2. Среди выбранных для партиции нод лидером становится та, у которой
//     сейчас меньше всего партиций, где она уже лидер (leaderCount) — это не
//     даёт лидерским партициям скапливаться на одной ноде.
//  3. Счётчики partitionCount/leaderCount обновляются "на лету" после каждой
//     партиции внутри одного вызова CreateTopic, поэтому распределение
//     остаётся сбалансированным даже при создании сразу многих партиций.
//  4. Лидер новой (изначально пустой) партиции сразу считается in-sync сам с
//     собой; остальные назначенные реплики — обычные (не in-sync) и достигнут
//     ISR самостоятельно через существующий механизм watchTopicLoop ->
//     applyTopicParams -> startCatchUp (для пустой партиции это происходит
//     почти мгновенно, т.к. high watermark лидера равен 0).
func (n *NodeCoordinator) CreateTopic(ctx context.Context, req *gen.CreateTopicRequest) (*gen.CreateTopicResponse, error) {
	if n.broker.ReadRole() != datatypes.Controller {
		addr := n.findControllerAddress()
		return &gen.CreateTopicResponse{
			Success:           false,
			Error:             "this node is not the cluster controller",
			NotController:     true,
			ControllerAddress: addr,
		}, nil
	}

	if req.TopicName == "" {
		return &gen.CreateTopicResponse{Success: false, Error: "topic_name is required"}, nil
	}
	numPartitions := int(req.NumPartitions)
	if numPartitions <= 0 {
		return &gen.CreateTopicResponse{Success: false, Error: "num_partitions must be > 0"}, nil
	}
	rf := int(req.ReplicationFactor)
	if rf <= 0 {
		rf = 3 // по умолчанию: 2 реплики + 1 лидер
	}

	existing, _, err := n.topicMeta.GetTopic(ctx, req.TopicName)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return &gen.CreateTopicResponse{Success: false, Error: fmt.Sprintf("topic %q already exists", req.TopicName)}, nil
	}

	n.membersMx.RLock()
	aliveIDs := make([]int64, 0, len(n.members))
	for id := range n.members {
		aliveIDs = append(aliveIDs, id)
	}
	n.membersMx.RUnlock()

	if len(aliveIDs) < rf {
		return &gen.CreateTopicResponse{
			Success: false,
			Error:   fmt.Sprintf("not enough alive nodes (%d) for replication factor %d", len(aliveIDs), rf),
		}, nil
	}

	partitionCount, leaderCount, err := n.computeNodeLoad(ctx)
	if err != nil {
		return nil, err
	}

	partitions := make([]PartitionsParams, 0, numPartitions)
	for i := 0; i < numPartitions; i++ {
		// 1. Наименее загруженные ноды идут первыми (детерминированный
		// tie-break по id, чтобы результат был воспроизводим).
		sort.Slice(aliveIDs, func(a, b int) bool {
			la, lb := partitionCount[aliveIDs[a]], partitionCount[aliveIDs[b]]
			if la != lb {
				return la < lb
			}
			return aliveIDs[a] < aliveIDs[b]
		})
		chosen := append([]int64(nil), aliveIDs[:rf]...)

		// 2. Среди выбранных реплик лидер — та, у кого меньше всего leader-партиций.
		leader := chosen[0]
		for _, id := range chosen[1:] {
			if leaderCount[id] < leaderCount[leader] || (leaderCount[id] == leaderCount[leader] && id < leader) {
				leader = id
			}
		}

		// 3. Обновляем счётчики "на лету" — следующая партиция в этом же
		// вызове увидит уже актуальную нагрузку.
		for _, id := range chosen {
			partitionCount[id]++
		}
		leaderCount[leader]++

		partitions = append(partitions, PartitionsParams{
			PartitionId:    int64(i),
			LeaderNodeId:   leader,
			ReplicasNodeId: chosen,
			IsrNodeId:      []int64{leader}, // лидер пустой партиции сразу in-sync сам с собой
		})
	}

	params := TopicParams{
		Name:                     req.TopicName,
		DesiredReplicationFactor: rf,
		Partitions:               partitions,
	}

	ok, err := n.topicMeta.PutTopicCAS(ctx, params, 0) // expectedRev=0: ключ не должен существовать
	if err != nil {
		return nil, err
	}
	if !ok {
		return &gen.CreateTopicResponse{Success: false, Error: "race condition creating topic, please retry"}, nil
	}

	slog.Info("[Controller] Топик создан",
		slog.String("topic", req.TopicName), slog.Int("partitions", numPartitions), slog.Int("replicationFactor", rf))
	return &gen.CreateTopicResponse{Success: true}, nil
}

// findControllerAddress ищет среди известных нод ту, чья роль — Controller, и
// возвращает её control-plane адрес (для редиректа клиента при not_controller).
func (n *NodeCoordinator) findControllerAddress() string {
	n.membersMx.RLock()
	defer n.membersMx.RUnlock()
	for _, st := range n.members {
		if st.Role == datatypes.Controller {
			return st.ControlAddress
		}
	}
	return ""
}
