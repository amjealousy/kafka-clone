package cluster

import (
	"context"
	"errors"
	"fmt"
	brokertypes "kafka-clone/server/datatypes/broker"
	"log/slog"
	"sort"

	"kafka-clone/server/datatypes"
)

// Псевдонимы доменных ошибок control-plane: сами значения объявлены в пакете
// datatypes, чтобы транспортные слои могли их распознавать без импорта cluster.
var (
	TopicNotFoundError              = datatypes.ErrTopicNotFound
	TopicCredentialError            = datatypes.ErrTopicCredential
	TopicAlreadyExistsError         = datatypes.ErrTopicAlreadyExists
	ThisIsNotClusterControllerError = datatypes.ErrNotClusterController
	ThereIsNotEnoughAliveNodesError = datatypes.ErrNotEnoughAliveNodes
	RaceConditionError              = datatypes.ErrRaceCondition
)

// DescribeTopic реализует datatypes.ITopicManager. Может быть вызван на ЛЮБОЙ
// ноде кластера: и параметры топика, и адреса нод читаются из etcd одним
// согласованным снимком (см. cluster_view.go), поэтому обращаться именно к
// контроллеру для чтения метаданных не требуется.
//
// Раньше адреса брались из watch-кэша n.members. Это давало правдоподобный
// ответ даже когда нода уже отвалилась от кластера; теперь такая нода честно
// вернёт ошибку чтения etcd.
func (n *NodeCoordinator) DescribeTopic(ctx context.Context, req datatypes.DescribeTopicRequest) (datatypes.DescribeTopicResponse, error) {
	view, err := n.readClusterView(ctx, clusterViewTTL)
	if err != nil {
		return datatypes.DescribeTopicResponse{}, err
	}

	var params *TopicParams
	for i := range view.topics {
		if view.topics[i].Name == req.TopicName {
			params = &view.topics[i]
			break
		}
	}
	if params == nil {
		return datatypes.DescribeTopicResponse{
			Found:     false,
			Error:     TopicNotFoundError,
			TopicName: req.TopicName,
		}, nil
	}

	partitions := make([]datatypes.PartitionInfo, 0, len(params.Partitions))
	for _, p := range params.Partitions {
		replicas := make([]datatypes.ReplicaInfo, 0, len(p.ReplicasNodeId))
		for _, rid := range p.ReplicasNodeId {
			replicas = append(replicas, datatypes.ReplicaInfo{
				NodeID:  rid,
				Address: view.nodes[rid].TcpAddress,
				InSync:  p.IsInSync(rid),
			})
		}
		partitions = append(partitions, datatypes.PartitionInfo{
			PartitionID:   p.PartitionId,
			LeaderNodeID:  p.LeaderNodeId,
			LeaderAddress: view.nodes[p.LeaderNodeId].TcpAddress,
			Replicas:      replicas,
		})
	}

	return datatypes.DescribeTopicResponse{
		Found:      true,
		TopicName:  params.Name,
		Partitions: partitions,
	}, nil
}

// CreateTopic реализует datatypes.ITopicManager. В отличие от DescribeTopic,
// выполняется ТОЛЬКО
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
func (n *NodeCoordinator) CreateTopic(ctx context.Context, req datatypes.CreateTopicRequest) (datatypes.CreateTopicResponse, error) {
	if n.broker.ReadRole() != brokertypes.Controller {
		// Адрес контроллера — best-effort подсказка клиенту: если etcd
		// недоступен, отдаём пустую строку, но саму причину отказа не теряем.
		addr, addrErr := n.FindControllerAddress(ctx)
		if addrErr != nil {
			n.logger.Warn("не удалось определить адрес контроллера", slog.String("err", addrErr.Error()))
		}
		return datatypes.CreateTopicResponse{
			Success:           false,
			Error:             ThisIsNotClusterControllerError,
			NotController:     true,
			ControllerAddress: addr,
		}, nil
	}

	if req.TopicName == "" {
		return datatypes.CreateTopicResponse{Success: false, Error: TopicCredentialError}, nil
	}
	numPartitions := req.NumPartitions
	if numPartitions <= 0 {
		return datatypes.CreateTopicResponse{Success: false, Error: TopicCredentialError}, nil
	}
	rf := req.ReplicationFactor
	if rf <= 0 {
		rf = 3 // по умолчанию: 2 реплики + 1 лидер
	}

	existing, _, err := n.topicMeta.GetTopic(ctx, req.TopicName)
	if err != nil {
		return datatypes.CreateTopicResponse{}, err
	}
	if existing != nil {
		return datatypes.CreateTopicResponse{Success: false, Error: TopicAlreadyExistsError}, nil
	}

	// Живые ноды берём свежим чтением etcd (maxAge=0), а не из watch-кэша:
	// назначение реплик — это запись, и промахнуться мимо уже умершей ноды
	// здесь дороже, чем сходить в etcd лишний раз.
	view, err := n.readClusterView(ctx, 0)
	if err != nil {
		return datatypes.CreateTopicResponse{}, err
	}
	aliveIDs := make([]int64, 0, len(view.nodes))
	for id := range view.nodes {
		aliveIDs = append(aliveIDs, id)
	}

	if len(aliveIDs) < rf {
		return datatypes.CreateTopicResponse{
			Success: false,
			Error:   errors.Join(fmt.Errorf("not enough alive nodes (%d) for replication factor %d", len(aliveIDs), rf), ThereIsNotEnoughAliveNodesError),
		}, nil
	}

	// Нагрузку считаем по топикам из ТОГО ЖЕ снимка, что и список живых нод:
	// и лишнего чтения etcd нет, и балансировка не может опираться на состав
	// нод одной ревизии, а на раскладку партиций другой.
	partitionCount, leaderCount := computeNodeLoadFrom(view.topics)

	partitions := make([]PartitionsParams, 0, numPartitions)
	for i := range numPartitions {
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
		return datatypes.CreateTopicResponse{}, err
	}
	if !ok {
		return datatypes.CreateTopicResponse{Success: false, Error: RaceConditionError}, nil
	}
	n.invalidateClusterView()
	slog.Info("[Controller] Топик создан",
		slog.String("topic", req.TopicName), slog.Int("partitions", numPartitions), slog.Int("replicationFactor", rf))
	return datatypes.CreateTopicResponse{Success: true}, nil
}

// Чтение топологии кластера (GetClusterNodeList, GetClusterTopicsList,
// FindControllerAddress) живёт в cluster_view.go — оно идёт напрямую в etcd.
