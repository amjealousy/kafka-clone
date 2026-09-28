package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	brokertypes "kafka-clone/server/datatypes/broker"
	"sort"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// clusterViewTTL — окно, в течение которого снимок кластера переиспользуется
	// без повторного обращения к etcd. Линеаризуемое чтение идёт через лидера
	// etcd (кворум на запрос), поэтому поллинг из админки без кэша превратился
	// бы в лишнюю нагрузку на кворум.
	clusterViewTTL = 300 * time.Millisecond
	// clusterViewTimeout ограничивает чтение: если нода потеряла связь с etcd,
	// ручка обязана упасть с ошибкой за предсказуемое время, а не висеть.
	clusterViewTimeout = 2 * time.Second
)

// clusterView — согласованный снимок control-plane состояния кластера,
// прочитанный из etcd одной транзакцией (то есть на одной ревизии).
type clusterView struct {
	nodes map[int64]NodeState
	// controller — действующий контроллер или nil, если он сейчас не избран.
	controller *NodeState
	epoch      int64
	topics     []TopicParams
}

// readClusterView возвращает снимок кластера, переиспользуя кэш не старше
// maxAge. maxAge == 0 форсирует свежее чтение — так обязаны делать все пути
// записи, для которых устаревшие на треть секунды данные недопустимы.
//
// При ошибке чтения кэш НЕ отдаётся: устаревший ответ неотличим от актуального,
// и именно так отвалившаяся от кластера нода начинает врать клиенту.
func (n *NodeCoordinator) readClusterView(ctx context.Context, maxAge time.Duration) (*clusterView, error) {
	n.viewMx.Lock()
	defer n.viewMx.Unlock()

	if maxAge > 0 && n.viewCache != nil && time.Since(n.viewFetchedAt) < maxAge {
		return n.viewCache, nil
	}

	view, err := n.fetchClusterView(ctx)
	if err != nil {
		return nil, err
	}

	n.viewCache = view
	n.viewFetchedAt = time.Now()
	return view, nil
}

// fetchClusterView читает /kafka/nodes/, /kafka/controller/state и /kafka/topic/
// одной транзакцией. Txn без условий If выполняет все Range атомарно на одной
// ревизии — именно это гарантирует, что LeaderNodeId любой партиции
// разрешается в ноду из того же ответа, и UI не увидит "лидера-призрака".
func (n *NodeCoordinator) fetchClusterView(ctx context.Context) (*clusterView, error) {
	ctx, cancel := context.WithTimeout(ctx, clusterViewTimeout)
	defer cancel()

	resp, err := n.cli.Txn(ctx).Then(
		clientv3.OpGet(NodesDiscoveryPath, clientv3.WithPrefix()),
		clientv3.OpGet(StatePath),
		clientv3.OpGet(TopicPath, clientv3.WithPrefix()),
	).Commit()
	if err != nil {
		return nil, fmt.Errorf("read cluster view from etcd: %w", err)
	}
	if len(resp.Responses) != 3 {
		return nil, fmt.Errorf("read cluster view from etcd: got %d range responses, want 3", len(resp.Responses))
	}

	return buildClusterView(
		resp.Responses[0].GetResponseRange().GetKvs(),
		resp.Responses[1].GetResponseRange().GetKvs(),
		resp.Responses[2].GetResponseRange().GetKvs(),
	), nil
}

// buildClusterView собирает снимок из сырых etcd-пар. Вынесено отдельно от
// сетевого вызова, чтобы логику разрешения контроллера можно было проверить
// тестами без поднятого etcd.
func buildClusterView(nodeKvs, stateKvs, topicKvs []*mvccpb.KeyValue) *clusterView {
	view := &clusterView{nodes: make(map[int64]NodeState, len(nodeKvs))}

	for _, kv := range nodeKvs {
		state, ok := decodeNodeState(kv)
		if !ok {
			continue
		}
		view.nodes[state.ID] = state
	}

	if len(stateKvs) > 0 {
		var state ControllerState
		if err := json.Unmarshal(stateKvs[0].Value, &state); err == nil {
			view.epoch = state.LeaderEpoch
			// /kafka/controller/state живёт БЕЗ аренды, поэтому сам по себе не
			// доказывает, что контроллер жив: после его смерти там остаётся
			// id покойника. Признаём контроллера только если его ключ с lease
			// присутствует в этом же снимке (та же проверка, что в electionLoop).
			if node, ok := view.nodes[state.LeaderId]; ok && state.LeaderId != 0 {
				controller := node
				view.controller = &controller
			}
		}
	}

	view.topics = make([]TopicParams, 0, len(topicKvs))
	for _, kv := range topicKvs {
		var params TopicParams
		if err := json.Unmarshal(kv.Value, &params); err != nil {
			continue
		}
		if params.Name == "" {
			params.Name = strings.TrimPrefix(string(kv.Key), TopicPath)
		}
		view.topics = append(view.topics, params)
	}
	sort.Slice(view.topics, func(a, b int) bool { return view.topics[a].Name < view.topics[b].Name })

	return view
}

// readMemberView — чтение снимка с обязательной самопроверкой членства.
//
// Нода может сохранять рабочее соединение с etcd для чтения и при этом уже быть
// исключённой из кластера: её аренда истекла (залипший keepalive, GC-пауза,
// заморозка контейнера), etcd удалил /kafka/nodes/<id>, контроллер переназначил
// её партиции. Данные она при этом отдаёт корректные, но сама рабочей точкой
// входа уже не является — клиент, закрепившийся за ней, будет получать отказы
// на produce/consume. Поэтому discovery-ручки на такой ноде обязаны падать,
// чтобы клиент ушёл на живого участника кластера.
//
// Проверка бесплатная: снимок уже прочитан.
func (n *NodeCoordinator) readMemberView(ctx context.Context, maxAge time.Duration) (*clusterView, error) {
	view, err := n.readClusterView(ctx, maxAge)
	if err != nil {
		return nil, err
	}
	if err := ensureMember(view, n.nodeID); err != nil {
		return nil, err
	}
	return view, nil
}

func ensureMember(view *clusterView, nodeID int64) error {
	if _, present := view.nodes[nodeID]; !present {
		return fmt.Errorf("%w: node %d", ErrNodeEvictedFromCluster, nodeID)
	}
	return nil
}

// GetClusterOverview реализует datatypes.IClusterDiscovery: отдаёт ноды,
// контроллера и топики одним согласованным снимком etcd.
func (n *NodeCoordinator) GetClusterOverview(ctx context.Context) (brokertypes.ClusterOverview, error) {
	view, err := n.readMemberView(ctx, clusterViewTTL)
	if err != nil {
		return brokertypes.ClusterOverview{}, err
	}
	return mapClusterOverview(view, n.nodeID), nil
}

// GetClusterNodeList отдаёт только состав кластера и действующего контроллера.
func (n *NodeCoordinator) GetClusterNodeList(ctx context.Context) (brokertypes.ClusterSnapshot, error) {
	view, err := n.readMemberView(ctx, clusterViewTTL)
	if err != nil {
		return brokertypes.ClusterSnapshot{}, err
	}
	return mapClusterSnapshot(view, n.nodeID), nil
}

// GetClusterTopicsList отдаёт топики с уже разрешёнными адресами лидеров.
func (n *NodeCoordinator) GetClusterTopicsList(ctx context.Context) ([]brokertypes.ClusterTopic, error) {
	view, err := n.readMemberView(ctx, clusterViewTTL)
	if err != nil {
		return nil, err
	}
	return mapClusterTopics(view), nil
}

// GetClusterTopic возвращает один топик из того же снимка.
func (n *NodeCoordinator) GetClusterTopic(ctx context.Context, name string) (brokertypes.ClusterTopic, bool, error) {
	view, err := n.readMemberView(ctx, clusterViewTTL)
	if err != nil {
		return brokertypes.ClusterTopic{}, false, err
	}
	for i := range view.topics {
		if view.topics[i].Name == name {
			return mapClusterTopic(view, view.topics[i]), true, nil
		}
	}
	return brokertypes.ClusterTopic{}, false, nil
}

// FindControllerAddress возвращает control-plane gRPC-адрес действующего
// контроллера.
//
// Раньше контроллер искался перебором поля Role в списке нод — это ненадёжно:
// Role публикуется в /kafka/nodes/<id> асинхронно (publishOwnRole), и во время
// failover в кластере видно либо ноль контроллеров, либо сразу два. Источник
// истины — /kafka/controller/state, плюс проверка, что аренда лидера жива.
//
// Пустая строка без ошибки = контроллер сейчас не избран (идут выборы).
func (n *NodeCoordinator) FindControllerAddress(ctx context.Context) (string, error) {
	view, err := n.readClusterView(ctx, clusterViewTTL)
	if err != nil {
		return "", err
	}
	if view.controller == nil {
		return "", nil
	}
	return view.controller.ControlAddress, nil
}

// GetNodeReadiness — ответ для /readyz. Ошибку наружу не возвращает: у пробы
// не должно быть неоднозначных состояний, любая проблема кодируется в Ready и
// Reason. Нода готова, только если она прочитала etcd И видит себя в составе
// кластера.
func (n *NodeCoordinator) GetNodeReadiness(ctx context.Context) brokertypes.NodeReadiness {
	readiness := brokertypes.NodeReadiness{NodeID: n.nodeID}

	view, err := n.readClusterView(ctx, clusterViewTTL)
	if err != nil {
		readiness.Reason = "etcd is unreachable: " + err.Error()
		return readiness
	}
	return buildNodeReadiness(view, n.nodeID)
}

func buildNodeReadiness(view *clusterView, nodeID int64) brokertypes.NodeReadiness {
	readiness := brokertypes.NodeReadiness{
		NodeID:       nodeID,
		ClusterSize:  len(view.nodes),
		ControllerID: controllerID(view),
	}
	_, readiness.InCluster = view.nodes[nodeID]
	readiness.IsController = readiness.ControllerID == nodeID && readiness.ControllerID != 0

	if !readiness.InCluster {
		readiness.Reason = "node is not registered in the cluster (lease expired)"
		return readiness
	}

	readiness.Ready = true
	return readiness
}
