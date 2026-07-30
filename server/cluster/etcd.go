package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-clone/server/broker"
	"kafka-clone/server/datatypes"
	"kafka-clone/server/topic"
	"log/slog"

	"sync"

	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	StatePath          = "/kafka/controller/state"
	NodesDiscoveryPath = "/kafka/nodes/"
	TopicPath          = "/kafka/topic/"
)

type ControllerState struct {
	LeaderId    int64 `json:"leader_id"`
	LeaderEpoch int64 `json:"leader_epoch"`
}
type TopicParams struct {
	Name string `json:"name"`
	// DesiredReplicationFactor — сколько всего реплик (включая лидера) должно быть
	// у каждой партиции. По условию бизнес-логики это 2+1 лидер = 3.
	DesiredReplicationFactor int                `json:"desired_replication_factor"`
	Partitions               []PartitionsParams `json:"partitions"`
}
type PartitionsParams struct {
	PartitionId  int64 `json:"partition_id"`
	LeaderNodeId int64 `json:"leader_node_id"`
	// ReplicasNodeId — полный набор нод, назначенных на партицию (включая лидера).
	// Нода из этого списка может принимать сообщения от лидера, но пока её нет в
	// IsrNodeId — она ещё не догнала лог и не является in-sync.
	ReplicasNodeId []int64 `json:"replicas_node_id"`
	// IsrNodeId — подмножество ReplicasNodeId, чьи локальные логи полностью
	// синхронизированы с лидером (in-sync replicas).
	IsrNodeId []int64 `json:"isr_node_id"`
}

// ContainsNode проверяет, назначена ли нода в набор реплик партиции.
func (p PartitionsParams) ContainsNode(nodeID int64) bool {
	for _, id := range p.ReplicasNodeId {
		if id == nodeID {
			return true
		}
	}
	return false
}

// IsInSync проверяет, входит ли нода в набор in-sync реплик партиции.
func (p PartitionsParams) IsInSync(nodeID int64) bool {
	for _, id := range p.IsrNodeId {
		if id == nodeID {
			return true
		}
	}
	return false
}

type NodeState struct {
	ID int64 `json:"id"`
	// Address — gRPC-адрес репликации (ReplicationService: AppendEntries/FetchLog).
	Address string `json:"address"`
	// TcpAddress — TCP-адрес ноды для produce/consume клиентов. Именно его
	// ControlService.DescribeTopic отдаёт клиенту как "лидер"/"in-sync реплика".
	TcpAddress string `json:"tcp_address"`
	// ControlAddress — gRPC-адрес control-plane API (ControlService:
	// DescribeTopic/CreateTopic), отдельный порт от репликации и TCP.
	ControlAddress string                `json:"control_address"`
	StartTime      string                `json:"start_time"`
	Role           datatypes.ClusterRole `json:"role"`
}
type NodeDiscovery struct {
	cli     *clientv3.Client
	mx      *sync.Mutex
	leaseId clientv3.LeaseID
	logger  *slog.Logger
}

func NewNodeDiscovery(client *clientv3.Client, logger *slog.Logger) *NodeDiscovery {
	return &NodeDiscovery{cli: client, mx: &sync.Mutex{}, logger: logger}
}
func (nd *NodeDiscovery) PutNodeState(ctx context.Context, state NodeState, onSessionLost func()) error {
	nodeKey := fmt.Sprintf("%s%d", NodesDiscoveryPath, state.ID)

	// 1. Создаем аренду
	leaseResp, err := nd.cli.Grant(ctx, 6)
	if err != nil {
		return err
	}
	localLeaseID := leaseResp.ID

	// Сохраняем LeaseID внутрь структуры для разделяемого доступа
	nd.mx.Lock()
	nd.leaseId = localLeaseID
	nd.mx.Unlock()

	// 2. Сериализуем структуру NodeState в JSON
	valBytes, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// 3. Записываем в etcd
	_, err = nd.cli.Put(ctx, nodeKey, string(valBytes), clientv3.WithLease(localLeaseID))
	if err != nil {
		return err
	}

	// 4. Запускаем KeepAlive
	keepAliveChan, err := nd.cli.KeepAlive(ctx, localLeaseID)
	if err != nil {
		return err
	}

	// Фоновый процесс отслеживания жизни сессии
	go func() {
		for {
			select {
			case <-ctx.Done():
				nd.logger.Error("[Discovery] Контекст отменен. Отзываем аренду", slog.String("nodeKey", nodeKey))
				_, _ = nd.cli.Revoke(context.Background(), localLeaseID)
				return
			case msg := <-keepAliveChan:
				if msg == nil {
					nd.logger.Warn("[Discovery] Сессия etcd потеряна!", slog.String("nodeKey", nodeKey))

					// Сбрасываем leaseId, так как эта аренда на стороне etcd больше не существует
					nd.mx.Lock()
					nd.leaseId = 0
					nd.mx.Unlock()

					// Вызываем fallback-инструкцию координатора
					if onSessionLost != nil {
						onSessionLost()
					}
					return
				}
			}
		}
	}()

	return nil
}
func (nd *NodeDiscovery) GetLeaseID() clientv3.LeaseID {
	nd.mx.Lock()
	defer nd.mx.Unlock()
	return nd.leaseId
}

// Start запускает выборы и отслеживание метаданных. grpcAddr — адрес
// репликации (ReplicationService), tcpAddr — адрес для produce/consume
// клиентов, controlAddr — адрес административного ControlService.
func (n *NodeCoordinator) Start(grpcAddr, tcpAddr, controlAddr string) {
	// Привязываем внутренний глобальный контекст к контексту верхнего уровня (например, из main.go)
	n.address = grpcAddr
	n.tcpAddress = tcpAddr
	n.controlAddress = controlAddr

	n.discovery = NewNodeDiscovery(n.cli, n.logger)
	state := NodeState{
		ID:             n.nodeID,
		Address:        grpcAddr,
		TcpAddress:     tcpAddr,
		ControlAddress: controlAddr,
		StartTime:      time.Now().String(),
		Role:           datatypes.Unroled, // Начинаем строго как UNROLED
	}

	// Регистрация привязана к глобальному контексту ноды
	err := n.discovery.PutNodeState(n.ctx, state, n.handleSessionLoss)
	if err != nil {
		n.logger.Error("Первоначальная регистрация провалена", slog.String("err", err.Error()))
		return
	}

	// Все системные вотчеры работают на глобальном контексте ноды
	go n.discovery.WatchNodes(n.ctx, n.onNodeJoin, n.onNodeLeave)
	go n.watchMetadataLoop(n.ctx)
	go n.watchTopicLoop(n.ctx)
	go n.runControllerReconcileLoop(n.ctx)
	go n.runLocalStateReportLoop(n.ctx)

	// electionCtx уже создан в NewNodeCoordinator и живёт до потери etcd-сессии
	// или остановки ноды. Без запуска этого цикла все ноды навсегда остаются
	// Unroled, а /kafka/controller/state не создаётся.
	go n.electionLoop(n.electionCtx)
}

// publishOwnRole обновляет роль текущей ноды, сохраняя lease регистрации.
// Роль брокера в памяти и роль в /kafka/nodes/<id> должны изменяться вместе.
func (n *NodeCoordinator) publishOwnRole(ctx context.Context, role datatypes.ClusterRole) {
	nodeKey := fmt.Sprintf("%s%d", NodesDiscoveryPath, n.nodeID)
	resp, err := n.cli.Get(ctx, nodeKey)
	if err != nil || len(resp.Kvs) == 0 {
		if err != nil {
			n.logger.Error("failed to read node state for role update", "role", role, "error", err)
		}
		return
	}

	kv := resp.Kvs[0]
	var state NodeState
	if err := json.Unmarshal(kv.Value, &state); err != nil {
		n.logger.Error("failed to decode node state for role update", "role", role, "error", err)
		return
	}
	if state.Role == role {
		return
	}

	state.Role = role
	value, err := json.Marshal(state)
	if err != nil {
		n.logger.Error("failed to encode node state for role update", "role", role, "error", err)
		return
	}

	txnResp, err := n.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(nodeKey), "=", kv.ModRevision)).
		Then(clientv3.OpPut(nodeKey, string(value), clientv3.WithLease(clientv3.LeaseID(kv.Lease)))).
		Commit()
	if err != nil {
		n.logger.Error("failed to publish node role", "role", role, "error", err)
	} else if !txnResp.Succeeded {
		n.logger.Warn("node state changed concurrently; role will be retried by election loop", "role", role)
	}
}

func (n *NodeCoordinator) handleSessionLoss() {
	n.logger.Warn("[Coordinator] Обнаружена потеря сессии etcd! Запускаем аварийный режим.")

	// ОСТАНАВЛИВАЕМ ВЫБОРЫ: Нода больше не имеет права претендовать на лидерство
	n.mu.Lock()
	if n.cancelElection != nil {
		n.cancelElection()
	}
	n.mu.Unlock()

	// Ставим брокер на паузу (запрещаем обработку сообщений)
	resume := make(chan bool)
	go n.ClusterPause(resume)

	// Запускаем восстановление подключения и повторную синхронизацию данных
	go n.recoveryLoop(resume)
}

func (n *NodeCoordinator) recoveryLoop(resume chan bool) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done(): // Если приложение закрылось глобально — выходим
			return
		case <-ticker.C:
			n.logger.Info("[Coordinator] Попытка восстановления сессии...")

			initialState := NodeState{
				ID:             n.nodeID,
				Address:        n.address,
				TcpAddress:     n.tcpAddress,
				ControlAddress: n.controlAddress,
				StartTime:      time.Now().Format(time.RFC3339),
				Role:           datatypes.Unroled, // При восстановлении роль снова сбрасывается
			}

			// Пытаемся перерегистрироваться в etcd под защитой глобального n.ctx
			err := n.discovery.PutNodeState(n.ctx, initialState, n.handleSessionLoss)
			if err == nil {
				n.logger.Info("[Coordinator] Сессия восстановлена. Догоняем упущенные офсеты...")

				// Выкачиваем данные, накопленные кластером за время нашего сбоя
				if err := n.broker.RestoreData(n.ctx, int64(n.GetLeaderId())); err != nil {
					slog.Error("[Coordinator] Ошибка догоняющей синхронизации лога", slog.String("err", err.Error()))
					continue // Не выходим из recovery, пока лог не будет чист
				}

				n.logger.Info("[Coordinator] Лог актуализирован. Создаем новый чистый electionCtx.")

				n.mu.Lock()
				// Создаем совершенно новый, чистый контекст выборов от глобального n.ctx
				n.electionCtx, n.cancelElection = context.WithCancel(n.ctx)
				// Перезапускаем выборы с новым контекстом
				go n.electionLoop(n.electionCtx)
				n.mu.Unlock()

				resume <- true
				return
			}

			n.logger.Error("[Coordinator] Не удалось восстановить сессию", slog.String("err", err.Error()))
		}
	}
}

func (n *NodeCoordinator) electionLoop(ctx context.Context) {
	n.logger.Debug("Start election loop")
	stateKey := StatePath
	for {
		// Проверяем отмену контекста (сработает как при cancelElection(), так и при общем n.cancel())
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Используем переданный ctx во ВСЕХ операциях с etcd
		resp, err := n.cli.Get(ctx, StatePath)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		var currentController int64 = 0
		var currentEpoch int64 = 0
		var modRev int64 = 0

		if len(resp.Kvs) > 0 {
			var state ControllerState
			if err := json.Unmarshal(resp.Kvs[0].Value, &state); err == nil {
				currentController = state.LeaderId
				currentEpoch = state.LeaderEpoch
				modRev = resp.Kvs[0].ModRevision
			}
			n.logger.Debug("Текущий статус ControllerState", slog.Any("state", state))
		}

		n.logger.Debug("Текущий Controller", slog.Any("currentController", currentController))
		if currentController == n.nodeID {
			n.SwitchToControllerMode(currentEpoch)
			<-ctx.Done() // Удерживаем лидера до отмены ЭТОГО контекста сессии
			return
		}

		leaderAlive := false
		if currentController != 0 {
			nodeKey := fmt.Sprintf("%s%d", NodesDiscoveryPath, currentController)
			nodeResp, err := n.cli.Get(ctx, nodeKey)
			if err == nil && len(nodeResp.Kvs) > 0 {
				leaderAlive = true
			}
		}

		if leaderAlive {
			n.SwitchToFollowerMode(currentController, currentEpoch)

			// Вместо создания нового контекста с таймаутом, используем встроенный селект на базе ctx
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				// Спокойно идем на следующую итерацию проверки лидера
			}
			continue
		}
		n.logger.Debug("Нода пытается стать Controller")
		// Попытка CAS-захвата лидерства (подготовка структуры ноды)
		myNodeKey := fmt.Sprintf("%s%d", NodesDiscoveryPath, n.nodeID)
		myNodeResp, err := n.cli.Get(ctx, myNodeKey)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
			continue
		}

		var myNodeModRev int64 = 0
		var myNodeState NodeState
		if len(myNodeResp.Kvs) > 0 {
			_ = json.Unmarshal(myNodeResp.Kvs[0].Value, &myNodeState)
			myNodeModRev = myNodeResp.Kvs[0].ModRevision
		}
		myNodeState.Role = datatypes.Controller
		newMyNodeState, _ := json.Marshal(myNodeState)

		var cmpNode clientv3.Cmp
		if myNodeModRev == 0 {
			cmpNode = clientv3.Compare(clientv3.Version(myNodeKey), "=", 0)
		} else {
			cmpNode = clientv3.Compare(clientv3.ModRevision(myNodeKey), "=", myNodeModRev)
		}

		var cmp clientv3.Cmp
		if modRev == 0 {
			cmp = clientv3.Compare(clientv3.Version(StatePath), "=", 0)
		} else {
			cmp = clientv3.Compare(clientv3.ModRevision(StatePath), "=", modRev)
		}

		nextEpoch := currentEpoch + 1
		newState := ControllerState{LeaderId: n.nodeID, LeaderEpoch: nextEpoch}
		newValue, _ := json.Marshal(newState)

		n.discovery.mx.Lock()
		leaseID := n.discovery.leaseId
		n.discovery.mx.Unlock()

		txn := n.cli.Txn(ctx).
			If(cmp, cmpNode).
			Then(
				clientv3.OpPut(stateKey, string(newValue)),
				clientv3.OpPut(myNodeKey, string(newMyNodeState), clientv3.WithLease(leaseID)),
			).
			Else()

		txnResp, err := txn.Commit()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
			continue
		}

		if txnResp.Succeeded {
			n.SwitchToControllerMode(nextEpoch)
			<-ctx.Done() // Лидер активен, пока жива сессия etcd или работает приложение
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (n *NodeCoordinator) watchMetadataLoop(ctx context.Context) {

	watchChan := n.cli.Watch(ctx, StatePath)
	slog.Info("[Watcher] Запущен мониторинг глобального состояния контроллера кластера")

	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			if event.Type == clientv3.EventTypePut {
				var state ControllerState
				if err := json.Unmarshal(event.Kv.Value, &state); err != nil {
					slog.Error("[Watcher] Не удалось распарсить ControllerState", slog.String("err", err.Error()))
					continue
				}

				// Если в etcd записан лидер и это НЕ наш ID -> инициализируем/обновляем режим фолловера
				if state.LeaderId != n.nodeID {
					n.SwitchToFollowerMode(state.LeaderId, state.LeaderEpoch)
				}
			}
		}
	}
}

// WatchNodes отслеживает подключение/отключение нод кластера. onJoin/onLeave —
// колбэки, вызываемые при появлении и исчезновении ноды соответственно.
func (nd *NodeDiscovery) WatchNodes(ctx context.Context, onJoin func(NodeState), onLeave func(NodeState)) {
	slog.Info("[Discovery] Запуск отслеживания активных нод кластера...")

	// ШАГ А: Запрашиваем текущее состояние (Bootstrap)
	getResp, err := nd.cli.Get(ctx, NodesDiscoveryPath, clientv3.WithPrefix())
	if err != nil {
		slog.Error(fmt.Sprintf("Не удалось получить список начальных нод: %v", err))
	}

	slog.Info("=== ТЕКУЩИЕ АКТИВНЫЕ НОДЫ В КЛАСТЕРЕ ===")
	for _, kv := range getResp.Kvs {
		var info NodeState
		if err := json.Unmarshal(kv.Value, &info); err == nil {
			slog.Info(fmt.Sprintf("Node ID: %d, Адрес: %s (Запущена в: %s)\n", info.ID, info.Address, info.StartTime))
			if onJoin != nil {
				onJoin(info)
			}
		}
	}
	slog.Info("========================================")

	// ШАГ Б: Запускаем Watch с точки остановки Get
	watchChan := nd.cli.Watch(ctx, NodesDiscoveryPath,
		clientv3.WithPrefix(),
		clientv3.WithPrevKV(),
		clientv3.WithRev(getResp.Header.Revision+1),
	)

	for watchResp := range watchChan {
		if watchResp.Canceled {
			slog.Error(fmt.Sprintf("[Discovery] Наблюдение за нодами отменено: %v", watchResp.Err()))
			return
		}

		for _, ev := range watchResp.Events {
			fullKey := string(ev.Kv.Key)
			nodeIDStr := strings.TrimPrefix(fullKey, NodesDiscoveryPath)

			switch ev.Type {
			case clientv3.EventTypePut:
				var info NodeState
				if err := json.Unmarshal(ev.Kv.Value, &info); err != nil {
					continue
				}
				if ev.Kv.Version == 1 {
					slog.Error(fmt.Sprintf("[КЛАСТЕР] >>> Нода ПОДКЛЮЧИЛАСЬ: ID=%s, Адрес=%s\n", nodeIDStr, info.Address))
				} else {
					slog.Error(fmt.Sprintf("[КЛАСТЕР] ↺ Нода ОБНОВИЛА данные: ID=%s, Новый адрес=%s\n", nodeIDStr, info.Address))
				}
				if onJoin != nil {
					onJoin(info)
				}

			case clientv3.EventTypeDelete:
				slog.Error(fmt.Sprintf("[КЛАСТЕР] <<< Нода ОТКЛЮЧИЛАСЬ или УПАЛА: ID=%s\n", nodeIDStr))
				if ev.PrevKv != nil {
					var oldInfo NodeState
					if err := json.Unmarshal(ev.PrevKv.Value, &oldInfo); err == nil {
						slog.Error(fmt.Sprintf("Последний известный адрес ноды %d был: %s\n", oldInfo.ID, oldInfo.Address))
						if onLeave != nil {
							onLeave(oldInfo)
						}
					}
				}
			}
		}
	}
}
func (n *NodeCoordinator) watchTopicLoop(ctx context.Context) {
	slog.Info("[TopicWatcher] Запуск отслеживания назначений топиков...")

	// ШАГ А: Bootstrap — применяем уже существующие в etcd назначения,
	// чтобы нода, стартовавшая позже, подхватила свои партиции.
	getResp, err := n.cli.Get(ctx, TopicPath, clientv3.WithPrefix())
	if err != nil {
		slog.Error("[TopicWatcher] Не удалось получить начальный список топиков", slog.String("err", err.Error()))
	} else {
		for _, kv := range getResp.Kvs {
			topicName := strings.TrimPrefix(string(kv.Key), TopicPath)
			var params TopicParams
			if err := json.Unmarshal(kv.Value, &params); err == nil {
				params.Name = topicName
				n.applyTopicParams(ctx, params)
			}
		}
	}

	// ШАГ Б: Watch с точки остановки Get, чтобы не потерять события.
	startRev := int64(1)
	if getResp != nil {
		startRev = getResp.Header.Revision + 1
	}
	watchChan := n.cli.Watch(ctx, TopicPath,
		clientv3.WithPrefix(),
		clientv3.WithPrevKV(),
		clientv3.WithRev(startRev),
	)

	for watchResp := range watchChan {
		if watchResp.Canceled {
			slog.Warn("[TopicWatcher] Watch отменён", slog.String("err", watchResp.Err().Error()))
			return
		}

		for _, ev := range watchResp.Events {
			topicName := strings.TrimPrefix(string(ev.Kv.Key), TopicPath)

			switch ev.Type {
			case clientv3.EventTypePut:
				var params TopicParams
				if err := json.Unmarshal(ev.Kv.Value, &params); err != nil {
					slog.Error("[TopicWatcher] Ошибка парсинга TopicParams", slog.String("topic", topicName), slog.String("err", err.Error()))
					continue
				}
				params.Name = topicName
				slog.Info("[TopicWatcher] Изменение топика", slog.String("topic", topicName), slog.Int64("version", ev.Kv.Version))
				n.applyTopicParams(ctx, params)

			case clientv3.EventTypeDelete:
				slog.Info("[TopicWatcher] Топик удалён", slog.String("topic", topicName))
				//todo
			}
		}
	}
}

// applyTopicParams реагирует на конфигурацию топика из etcd: если данная нода
// назначена репликой на одну из партиций, она создаёт топик/партицию локально
// и, если ещё не является in-sync, запускает восстановление лога (catch-up).
func (n *NodeCoordinator) applyTopicParams(ctx context.Context, params TopicParams) {
	for _, part := range params.Partitions {
		// Запоминаем лидера партиции (нужно и лидеру, и репликам для FetchLog).
		n.broker.SetPartitionLeader(params.Name, int(part.PartitionId), int(part.LeaderNodeId))

		if !part.ContainsNode(n.nodeID) {
			continue // Эта нода не участвует в данной партиции
		}

		// Строим локальное описание партиции с полным набором реплик.
		replicas := make([]topic.Replica, 0, len(part.ReplicasNodeId))
		for _, rid := range part.ReplicasNodeId {
			replicas = append(replicas, topic.Replica{
				Id:     int(rid),
				InSync: part.IsInSync(rid),
			})
		}
		localPart := &topic.Partition{
			Id:          int(part.PartitionId),
			Replicas:    replicas,
			Retention:   time.Hour,
			StartOffset: 0,
		}

		pp, created, err := n.broker.EnsureLocalPartition(ctx, params.Name, localPart)
		if err != nil {
			slog.Error("[TopicWatcher] Не удалось создать локальную партицию", slog.String("topic", params.Name), slog.String("err", err.Error()))
			continue
		}
		if created {
			slog.Info("[TopicWatcher] Локально создана партиция по назначению из etcd",
				slog.String("topic", params.Name), slog.Int64("partition", part.PartitionId))
		}

		// Партиция могла существовать и ДО этого события (например, была
		// восстановлена из манифеста MongoDB при старте) — в этом случае
		// EnsurePartition не пересоздаёт её и initialState конструктора не
		// применяется. Поэтому набор реплик всегда обновляем явно, чтобы
		// лидер видел актуальный ISR для расчёта кворума в ReplicateAndAppend.
		pp.SetReplicas(replicas)

		// Если мы лидер этой партиции — мы по определению уже "в синхроне"
		// сами с собой, восстанавливать нечего.
		if part.LeaderNodeId == n.nodeID {
			pp.ForceInSync()
			continue
		}

		// Если etcd уже считает нас in-sync (например, короткая потеря сессии,
		// локальный лог не успел отстать) — не гоняем catch-up заново, но всё
		// равно локально фиксируем состояние.
		if part.IsInSync(n.nodeID) {
			pp.ForceInSync()
			n.broker.MarkPartitionReplicaInSync(params.Name, int(part.PartitionId), int(n.nodeID))
			continue
		}

		// Иначе мы "просто реплика": пока восстанавливаем лог, PUSH от лидера
		// отклоняется на уровне PartitionProcessor (см. TryApplyPush), поэтому
		// сообщения будут получены исключительно через FetchLog catch-up.
		if pp.SyncState() != broker.StateInSync {
			n.startCatchUp(ctx, params.Name, int(part.PartitionId), int(part.LeaderNodeId))
		}
	}
}

// startCatchUp единожды (на партицию) запускает фоновое восстановление лога.
func (n *NodeCoordinator) startCatchUp(ctx context.Context, topicName string, partitionID int, leaderNodeID int) {
	key := fmt.Sprintf("%s/%d", topicName, partitionID)

	n.syncingMx.Lock()
	if n.syncing[key] {
		n.syncingMx.Unlock()
		return // Уже синхронизируется
	}
	n.syncing[key] = true
	n.syncingMx.Unlock()

	go func() {
		defer func() {
			n.syncingMx.Lock()
			delete(n.syncing, key)
			n.syncingMx.Unlock()
		}()

		slog.Info("[CatchUp] Начинаем восстановление лога реплики",
			slog.String("topic", topicName), slog.Int("partition", partitionID), slog.Int("leader", leaderNodeID))

		// CatchUpPartition сама атомарно переводит партицию в StateInSync
		// (через TryFinalizeInSync) в тот момент, когда локальный nextOffset
		// догоняет HW лидера — см. подробный комментарий в broker.CatchUpPartition.
		// Поэтому к моменту успешного возврата этой функции партиция УЖЕ
		// принимает обычные push-сообщения; всё, что ниже — лишь публикация
		// этого факта наружу (в topic.Replica-список и в etcd).
		if err := n.broker.CatchUpPartition(ctx, topicName, partitionID, leaderNodeID); err != nil {
			slog.Error("[CatchUp] Ошибка восстановления лога",
				slog.String("topic", topicName), slog.Int("partition", partitionID), slog.String("err", err.Error()))
			return
		}

		// Лог восстановлен — локально помечаем реплику in-sync...
		n.broker.MarkPartitionReplicaInSync(topicName, partitionID, int(n.nodeID))

		// ...и публикуем факт вступления в ISR в etcd, чтобы это увидел весь кластер.
		if err := n.topicMeta.MarkNodeInSync(ctx, topicName, int64(partitionID), n.nodeID); err != nil {
			slog.Error("[CatchUp] Не удалось записать ISR в etcd",
				slog.String("topic", topicName), slog.Int("partition", partitionID), slog.String("err", err.Error()))
			return
		}

		slog.Info("[CatchUp] Реплика стала IN-SYNC",
			slog.String("topic", topicName), slog.Int("partition", partitionID))
	}()
}
