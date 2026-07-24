package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"kafka-clone/server/datatypes"
	"log"
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
	Partitions        int               `json:"partitions"`
	ReplicationFactor int               `json:"replication_factor"`
	Config            map[string]string `json:"config"`
	LeaderNodeId      int64             `json:"leader_node_id"`
	ReplicasNodeId    []int64           `json:"replicas_node_id"`
}
type NodeState struct {
	ID        int64                 `json:"id"`
	Address   string                `json:"address"` // например, "192.168.1.50:9092"
	StartTime string                `json:"start_time"`
	Role      datatypes.ClusterRole `json:"role"`
}
type NodeDiscovery struct {
	cli     *clientv3.Client
	mx      *sync.Mutex
	leaseId clientv3.LeaseID
}

func NewNodeDiscovery(client *clientv3.Client) *NodeDiscovery {
	return &NodeDiscovery{cli: client, mx: &sync.Mutex{}}
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
				slog.Error("[Discovery] Контекст отменен. Отзываем аренду", slog.String("nodeKey", nodeKey))
				_, _ = nd.cli.Revoke(context.Background(), localLeaseID)
				return
			case msg := <-keepAliveChan:
				if msg == nil {
					slog.Warn("[Discovery] Сессия etcd потеряна!", slog.String("nodeKey", nodeKey))

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

// Start запускает выборы и отслеживание метаданных
func (n *NodeCoordinator) Start(address string) {
	// Привязываем внутренний глобальный контекст к контексту верхнего уровня (например, из main.go)

	n.discovery = NewNodeDiscovery(n.cli)
	state := NodeState{
		ID:        n.nodeID,
		Address:   address,
		StartTime: time.Now().String(),
		Role:      datatypes.Unroled, // Начинаем строго как UNROLED
	}

	// Регистрация привязана к глобальному контексту ноды
	err := n.discovery.PutNodeState(n.ctx, state, n.handleSessionLoss)
	if err != nil {
		slog.Error("Первоначальная регистрация провалена", slog.String("err", err.Error()))
		return
	}

	// Все системные вотчеры работают на глобальном контексте ноды
	go n.discovery.WatchNodes(n.ctx)
	go n.watchMetadataLoop(n.ctx)
	go n.watchTopicLoop(n.ctx)

	// Запускаем последовательность бутстрепа (без передачи параметров, она сама возьмет n.ctx)
	go n.bootstrapAndSyncSequence()
}

func (n *NodeCoordinator) handleSessionLoss() {
	slog.Warn("[Coordinator] Обнаружена потеря сессии etcd! Запускаем аварийный режим.")

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
			slog.Info("[Coordinator] Попытка восстановления сессии...")

			initialState := NodeState{
				ID:        n.nodeID,
				Address:   "192.168.1.X:9092",
				StartTime: time.Now().Format(time.RFC3339),
				Role:      datatypes.Unroled, // При восстановлении роль снова сбрасывается
			}

			// Пытаемся перерегистрироваться в etcd под защитой глобального n.ctx
			err := n.discovery.PutNodeState(n.ctx, initialState, n.handleSessionLoss)
			if err == nil {
				slog.Info("[Coordinator] Сессия восстановлена. Догоняем упущенные офсеты...")

				// Выкачиваем данные, накопленные кластером за время нашего сбоя
				if err := n.broker.RestoreData(n.ctx, int64(n.GetLeaderId())); err != nil {
					slog.Error("[Coordinator] Ошибка догоняющей синхронизации лога", slog.String("err", err.Error()))
					continue // Не выходим из recovery, пока лог не будет чист
				}

				slog.Info("[Coordinator] Лог актуализирован. Создаем новый чистый electionCtx.")

				n.mu.Lock()
				// Создаем совершенно новый, чистый контекст выборов от глобального n.ctx
				n.electionCtx, n.cancelElection = context.WithCancel(n.ctx)
				// Перезапускаем выборы с новым контекстом
				go n.electionLoop(n.electionCtx)
				n.mu.Unlock()

				resume <- true
				return
			}

			slog.Error("[Coordinator] Не удалось восстановить сессию", slog.String("err", err.Error()))
		}
	}
}

func (n *NodeCoordinator) incrementEpochAndClaimLeadership(leaseID clientv3.LeaseID) (int64, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stateKey := StatePath

	// 1. Читаем текущее глобальное состояние лидера из etcd
	resp, err := n.cli.Get(ctx, stateKey)
	if err != nil {
		return 0, err
	}

	var currentEpoch int64 = 0
	var modRev int64 = 0

	if len(resp.Kvs) > 0 {
		var state ControllerState
		_ = json.Unmarshal(resp.Kvs[0].Value, &state)
		currentEpoch = state.LeaderEpoch
		modRev = resp.Kvs[0].ModRevision
	}

	nextEpoch := currentEpoch + 1
	newState := ControllerState{
		LeaderId:    n.nodeID,
		LeaderEpoch: nextEpoch,
	}
	newValue, _ := json.Marshal(newState)

	// Настраиваем CAS-проверку для глобального состояния
	var cmp clientv3.Cmp
	if modRev == 0 {
		cmp = clientv3.Compare(clientv3.Version(stateKey), "=", 0)
	} else {
		cmp = clientv3.Compare(clientv3.ModRevision(stateKey), "=", modRev)
	}

	// 2. Читаем текущее состояние нашей собственной ноды
	// Примечание: если n.nodeID строка, используй %s вместо %d
	nodeKey := fmt.Sprintf("%s%s", NodesDiscoveryPath, n.nodeID)
	currNodestate, err := n.cli.Get(ctx, nodeKey)
	if err != nil {
		slog.Info("couldn't get nodestate", slog.Any("node", n.nodeID), slog.String("err", err.Error()))
		return 0, err
	}

	var (
		cmp1      clientv3.Cmp
		modRev1   int64
		nodestate NodeState
	)

	if len(currNodestate.Kvs) > 0 {

		_ = json.Unmarshal(currNodestate.Kvs[0].Value, &nodestate)
		nodestate.Role = datatypes.Controller
		modRev1 = currNodestate.Kvs[0].ModRevision
	} else {

		nodestate = NodeState{
			Role: datatypes.Controller,
		}
	}
	newNodeState, _ := json.Marshal(nodestate)

	// Настраиваем CAS-проверку для ключа ноды
	if modRev1 == 0 {
		cmp1 = clientv3.Compare(clientv3.Version(nodeKey), "=", 0)
	} else {
		cmp1 = clientv3.Compare(clientv3.ModRevision(nodeKey), "=", modRev1)
	}

	txn := n.cli.Txn(ctx).
		If(cmp, cmp1).
		Then(
			clientv3.OpPut(stateKey, string(newValue)),
			clientv3.OpPut(nodeKey, string(newNodeState), clientv3.WithLease(leaseID)),
		).
		Else()

	txnResp, err := txn.Commit()
	if err != nil {
		return 0, err
	}

	if !txnResp.Succeeded {
		return 0, errors.New("race condition: state or node registration modified by another node")
	}

	return nextEpoch, nil
}

func (n *NodeCoordinator) electionLoop(ctx context.Context) {
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

		var currentLeader int64 = 0
		var currentEpoch int64 = 0
		var modRev int64 = 0

		if len(resp.Kvs) > 0 {
			var state ControllerState
			if err := json.Unmarshal(resp.Kvs[0].Value, &state); err == nil {
				currentLeader = state.LeaderId
				currentEpoch = state.LeaderEpoch
				modRev = resp.Kvs[0].ModRevision
			}
		}

		if currentLeader == n.nodeID {
			n.SwitchToLeaderMode(currentEpoch)
			<-ctx.Done() // Удерживаем лидера до отмены ЭТОГО контекста сессии
			return
		}

		leaderAlive := false
		if currentLeader != 0 {
			nodeKey := fmt.Sprintf("%s%d", NodesDiscoveryPath, currentLeader)
			nodeResp, err := n.cli.Get(ctx, nodeKey)
			if err == nil && len(nodeResp.Kvs) > 0 {
				leaderAlive = true
			}
		}

		if leaderAlive {
			n.SwitchToFollowerMode(currentLeader, currentEpoch)

			// Вместо создания нового контекста с таймаутом, используем встроенный селект на базе ctx
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				// Спокойно идем на следующую итерацию проверки лидера
			}
			continue
		}

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
			n.SwitchToLeaderMode(nextEpoch)
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

	// Используем классический Watch для отслеживания изменений структуры ControllerState
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

func (nd *NodeDiscovery) WatchNodes(ctx context.Context) {
	log.Println("[Discovery] Запуск отслеживания активных нод кластера...")

	// ШАГ А: Запрашиваем текущее состояние (Bootstrap)
	// Это важно, чтобы узнать, какие ноды уже работают до того, как мы включили Watch.
	getResp, err := nd.cli.Get(ctx, NodesDiscoveryPath, clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("Не удалось получить список начальных нод: %v", err)
	}

	fmt.Println("=== ТЕКУЩИЕ АКТИВНЫЕ НОДЫ В КЛАСТЕРЕ ===")
	for _, kv := range getResp.Kvs {
		var info NodeState
		if err := json.Unmarshal(kv.Value, &info); err == nil {
			fmt.Printf("• Нода ID: %s, Адрес: %s (Запущена: %s)\n", info.ID, info.Address, info.StartTime)
		}
	}
	fmt.Println("========================================")

	// ШАГ Б: Запускаем Watch с точки остановки Get
	// Мы передаем clientv3.WithRev(getResp.Header.Revision + 1), чтобы не пропустить
	// ни одного события, произошедшего между вызовами Get и Watch.
	watchChan := nd.cli.Watch(ctx, NodesDiscoveryPath,
		clientv3.WithPrefix(),
		clientv3.WithPrevKV(),
		clientv3.WithRev(getResp.Header.Revision+1),
	)

	for watchResp := range watchChan {
		if watchResp.Canceled {
			log.Printf("[Discovery] Наблюдение за нодами отменено: %v", watchResp.Err())
			return
		}

		for _, ev := range watchResp.Events {
			fullKey := string(ev.Kv.Key)
			nodeID := strings.TrimPrefix(fullKey, NodesDiscoveryPath)

			switch ev.Type {
			case clientv3.EventTypePut:
				var info NodeState
				if err := json.Unmarshal(ev.Kv.Value, &info); err != nil {
					continue
				}

				if ev.Kv.Version == 1 {
					// Ключ появился впервые -> Нода подключилась
					fmt.Printf("[КЛАСТЕР] >>> Нода ПОДКЛЮЧИЛАСЬ: ID=%s, Адрес=%s\n", nodeID, info.Address)
				} else {
					// Ключ обновился -> Нода обновила метаданные
					fmt.Printf("[КЛАСТЕР] ↺ Нода ОБНОВИЛА данные: ID=%s, Новый адрес=%s\n", nodeID, info.Address)
				}

			case clientv3.EventTypeDelete:
				// Ключ удален -> Нода отключилась штатно ИЛИ упала (истек Lease)
				fmt.Printf("[КЛАСТЕР] <<< Нода ОТКЛЮЧИЛАСЬ или УПАЛА: ID=%s\n", nodeID)

				// Достаем из истории то, какой она была до падения
				if ev.PrevKv != nil {
					var oldInfo NodeState
					if err := json.Unmarshal(ev.PrevKv.Value, &oldInfo); err == nil {
						fmt.Printf("          Последний известный адрес ноды %s был: %s\n", nodeID, oldInfo.Address)
					}
				}
			}
		}
	}

}
func (n *NodeCoordinator) watchTopicLoop(ctx context.Context) {
	fmt.Println("Запуск отслеживания топиков...")

	// Настраиваем Watcher.
	// clientv3.WithPrefix() — заставляет следить за всеми ключами, начинающимися с префикса.
	// clientv3.WithPrevKV() — Позволяет получить предыдущее значение ключа при удалении или обновлении.
	watchChan := n.cli.Watch(ctx, TopicPath, clientv3.WithPrefix(), clientv3.WithPrevKV())

	for watchResp := range watchChan {
		if watchResp.Canceled {
			log.Printf("Watch был отменен: %v", watchResp.Err())
			return
		}

		for _, ev := range watchResp.Events {
			// Извлекаем имя топика из полного ключа
			// Например: "/kafka/topic/orders" -> "orders"
			fullKey := string(ev.Kv.Key)
			topicName := strings.TrimPrefix(fullKey, TopicPath)

			switch ev.Type {

			// ==========================================
			// СЛУЧАЙ 1: Ключ создан или обновлен (PUT)
			// ==========================================
			case clientv3.EventTypePut:
				var params TopicParams
				if err := json.Unmarshal(ev.Kv.Value, &params); err != nil {
					log.Printf("Ошибка парсинга JSON для топика %s: %v", topicName, err)
					continue
				}

				// Магия MVCC: Проверяем версию ключа
				if ev.Kv.Version == 1 {
					// Если Version == 1, значит этот ключ только что появился в etcd
					fmt.Printf("[СОЗДАН] Топик: %s\n", topicName)
					fmt.Printf("         Параметры: %+v\n", params)
				} else {
					// Если Version > 1, значит ключ уже существовал и это его обновление
					fmt.Printf("[ОБНОВЛЕН] Топик: %s (Новая версия: %d)\n", topicName, ev.Kv.Version)
					fmt.Printf("           Новые параметры: %+v\n", params)

					// Благодаря WithPrevKV() мы можем посмотреть, что изменилось:
					if ev.PrevKv != nil {
						var oldParams TopicParams
						_ = json.Unmarshal(ev.PrevKv.Value, &oldParams)
						fmt.Printf("           Старые параметры: %+v\n", oldParams)
					}
				}

			// ==========================================
			// СЛУЧАЙ 2: Ключ удален (DELETE)
			// ==========================================
			case clientv3.EventTypeDelete:
				fmt.Printf("[УДАЛЕН] Топик: %s\n", topicName)

				// При удалении ev.Kv.Value всегда пустой.
				// Но благодаря опции WithPrevKV() мы можем узнать, каким топик был перед удалением:
				if ev.PrevKv != nil {
					var deletedParams TopicParams
					if err := json.Unmarshal(ev.PrevKv.Value, &deletedParams); err == nil {
						fmt.Printf("         Удаленные параметры: %+v\n", deletedParams)
					}
				}
			}
		}
	}
}
