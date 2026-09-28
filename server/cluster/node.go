package cluster

import (
	"context"
	"errors"
	broker "kafka-clone/server/broker"
	brokertypes "kafka-clone/server/datatypes/broker"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var (
	ErrStaleEpoch         = errors.New("fencing error: request epoch is stale")
	ErrNotFound           = errors.New("offset not found in index")
	ErrLeaderNotAvailable = errors.New("LEADER_NOT_AVAILABLE")
)

type NodeCoordinator struct {
	nodeID         int64
	broker         *broker.Broker
	mu             *sync.RWMutex
	ctx            context.Context // etcd(cluster) drop or SIG interruption
	cancel         context.CancelFunc
	cli            *clientv3.Client
	LeaderNodeId   int
	discovery      *NodeDiscovery
	electionCtx    context.Context
	cancelElection context.CancelFunc
	topicMeta      *TopicMetaStore
	localState     *NodeLocalStateStore
	// syncing защищает от параллельного запуска catch-up для одной и той же
	// партиции при повторных событиях watchTopicLoop.
	syncingMx sync.Mutex
	syncing   map[string]bool

	// membersMx защищает мапу известных нод кластера (id -> состояние).
	// ВАЖНО: это watch-кэш для ВНУТРЕННЕЙ логики (репликация, реконфигурация
	// партиций). Внешние read-ручки его НЕ используют — они читают etcd
	// напрямую (см. cluster_view.go), иначе отвалившаяся от кластера нода
	// продолжала бы отдавать клиенту правдоподобный устаревший состав.
	membersMx sync.RWMutex
	members   map[int64]NodeState

	// addresses — advertised-адреса этой ноды, опубликованные в etcd.
	addresses NodeAddresses
	logger    *slog.Logger

	// viewMx сериализует чтение снимка кластера из etcd и заодно работает как
	// singleflight: параллельные запросы админки не превращаются в N чтений.
	viewMx        sync.Mutex
	viewCache     *clusterView
	viewFetchedAt time.Time

	// controlConns — переиспользуемые gRPC-соединения с control-plane других
	// нод; нужны для проксирования мутаций на контроллер.
	controlMx    sync.Mutex
	controlConns map[string]*grpc.ClientConn

	// paused — нода в аварийном режиме после потери сессии etcd. Он же
	// защищает от параллельного запуска нескольких recoveryLoop: потерю
	// сессии может обнаружить и keepalive-горутина, и ре-бутстрап WatchNodes.
	paused atomic.Bool

	// rebalanceCh — очередь выбывших нод на реконфигурацию партиций. Очередь,
	// а не горутина на каждое выбытие: после ре-бутстрапа watch'а разом
	// "уходит" сразу несколько нод, и параллельные rebalanceAfterNodeLoss
	// устроили бы шторм записей в etcd по одним и тем же топикам.
	rebalanceCh chan int64

	offsetCommitCh chan broker.OffsetCommit
}

// Close останавливает фоновые циклы координатора и закрывает исходящие
// control-plane соединения. Вызывается при штатной остановке ноды.
func (n *NodeCoordinator) Close() error {
	n.cancel()

	n.controlMx.Lock()
	defer n.controlMx.Unlock()
	for addr, conn := range n.controlConns {
		if err := conn.Close(); err != nil {
			n.logger.Warn("failed to close control-plane connection", "addr", addr, "error", err)
		}
		delete(n.controlConns, addr)
	}
	return nil
}

func NewNodeCoordinator(nodeID int64, logger *slog.Logger, broker *broker.Broker, cli *clientv3.Client) *NodeCoordinator {
	ctx, cancel := context.WithCancel(context.Background())
	logger = logger.With("component", "NodeCoordinator")
	electionCtx, electionCancel := context.WithCancel(ctx)
	b := &NodeCoordinator{
		nodeID:         nodeID,
		cli:            cli,
		broker:         broker,
		mu:             &sync.RWMutex{},
		ctx:            ctx,
		cancel:         cancel,
		electionCtx:    electionCtx,
		cancelElection: electionCancel,
		topicMeta:      NewTopicMetaStore(cli),
		localState:     NewNodeLocalStateStore(cli),
		syncing:        make(map[string]bool),
		members:        make(map[int64]NodeState),
		logger:         logger,
		rebalanceCh:    make(chan int64, 64),
	}

	return b
}

// IsPaused сообщает, находится ли нода в аварийном режиме после потери сессии
// etcd. Пока флаг взведён, нода не является полноценным участником кластера:
// её ключ в /kafka/nodes/ истёк, партиции переназначены, выборы остановлены.
//
// Раньше эта пауза была реализована горутиной ClusterPause, которая брала
// n.mu на запись и блокировалась на канале resume в ожидании recoveryLoop.
// Это был гарантированный дедлок: recoveryLoop по пути к отправке resume
// вызывает GetLeaderId, а тот берёт n.mu.RLock и навсегда упирается в
// удерживаемый write-lock. Нода перерегистрировалась в etcd, выглядела для
// остальных живой и при этом навсегда оставалась Unroled с заблокированным
// n.mu. Состояние-флаг решает ту же задачу, ничего не блокируя.
func (n *NodeCoordinator) IsPaused() bool {
	return n.paused.Load()
}

func (n *NodeCoordinator) UpdateLeaderNode(leader int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.logger.Info("Update leader node", slog.Int64("leader-id", leader))
	n.LeaderNodeId = int(leader)

}
func (n *NodeCoordinator) bootstrapAndSyncSequence() {
	n.logger.Info("[Coordinator] Ожидание стабилизации watchers перед синхронизацией...")

	// Защита: спим по тикеру или выходим, если приложение закрывается
	select {
	case <-n.ctx.Done():
		return
	case <-time.After(1 * time.Second):
	}

	for {
		select {
		case <-n.ctx.Done(): // Если приложение закрывают во время синхронизации — выходим
			return
		default:
		}

		// Выкачиваем данные от текущего лидера (используем глобальный контекст)
		if err := n.broker.RestoreData(n.ctx, int64(n.GetLeaderId())); err != nil {
			n.logger.Error("[Coordinator] Ошибка репликации при старте. Повтор...", slog.String("err", err.Error()))

			select {
			case <-n.ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		break
	}

	n.logger.Info("[Coordinator] Данные синхронизированы. Инициализация electionCtx для выборов...")

	n.mu.Lock()
	// Создаем контекст выборов КАК ДОЧЕРНИЙ от глобального n.ctx.
	// Если n.ctx отменится, electionCtx закроется автоматически!
	n.electionCtx, n.cancelElection = context.WithCancel(n.ctx)

	// Запускаем луп выборов строго с его изолированным контекстом
	go n.electionLoop(n.electionCtx)
	n.mu.Unlock()
}

func (n *NodeCoordinator) SwitchToControllerMode(epoch int64) {
	n.mu.Lock()
	_ = n.broker.ToController()
	_ = n.broker.SetEpoch(epoch)
	n.LeaderNodeId = int(n.nodeID)
	n.mu.Unlock()

	n.publishOwnRole(n.ctx, brokertypes.Controller)

	n.logger.Info("★★★ УСПЕШНО СТАЛ Controller  ★★★", slog.Any("nodeId", n.nodeID), slog.Any("epoch", epoch))
}

func (n *NodeCoordinator) SwitchToFollowerMode(controllerID int64, epoch int64) {
	n.mu.Lock()
	_ = n.broker.ToFollower()
	_ = n.broker.SetEpoch(epoch)
	n.LeaderNodeId = int(controllerID)
	n.mu.Unlock()

	n.publishOwnRole(n.ctx, brokertypes.Follower)
	n.logger.Info("Переведен в режим FOLLOWER. Controller в кластере:", slog.Any("nodeId", n.nodeID), slog.Int64("controllerID", controllerID), slog.Int64("epoch", epoch))
}

func (n *NodeCoordinator) GetLeaderId() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.LeaderNodeId
}

func (n *NodeCoordinator) invalidateClusterView() {
	n.viewMx.Lock()
	defer n.viewMx.Unlock()

	n.viewCache = nil
	n.viewFetchedAt = time.Time{}

}
