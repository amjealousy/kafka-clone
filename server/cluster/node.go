package cluster

import (
	"context"
	"errors"
	broker "kafka-clone/server/broker"
	"kafka-clone/server/datatypes"
	"log/slog"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
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

	// membersMx защищает карту известных нод кластера (id -> состояние).
	membersMx sync.RWMutex
	members   map[int64]NodeState

	address        string // gRPC-адрес репликации (ReplicationService) данной ноды
	tcpAddress     string // TCP-адрес для produce/consume клиентов
	controlAddress string // gRPC-адрес control-plane API (ControlService)
	logger         *slog.Logger

	offsetCommitCh chan broker.OffsetCommit
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
	}

	return b
}
func (n *NodeCoordinator) ClusterPause(resume chan bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	_ = n.broker.ToFollower()
	n.logger.Error("Node goes to cluster paused")
	//todo this method is filler a bit for current state, remake for infra needs
	<-resume
	n.ClusterResume()

}

func (n *NodeCoordinator) ClusterResume() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.logger.Info("[Redundant] Node goes to cluster resume")
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

	n.publishOwnRole(n.ctx, datatypes.Controller)

	n.logger.Info("★★★ УСПЕШНО СТАЛ Controller  ★★★", slog.Any("nodeId", n.nodeID), slog.Any("epoch", epoch))
}

func (n *NodeCoordinator) SwitchToFollowerMode(controllerID int64, epoch int64) {
	n.mu.Lock()
	_ = n.broker.ToFollower()
	_ = n.broker.SetEpoch(epoch)
	n.LeaderNodeId = int(controllerID)
	n.mu.Unlock()

	n.publishOwnRole(n.ctx, datatypes.Follower)
	n.logger.Info("Переведен в режим FOLLOWER. Controller в кластере:", slog.Any("nodeId", n.nodeID), slog.Int64("controllerID", controllerID), slog.Int64("epoch", epoch))
}

func (n *NodeCoordinator) GetLeaderId() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.LeaderNodeId
}
