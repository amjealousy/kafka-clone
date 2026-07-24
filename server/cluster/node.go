package cluster

import (
	"context"
	"errors"
	"fmt"
	broker "kafka-clone/server/broker"
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
}

func NewNodeCoordinator(nodeID int64, broker *broker.Broker, cli *clientv3.Client) *NodeCoordinator {
	ctx, cancel := context.WithCancel(context.Background())
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
	}

	return b
}
func (n *NodeCoordinator) ClusterPause(resume chan bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	_ = n.broker.ToFollower()
	slog.Error("Node goes to cluster paused")
	//todo this method is filler a bit for current state, remake for infra needs
	<-resume
	n.ClusterResume()

}

func (n *NodeCoordinator) ClusterResume() {
	n.mu.Lock()
	defer n.mu.Unlock()
	slog.Info("[Redundant] Node goes to cluster resume")
}

func (n *NodeCoordinator) UpdateLeaderNode(leader int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.LeaderNodeId = int(leader)

}
func (n *NodeCoordinator) bootstrapAndSyncSequence() {
	slog.Info("[Coordinator] Ожидание стабилизации watchers перед синхронизацией...")

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
			slog.Error("[Coordinator] Ошибка репликации при старте. Повтор...", slog.String("err", err.Error()))

			select {
			case <-n.ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		break
	}

	slog.Info("[Coordinator] Данные синхронизированы. Инициализация electionCtx для выборов...")

	n.mu.Lock()
	// Создаем контекст выборов КАК ДОЧЕРНИЙ от глобального n.ctx.
	// Если n.ctx отменится, electionCtx закроется автоматически!
	n.electionCtx, n.cancelElection = context.WithCancel(n.ctx)

	// Запускаем луп выборов строго с его изолированным контекстом
	go n.electionLoop(n.electionCtx)
	n.mu.Unlock()
}

func (n *NodeCoordinator) SwitchToLeaderMode(epoch int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	_ = n.broker.ToController()
	_ = n.broker.SetEpoch(epoch)

	fmt.Printf("\n★★★ [%s] УСПЕШНО СТАЛ ЛИДЕРОМ (ЭПОХА %d) ★★★\n\n", n.nodeID, epoch)
}

func (n *NodeCoordinator) SwitchToFollowerMode(leaderID int64, epoch int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Обновляем эпоху хранилища, чтобы заблокировать локальный диск от зомби-потоков
	_ = n.broker.ToFollower()
	_ = n.broker.SetEpoch(epoch)
	n.UpdateLeaderNode(leaderID)
	fmt.Printf("[%s] Переведен в режим FOLLOWER. Лидер в кластере: %s (Эпоха %d)\n", n.nodeID, leaderID, epoch)
}

func (n *NodeCoordinator) GetLeaderId() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.LeaderNodeId
}
