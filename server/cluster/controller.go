package cluster

import (
	"cmp"
	"context"
	"kafka-clone/server/datatypes/broker"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"time"

	"kafka-clone/server/datatypes"
	gen "kafka-clone/server/datatypes/proto-generated"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// onNodesSnapshot применяет полный состав кластера, полученный при
// (ре-)бутстрапе наблюдения.
//
// Ключевой момент — обработка ИСЧЕЗНУВШИХ нод. Пока наблюдение было разорвано,
// события об их удалении прошли мимо нас; если просто применить снимок как
// набор onNodeJoin, они останутся в n.members навсегда, и контроллер будет
// продолжать назначать на них партиции. Поэтому считаем разницу.
func (n *NodeCoordinator) onNodesSnapshot(snapshot map[int64]NodeState) {
	n.membersMx.RLock()
	previous := maps.Clone(n.members)
	n.membersMx.RUnlock()

	left, joined := diffMembers(previous, snapshot)

	for _, state := range left {
		n.onNodeLeave(state)
	}
	for _, state := range joined {
		n.onNodeJoin(state)
	}

	if len(left) > 0 || len(joined) > 0 {
		n.logger.Info("[Discovery] Состав кластера пересобран",
			slog.Int("left", len(left)), slog.Int("joined", len(joined)), slog.Int("total", len(snapshot)))
	}

	// Нас самих нет в составе кластера: аренда истекла, пока мы не смотрели.
	// Это не "ушёл кто-то другой" — ребалансировать кластер от своего имени мы
	// права не имеем, нужно восстанавливать собственную регистрацию.
	if _, present := snapshot[n.nodeID]; !present {
		n.logger.Warn("[Discovery] Нода отсутствует в собственном снимке кластера — аварийное восстановление")
		n.handleSessionLoss()
	}
}

// diffMembers возвращает выбывшие и появившиеся/изменившиеся ноды.
// NodeState сравним целиком, поэтому смена роли, адресов или перерегистрация
// с новым StartTime тоже попадают в joined.
func diffMembers(previous, current map[int64]NodeState) (left, joined []NodeState) {
	for id, old := range previous {
		if _, still := current[id]; !still {
			left = append(left, old)
		}
	}
	for id, state := range current {
		if old, existed := previous[id]; !existed || old != state {
			joined = append(joined, state)
		}
	}

	// Детерминированный порядок: иначе реконфигурация партиций на разных
	// прогонах выбирает разные ноды, и воспроизвести баг невозможно.
	slices.SortFunc(left, func(a, b NodeState) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(joined, func(a, b NodeState) int { return cmp.Compare(a.ID, b.ID) })
	return left, joined
}

// onNodeJoin вызывается при появлении/обновлении ноды в кластере. Помимо
// учёта членства, устанавливает gRPC-соединение для репликации (кроме себя).
func (n *NodeCoordinator) onNodeJoin(state NodeState) {
	n.membersMx.Lock()
	_, existed := n.members[state.ID]
	n.members[state.ID] = state
	n.membersMx.Unlock()

	if state.ID == n.nodeID {
		return // с самим собой соединение не нужно
	}
	if existed {
		return // соединение уже устанавливалось
	}
	if state.Address == "" {
		return
	}

	// Устанавливаем gRPC-соединение с пиром для репликации/восстановления лога.
	conn, err := grpc.NewClient(state.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("[Controller] Не удалось создать gRPC-клиент к пиру",
			slog.Int64("peer", state.ID), slog.String("addr", state.Address), slog.String("err", err.Error()))
		return
	}
	peer := &datatypes.Peer{
		ID:                int(state.ID),
		Addr:              state.Address,
		GrpcConn:          conn,
		ReplicationClient: gen.NewReplicationServiceClient(conn),
	}
	if err := n.broker.AddPeers(peer); err != nil {
		slog.Error("[Controller] Не удалось зарегистрировать пира", slog.Int64("peer", state.ID), slog.String("err", err.Error()))
	}
}

// onNodeLeave вызывается при выбытии ноды из кластера (штатно или по lease).
// На контроллере запускает переназначение партиций, где выбывшая нода была ISR.
func (n *NodeCoordinator) onNodeLeave(state NodeState) {
	n.membersMx.Lock()
	delete(n.members, state.ID)
	n.membersMx.Unlock()

	n.broker.RemovePeer(int(state.ID))

	// Реагировать на топологию кластера имеет право только контроллер.
	if n.broker.ReadRole() != broker.Controller {
		return
	}

	slog.Warn("[Controller] Обнаружено выбытие ноды, ставим в очередь реконфигурацию партиций",
		slog.Int64("deadNode", state.ID))

	n.enqueueRebalance(state.ID)
}

// enqueueRebalance ставит выбывшую ноду в очередь реконфигурации.
//
// Раньше здесь была горутина на каждое выбытие. После ре-бутстрапа наблюдения
// "уходят" сразу несколько нод, и параллельные rebalanceAfterNodeLoss начали бы
// наперегонки переписывать одни и те же ключи топиков, проигрывая CAS друг
// другу. Отправка неблокирующая: наблюдение за кластером важнее, а пропущенное
// переназначение всё равно подхватит runControllerReconcileLoop.
func (n *NodeCoordinator) enqueueRebalance(deadNodeID int64) {
	select {
	case n.rebalanceCh <- deadNodeID:
	default:
		n.logger.Warn("[Controller] Очередь реконфигурации переполнена, полагаемся на reconcile-цикл",
			slog.Int64("deadNode", deadNodeID))
	}
}

// runRebalanceWorker последовательно обрабатывает очередь выбывших нод.
func (n *NodeCoordinator) runRebalanceWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case deadNodeID := <-n.rebalanceCh:
			// Роль могла смениться, пока задача ждала в очереди.
			if n.broker.ReadRole() != broker.Controller {
				continue
			}
			n.rebalanceAfterNodeLoss(deadNodeID)
		}
	}
}

// rebalanceAfterNodeLoss просматривает все топики и для каждой партиции, где
// выбывшая нода была назначена, убирает её и (если после этого число in-sync
// реплик стало меньше желаемого replication factor) назначает новую Unroled-ноду.
func (n *NodeCoordinator) rebalanceAfterNodeLoss(deadNodeID int64) {
	ctx, cancel := context.WithTimeout(n.ctx, 15*time.Second)
	defer cancel()

	topics, err := n.topicMeta.ListTopics(ctx)
	if err != nil {
		slog.Error("[Controller] Не удалось получить список топиков для реконфигурации", slog.String("err", err.Error()))
		return
	}

	for _, tparams := range topics {
		desiredRF := tparams.DesiredReplicationFactor
		if desiredRF <= 0 {
			desiredRF = 3 // 2 реплики + 1 лидер по умолчанию
		}

		for _, part := range tparams.Partitions {
			if !part.ContainsNode(deadNodeID) {
				continue
			}

			// 1. Убираем выбывшую ноду из реплик и ISR партиции.
			if err := n.topicMeta.RemoveNodeFromPartition(ctx, tparams.Name, part.PartitionId, deadNodeID); err != nil {
				slog.Error("[Controller] Не удалось убрать выбывшую ноду из партиции",
					slog.String("topic", tparams.Name), slog.Int64("partition", part.PartitionId), slog.String("err", err.Error()))
				continue
			}
			slog.Info("[Controller] Выбывшая нода удалена из партиции",
				slog.String("topic", tparams.Name), slog.Int64("partition", part.PartitionId), slog.Int64("deadNode", deadNodeID))

			// 2. Перечитываем актуальное состояние партиции после удаления.
			cur, _, err := n.topicMeta.GetTopic(ctx, tparams.Name)
			if err != nil || cur == nil {
				continue
			}
			var curPart *PartitionsParams
			for i := range cur.Partitions {
				if cur.Partitions[i].PartitionId == part.PartitionId {
					curPart = &cur.Partitions[i]
					break
				}
			}
			if curPart == nil {
				continue
			}

			// 3. Если реплик всё ещё достаточно — назначать новую не нужно.
			if len(curPart.ReplicasNodeId) >= desiredRF {
				continue
			}

			// 4. Ищем кандидата на замену: свободную Unroled-ноду, ещё не входящую
			// в эту партицию. Среди таких нод приоритет — у той, что уже
			// само-репортила в etcd наличие локальных данных именно по этой
			// (topic, partition): ей потребуется докачать через FetchLog
			// значительно меньше, чем полностью пустой ноде.
			candidate := n.pickReplicaCandidate(ctx, tparams.Name, part.PartitionId, curPart.ReplicasNodeId)
			if candidate == 0 {
				slog.Warn("[Controller] Нет свободной Unroled-ноды для назначения на партицию",
					slog.String("topic", tparams.Name), slog.Int64("partition", part.PartitionId))
				continue
			}

			// 5. Назначаем ноду как реплику (НЕ in-sync). Как только запись в etcd
			// применится, назначенная нода через watchTopicLoop создаст локально
			// топик/партицию и начнёт catch-up, после чего сама впишет себя в ISR.
			if err := n.topicMeta.AddReplicaToPartition(ctx, tparams.Name, part.PartitionId, candidate); err != nil {
				slog.Error("[Controller] Не удалось назначить новую реплику",
					slog.String("topic", tparams.Name), slog.Int64("partition", part.PartitionId),
					slog.Int64("candidate", candidate), slog.String("err", err.Error()))
				continue
			}
			slog.Info("[Controller] На партицию назначена новая нода-реплика",
				slog.String("topic", tparams.Name), slog.Int64("partition", part.PartitionId), slog.Int64("newReplica", candidate))
		}
	}
}

// runControllerReconcileLoop периодически проверяет, что у каждой партиции
// достаточно назначенных реплик (>= desiredRF). Работает эффективно только
// когда данная нода является контроллером; иначе просто ждёт. Это покрывает
// сценарий "в топике появилась новая партиция" — контроллер доназначает на
// неё свободные Unroled-ноды.
func (n *NodeCoordinator) runControllerReconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n.broker.ReadRole() != broker.Controller {
				continue
			}
			n.reconcileReplication(ctx)
		}
	}
}

// reconcileReplication проходит по всем партициям всех топиков и до-назначает
// реплики там, где их меньше желаемого replication factor.
func (n *NodeCoordinator) reconcileReplication(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()

	topics, err := n.topicMeta.ListTopics(ctx)
	if err != nil {
		return
	}

	for _, tparams := range topics {
		desiredRF := tparams.DesiredReplicationFactor
		if desiredRF <= 0 {
			desiredRF = 3
		}
		for _, part := range tparams.Partitions {
			// До-назначаем недостающие реплики по одной за проход.
			if len(part.ReplicasNodeId) >= desiredRF {
				continue
			}
			candidate := n.pickReplicaCandidate(ctx, tparams.Name, part.PartitionId, part.ReplicasNodeId)
			if candidate == 0 {
				continue
			}
			if err := n.topicMeta.AddReplicaToPartition(ctx, tparams.Name, part.PartitionId, candidate); err != nil {
				slog.Error("[Controller] reconcile: не удалось назначить реплику",
					slog.String("topic", tparams.Name), slog.Int64("partition", part.PartitionId), slog.String("err", err.Error()))
				continue
			}
			slog.Info("[Controller] reconcile: назначена новая реплика на партицию",
				slog.String("topic", tparams.Name), slog.Int64("partition", part.PartitionId), slog.Int64("newReplica", candidate))
		}
	}
}

// computeNodeLoad сканирует ВСЕ топики в etcd и считает для каждой ноды:
//   - partitionCount[nodeID] — в скольких партициях (в любой роли: лидер или
//     реплика) она сейчас участвует по всему кластеру;
//   - leaderCount[nodeID] — в скольких партициях она именно лидер.
//
// Используется и для балансировки при создании нового топика (CreateTopic),
// и как фолбэк-критерий при подборе замены выбывшей реплики.
func (n *NodeCoordinator) computeNodeLoad(ctx context.Context) (partitionCount map[int64]int, leaderCount map[int64]int, err error) {
	topics, err := n.topicMeta.ListTopics(ctx)
	if err != nil {
		return nil, nil, err
	}
	partitionCount, leaderCount = computeNodeLoadFrom(topics)
	return partitionCount, leaderCount, nil
}

// computeNodeLoadFrom — та же арифметика поверх уже прочитанного списка
// топиков. Нужна, чтобы CreateTopic мог посчитать нагрузку по снимку кластера
// (cluster_view) и не ходить в etcd за топиками второй раз.
func computeNodeLoadFrom(topics []TopicParams) (partitionCount map[int64]int, leaderCount map[int64]int) {
	partitionCount = make(map[int64]int)
	leaderCount = make(map[int64]int)
	for _, t := range topics {
		for _, p := range t.Partitions {
			for _, rid := range p.ReplicasNodeId {
				partitionCount[rid]++
			}
			leaderCount[p.LeaderNodeId]++
		}
	}
	return partitionCount, leaderCount
}

// pickReplicaCandidate выбирает ноду для назначения на партицию (topicName,
// partitionID) среди ВСЕХ живых нод кластера (кластерная роль Controller/
// Follower/Unroled тут ни при чём — это про выборы контроллера, а не про
// наличие свободной ёмкости для данных; раньше здесь ошибочно фильтровалось
// по Role==Unroled, из-за чего почти сразу после старта кандидатов не
// находилось вообще, т.к. electionLoop быстро переводит всех не-лидеров в
// Follower). Исключаются только уже входящие в exclude ноды.
//
// Приоритет №1 — нода, которая уже само-репортила в etcd (NodeLocalStateStore)
// наличие локальных данных именно по этой (topic, partition), с максимальным
// offset (меньше всего докачивать через FetchLog).
//
// Приоритет №2 (фолбэк, если таких нет) — наименее загруженная нода: та, у
// которой сейчас меньше всего партиций назначено по всему кластеру (см.
// computeNodeLoad) — чтобы новые/восстанавливаемые партиции равномерно
// распределялись, а не оседали на одной ноде.
//
// Возвращает id кандидата или 0, если подходящих нод нет вообще.
func (n *NodeCoordinator) pickReplicaCandidate(ctx context.Context, topicName string, partitionID int64, exclude []int64) int64 {
	excludeSet := make(map[int64]bool, len(exclude))
	for _, id := range exclude {
		excludeSet[id] = true
	}

	// Само-репорты нод — best-effort: ошибка чтения не должна блокировать
	// назначение реплики, просто деградируем до балансировки по нагрузке.
	localCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	localStates, lsErr := n.localState.ListAll(localCtx)
	cancel()
	if lsErr != nil {
		slog.Warn("[Controller] Не удалось прочитать само-репорты локального состояния нод", slog.String("err", lsErr.Error()))
	}

	partitionCount, _, loadErr := n.computeNodeLoad(ctx)
	if loadErr != nil {
		slog.Warn("[Controller] Не удалось посчитать нагрузку нод, балансировка деградирует", slog.String("err", loadErr.Error()))
		partitionCount = map[int64]int{}
	}

	n.membersMx.RLock()
	defer n.membersMx.RUnlock()

	var bestCandidate int64
	var bestOffset uint64
	candidates := make([]int64, 0, len(n.members))

	for id := range n.members {
		if excludeSet[id] {
			continue
		}
		candidates = append(candidates, id)
		for _, entry := range localStates[id] {
			if entry.Topic != topicName || entry.PartitionId != partitionID {
				continue
			}
			if bestCandidate == 0 || entry.Offset > bestOffset {
				bestCandidate = id
				bestOffset = entry.Offset
			}
		}
	}

	if bestCandidate != 0 {
		slog.Info("[Controller] Найдена нода с остаточными локальными данными по партиции — приоритетно назначаем её",
			slog.String("topic", topicName), slog.Int64("partition", partitionID),
			slog.Int64("candidate", bestCandidate), slog.Uint64("localOffset", bestOffset))
		return bestCandidate
	}

	if len(candidates) == 0 {
		return 0
	}

	// Фолбэк: сортируем по возрастанию текущей нагрузки (детерминированный
	// tie-break по id) и берём наименее загруженную ноду.
	sort.Slice(candidates, func(a, b int) bool {
		la, lb := partitionCount[candidates[a]], partitionCount[candidates[b]]
		if la != lb {
			return la < lb
		}
		return candidates[a] < candidates[b]
	})
	return candidates[0]
}
