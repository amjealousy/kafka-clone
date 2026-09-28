package broker

import (
	"context"
	"errors"
	"fmt"

	gen "kafka-clone/server/datatypes/proto-generated"
	"kafka-clone/server/helper"
	"kafka-clone/server/topic"
	"log/slog"
	"sync"
	"time"
)

type TopicProcessor struct {
	*helper.Lifecycle
	id         int
	Name       string
	partitions map[int]*PartitionProcessor
	log        *slog.Logger
	mx         *sync.RWMutex
}

func NewTopicProcessor(id int, name string, logger *slog.Logger, partitions []*topic.Partition, poolCtx context.Context) *TopicProcessor {
	processors := make(map[int]*PartitionProcessor, len(partitions))
	logger.With("topic", name).With("component", "[TopicProcessor]")
	tp := &TopicProcessor{
		id:         id,
		Name:       name,
		mx:         &sync.RWMutex{},
		log:        logger,
		partitions: processors,

		Lifecycle: helper.DeriveLifecycle(poolCtx),
	}
	for _, partition := range partitions {
		processors[partition.Id] = NewPartitionProcessor(tp.Context(), partition.Id, name, partition.StartOffset, logger, partition.Retention, partition.Replicas)
	}
	return tp
}

var NotFoundPartitionErr = errors.New("not found partition")

func (tp *TopicProcessor) GetPartition(id int) (error, *PartitionProcessor) {
	tp.mx.RLock()
	defer tp.mx.RUnlock()
	if processor, ok := tp.partitions[id]; !ok {
		return NotFoundPartitionErr, nil
	} else {
		return nil, processor
	}
}
func (tp *TopicProcessor) DeletePartition(partID int, forceDelete bool) error {
	tp.mx.Lock()

	p, ok := tp.partitions[partID]
	if !ok {
		tp.mx.Unlock()
		return NotFoundPartitionErr
	}

	// Пока удерживается tp.mx, новые GetPartition не пройдут.
	p.mx.Lock()
	p.state = PartitionStopping
	delete(tp.partitions, partID)
	p.cond.Broadcast()
	p.mx.Unlock()

	tp.mx.Unlock()

	return p.StopAndRemove(forceDelete)
}

// EnsurePartition создаёт процессор партиции локально, если его ещё нет.
// Возвращает процессор и флаг created (была ли партиция только что создана).
func (tp *TopicProcessor) EnsurePartition(part *topic.Partition) (*PartitionProcessor, bool) {
	tp.mx.Lock()
	defer tp.mx.Unlock()
	if pp, ok := tp.partitions[part.Id]; ok {
		return pp, false
	}
	pp := NewPartitionProcessor(tp.Context(), part.Id, tp.Name, part.StartOffset, tp.log, part.Retention, part.Replicas)
	tp.partitions[part.Id] = pp
	tp.log.Info("Partition dynamically created", "topic", tp.Name, "partition", part.Id)
	return pp, true
}

// ListPartitions возвращает id всех локальных партиций топика.
func (tp *TopicProcessor) ListPartitions() []int {
	tp.mx.RLock()
	defer tp.mx.RUnlock()
	ids := make([]int, 0, len(tp.partitions))
	for id := range tp.partitions {
		ids = append(ids, id)
	}
	return ids
}

// replicaTarget — реплика, которой был отправлен push, вместе с уже
// полученным для неё gRPC-клиентом.
type replicaTarget struct {
	id     int
	client gen.ReplicationServiceClient
}

// invalidateTimeout — верхняя граница на откат записи по всем репликам.
const invalidateTimeout = 3 * time.Second

// invalidateReplicas рассылает InvalidateLastOffset всем репликам, которым
// была отправлена запись targetOffset, после того как лидер не смог
// закоммитить её локально.
//
// Контекст исходного produce-запроса здесь намеренно НЕ используется: откат
// обязан быть выполнен, даже если клиент уже отвалился или ctx отменён —
// иначе расхождение логов останется навсегда. Поэтому берём независимый
// контекст с собственным таймаутом.
//
// Ждём завершения всех вызовов (wg.Wait) до возврата ошибки продюсеру: так
// клиент получает отказ уже после того, как попытка привести кластер в
// согласованное состояние завершена, а не параллельно с ней.
func (tp *TopicProcessor) invalidateReplicas(
	targets []replicaTarget,
	partitionID int,
	targetOffset uint64,
	leaderID int,
	clusterNet ClusterPeerProvider,
) {
	if len(targets) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), invalidateTimeout)
	defer cancel()

	var wg sync.WaitGroup
	for _, target := range targets {
		wg.Add(1)
		go func(t replicaTarget) {
			defer wg.Done()

			req := &gen.InvalidateRequest{
				Term:         uint64(clusterNet.GetEpoch()),
				LeaderId:     int32(leaderID),
				TopicName:    tp.Name,
				PartitionId:  uint32(partitionID),
				TargetOffset: targetOffset,
			}

			ack, err := t.client.InvalidateLastOffset(ctx, req)
			if err != nil {
				// Реплика недоступна — она осталась с лишней записью. Догнать
				// её обычным push'ем уже не выйдет (будет PushRejectedGap),
				// расхождение придётся чинить через catch-up/переназначение.
				tp.log.Error("Failed to invalidate record on replica: log divergence",
					"slavePeer", t.id, "topic", tp.Name, "partition", partitionID,
					"offset", targetOffset, "error", err)
				return
			}

			if ack.GetStatus() != gen.Status_Ok {
				tp.log.Error("Replica rejected invalidate: log divergence",
					"slavePeer", t.id, "topic", tp.Name, "partition", partitionID,
					"offset", targetOffset, "message", ack.GetMessage())
				return
			}

			tp.log.Warn("Rolled back record on replica after leader commit failure",
				"slavePeer", t.id, "topic", tp.Name, "partition", partitionID,
				"offset", targetOffset)
		}(target)
	}

	wg.Wait()
}

func (tp *TopicProcessor) ReplicateAndAppend(
	ctx context.Context,
	partitionID int,
	payload []byte,
	clusterNet ClusterPeerProvider,
) (uint64, error) {
	mergedCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-tp.Done(): // Наш Lifecycle топика сказал "Стоп!"
			cancel() // Отменяем mergedCtx
		case <-mergedCtx.Done():
			return
		}
	}()
	// 1. Находим нужный процессор партиции
	err, partProcessor := tp.GetPartition(partitionID)
	if err != nil {
		return 0, err
	}

	// 2. Читаем актуальный набор реплик. Внешний лок на партицию не берём:
	//    все обращения к процессору партиции защищены его собственными
	//    внутренними блокировками (иначе был бы дедлок с RLock в геттерах).
	replicas := partProcessor.GetReplicas()

	// TargetOffset — оффсет, на который ляжет новое сообщение (текущий nextOffset).
	targetOffset := partProcessor.GetNextOffset()
	leaderID := clusterNet.GetNodeId()

	// Timestamp фиксируем ОДИН раз здесь и используем его и для пуша на все
	// реплики, и для последующего локального коммита лидера (шаг 5) — так лог
	// лидера и логи реплик содержат байт-в-байт идентичные (offset, timestamp,
	// payload) записи.
	timestamp := time.Now().UnixNano()

	var wg sync.WaitGroup
	ackChan := make(chan int, len(replicas))

	// pushed — реплики, которым запись реально была отправлена. Именно им
	// придётся слать InvalidateLastOffset, если лидер не сможет закоммитить
	// запись у себя (шаг 5). Собираем ВСЕХ, кому ушёл RPC, а не только тех,
	// кто прислал ACK: ответ мог потеряться уже после успешной записи на
	// реплике (таймаут/обрыв), и такая "молчаливая" реплика тоже обязана
	// откатиться.
	pushed := make([]replicaTarget, 0, len(replicas))

	// 3. Параллельная репликация по gRPC на все НАЗНАЧЕННЫЕ реплики (и in-sync,
	//    и ещё догоняющие лог — последние тоже принимают новые сообщения).
	for _, replica := range replicas {
		if replica.Id == leaderID {
			continue // Себя пропускаем, запишем на диск лидера позже
		}

		client, err := clusterNet.GetGrpcClient(replica.Id)
		if err != nil {
			tp.log.Error("Failed to get gRPC replication client", "peerID", replica.Id, "error", err)
			continue
		}

		pushed = append(pushed, replicaTarget{id: replica.Id, client: client})

		wg.Add(1)
		go func(pid int, cl gen.ReplicationServiceClient) {
			defer wg.Done()

			rpcCtx, rpcCancel := context.WithTimeout(mergedCtx, 1*time.Second)
			defer rpcCancel()

			req := &gen.AppendEntriesRequest{
				Term:         uint64(clusterNet.GetEpoch()),
				LeaderId:     int32(leaderID),
				TopicName:    tp.Name,
				PartitionId:  uint32(partitionID),
				TargetOffset: targetOffset,
				Payload:      payload,
				Timestamp:    timestamp,
			}

			res, err := cl.AppendEntries(rpcCtx, req)
			if err != nil {
				tp.log.Error("gRPC replication call failed", "slavePeer", pid, "error", err)
				return
			}
			if res.Success {
				ackChan <- pid
			} else if res.NotInSync {
				// Реплика ещё восстанавливает лог (StateReplicating) и осознанно
				// отклонила push — это ожидаемо, догонит через FetchLog.
				tp.log.Debug("Replica still catching up, push skipped", "slavePeer", pid)
			} else {
				// Реплика уже in-sync, но offset разошёлся (гонка/лаг сети).
				tp.log.Warn("Replica rejected log append: offset gap", "slavePeer", pid, "matchOffset", res.MatchOffset)
			}

		}(replica.Id, client)
	}

	wg.Wait()
	close(ackChan)

	acked := make(map[int]bool)
	for pid := range ackChan {
		acked[pid] = true
	}

	// 4. Кворум считаем ТОЛЬКО по in-sync репликам (ISR). Лидер сам входит в ISR.
	//    Реплики, которые ещё догоняют лог, не учитываются в кворуме, но новые
	//    сообщения им всё равно отправляются.
	isrTotal := 1 // лидер
	isrAcked := 1 // лидер записывает локально ниже
	for _, replica := range replicas {
		if replica.Id == leaderID || !replica.InSync {
			continue
		}
		isrTotal++
		if acked[replica.Id] {
			isrAcked++
		}
	}

	if isrAcked < isrTotal {
		return 0, fmt.Errorf("replication failed: ISR quorum broken (got %d/%d in-sync acks)", isrAcked, isrTotal)
	}

	// 5. Локальный коммит на лидере (физическая запись в Append-Only файл).
	// Используем AppendEntry (не PushQueue), чтобы offset и timestamp совпадали
	// 1-в-1 с тем, что уже разослано репликам на шаге 3.
	finalOffset, err := partProcessor.AppendEntry(targetOffset, timestamp, payload)
	if err != nil {
		// Реплики уже приняли запись, а у лидера её нет. Откатываем её на
		// репликах, иначе их лог уйдёт на одну запись вперёд лога лидера и
		// все последующие push'ы будут отклоняться как PushRejectedGap.
		tp.invalidateReplicas(pushed, partitionID, targetOffset, leaderID, clusterNet)

		return 0, fmt.Errorf("leader local commit failed: %w", err)
	}

	return finalOffset, nil
}
