package broker

import (
	"context"
	"errors"
	"fmt"
	"kafka-clone/server/datatypes"
	"kafka-clone/server/internal"
	"kafka-clone/server/topic"
	"log/slog"
	"sync"
	"time"
)

type TopicProcessor struct {
	*internal.Lifecycle
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

		Lifecycle: internal.DeriveLifecycle(poolCtx),
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

	// Захватываем локальный лок партиции. Другие партиции этого топика работают параллельно
	partProcessor.mx.Lock()
	defer partProcessor.mx.Unlock()

	// 2. Вытягиваем метаданные распределенной партиции из etcd
	replicas := partProcessor.GetReplicas()

	targetOffset := partProcessor.GetLastOffset()
	var wg sync.WaitGroup
	ackChan := make(chan int, len(replicas))

	// 3. Параллельный стриминг реплики по gRPC на все ноды из ISR
	for _, peerID := range replicas {
		if peerID.Id == partProcessor.GetId() {
			continue // Себя пропускаем, запишем на диск Лидера позже
		}

		// Запрашиваем типизированный gRPC-клиент у Брокера через интерфейс
		client, err := clusterNet.GetGrpcClient(peerID.Id)
		if err != nil {
			tp.log.Error("Failed to get gRPC replication client", "peerID", peerID, "error", err)
			continue
		}

		wg.Add(1)
		go func(pid int, cl datatypes.ReplicationServiceClient) {
			defer wg.Done()

			// Ограничиваем сетевой вызов к слейву в 1 секунду
			rpcCtx, rpcCancel := context.WithTimeout(mergedCtx, 1*time.Second)
			defer rpcCancel()

			req := &datatypes.AppendEntriesRequest{
				Term:         uint64(clusterNet.GetEpoch()), // Эпоха лидера из etcd
				LeaderId:     int32(partProcessor.GetId()),
				TopicName:    tp.Name,
				PartitionId:  uint32(partitionID),
				TargetOffset: targetOffset,
				Payload:      payload,
			}

			res, err := cl.AppendEntries(rpcCtx, req)
			if err != nil {
				tp.log.Error("gRPC replication call failed", "slavePeer", pid, "error", err)
				return
			}

			if res.Success {
				ackChan <- pid
			} else {
				tp.log.Warn("Slave rejected log append", "slavePeer", pid, "matchOffset", res.MatchOffset)
			}
		}(peerID.Id, client)
	}

	wg.Wait()
	close(ackChan)

	// Считаем подтверждения (Лидер уже в зачете)
	successCount := 1
	for range ackChan {
		successCount++
	}

	// Строгая гарантия: коммитим только если ВСЕ живые ноды из ISR подтвердили прием лога
	if successCount < len(replicas) {
		return 0, fmt.Errorf("replication failed: ISR quorum broken (got %d/%d updates)", successCount, len(replicas))
	}

	// 4. Локальный коммит на Лидере (физическая запись в Append-Only файл)
	finalOffset := partProcessor.PushQueue(payload)

	// 5. Асинхронное слияние (сброс) оффсета в etcd через канал оптимизатора партиции
	select {
	case clusterNet.GetOffsetCommitChan() <- OffsetCommit{offset: int64(finalOffset), topic: tp.Name, partition: int32(partProcessor.id)}:
	default:
		// Если канал переполнен, оффсет запишется со следующей итерацией тикера
	}

	return finalOffset, nil
}
