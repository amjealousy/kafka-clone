package broker

import (
	"context"
	"errors"
	"fmt"
	"kafka-clone/server/datatypes"
	"kafka-clone/server/internal"
	"kafka-clone/server/persistent/db"
	"kafka-clone/server/topic"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type TopicsConfig struct {
	lst []topic.Topic
}

type ClusterPeerProvider interface {
	GetGrpcClient(peerID int) (datatypes.ReplicationServiceClient, error) // Возвращает gRPC-клиент для слейва
	GetEpoch() int64
	GetNodeId() int
	GetOffsetCommitChan() chan OffsetCommit
}
type OffsetCommit struct {
	offset    int64
	topic     string
	partition int32
}

type Broker struct {
	*internal.Lifecycle
	id       int
	config   *TopicsConfig
	clientDB *db.MongoClient
	log      *slog.Logger
	pool     *ProcessorPool

	wg sync.WaitGroup
	mu sync.Mutex // Защищает b.config при изменении оффсетов

	//cluster parameters:
	leaderctx          context.Context
	leaderModecancel   context.CancelFunc
	followerCtx        context.Context
	followerModecancel context.CancelFunc
	epoch              atomic.Int64
	role               datatypes.ClusterRole
	peersMx            *sync.RWMutex
	peers              map[int]*datatypes.Peer
	etcdclient         *clientv3.Client
}

func (b *Broker) GetOffsetCommitChan() chan OffsetCommit {
	//TODO implement me
	panic("implement me")
}

func (b *Broker) GetEpoch() int64 {
	return b.epoch.Load()
}

func NewBroker(id int, dbClient *db.MongoClient, logger *slog.Logger, node context.Context, client *clientv3.Client) *Broker {

	lctx, lcancel := context.WithCancel(context.Background())
	fctx, fcancel := context.WithCancel(context.Background())
	b := &Broker{

		id: id,
		config: &TopicsConfig{
			lst: make([]topic.Topic, 0),
		},
		clientDB:           dbClient,
		log:                logger,
		Lifecycle:          internal.DeriveLifecycle(node),
		peers:              make(map[int]*datatypes.Peer, 0),
		etcdclient:         client,
		leaderctx:          lctx,
		leaderModecancel:   lcancel,
		followerCtx:        fctx,
		followerModecancel: fcancel,
	}
	pool := NewProcessorPool(b.Context(), logger)
	// todo this part exist `cause configuration API does not exist, remove when it is
	t := topic.Topic{
		Id:   1,
		Name: "test-topic",
		Partitions: append([]*topic.Partition{}, &topic.Partition{
			Id:          1,
			Retention:   time.Hour,
			StartOffset: 0,
		}),
	}
	t.Partitions[0].AddReplica(topic.Replica{Id: 1, InSync: true})

	pool.ConfigureTopic(t)
	b.pool = pool

	return b
}

func (b *Broker) GetGrpcClient(peerID int) (datatypes.ReplicationServiceClient, error) {
	b.peersMx.RLock()
	defer b.peersMx.RUnlock()

	peer, exists := b.peers[peerID]
	if !exists || peer.ReplicationClient == nil {
		return nil, fmt.Errorf("no active gRPC connection for broker %d", peerID)
	}
	return peer.ReplicationClient, nil
}

func (b *Broker) Shutdown(ctx context.Context) error {
	b.log.Info("initiating graceful shutdown...")

	// 1. Отменяем контекст брокера (сигнал всем воркерам завершаться)
	b.Stop()

	// 2. Ждем, пока все фоновые горутины (клиенты, воркеры) завершат работу
	b.log.Info("waiting for active tasks to complete...")

	// Создаем канал для ожидания WaitGroup
	wgDone := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(wgDone)
	}()

	// Ждем либо завершения всех горутин, либо таймаута, переданного извне
	select {
	case <-wgDone:
		b.log.Info("all active tasks finished")
	case <-ctx.Done():
		b.log.Warn("shutdown timeout reached, forcing state flush")
	}

	// 3. Сбрасываем (flush) состояние топиков в MongoDB
	b.log.Info("flushing topics state to mongodb...")
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, t := range b.config.lst {
		b.log.Debug("saving topic state", "name", t.Name, "current_offset", t.StartOffset)
		if err := b.clientDB.UpdateTopic(ctx, t); err != nil {
			b.log.Error("failed to save topic state during shutdown", "topic", t.Name, "error", err)
			// Продолжаем сохранять остальные топики, даже если один упал
		}
	}

	b.log.Info("graceful shutdown successfully completed")
	return nil
}
func (b *Broker) HandleCommand(ctx *TCPContext, body []byte) error {
	defer ctx.Close()
	b.log.Info("handling command", "command", ctx.Header.CommandType)

	switch ctx.Header.CommandType {
	case datatypes.Topic:
		//todo
		payload := &datatypes.TopicPayload{}

		err := b.topicHandler(ctx, payload)
		if errors.Is(err, &internal.FollowerModeError{}) {
			copy(ctx.buf.Reply, "Find cluster leader "+err.Error())
		}
		_ = copy(ctx.buf.Reply, "Topic handler not allowed")
		return nil
	case datatypes.Produce:
		payload := &datatypes.ProducePayload{}
		if err := datatypes.DecodeKafkaBody(body, payload); err != nil {
			return err
		} else {
			b.log.Debug("Kafka body", slog.Any("payload", payload))
			return b.producerHandler(ctx, payload)
		}
	case datatypes.Consume:
		payload := &datatypes.ConsumePayload{}
		if err := datatypes.DecodeKafkaBody(body, payload); err != nil {
			return err
		} else {
			b.log.Debug("Kafka body", slog.Any("payload", payload))
			return b.consumerHandler(ctx, payload)

		}
	default:
		return errors.New("unknown command")
	}
}

func (b *Broker) InitConfig(ctx context.Context) error {
	b.log.Info("loading topics configuration from mongodb...")

	// Запрашиваем данные из нашего обернутого клиента Mongo
	dbTopics, err := b.clientDB.FetchAllTopics(ctx)
	if err != nil {
		b.log.Error("failed to load topics from database", "error", err)
		return err
	}

	// Перекладываем данные из слайса в map (TopicsConfig) для быстрого доступа по O(1)
	for i, t := range dbTopics {
		b.config.lst[i] = t
		b.log.Debug("topic loaded into broker memory",
			"name", t.Name,
			"retention", t.Retention,
			"start_offset", t.StartOffset,
		)
	}

	b.log.Info("successfully loaded topics configuration", "count", len(b.config.lst))
	return nil
}

func (b *Broker) producerHandler(tctx *TCPContext, body *datatypes.ProducePayload) error {
	if body.TopicName == "" {
		b.log.Error("topic name is empty")
		s := "topic name is empty"
		response := datatypes.ProduceResponse{Status: datatypes.KafkaStatus_Error,
			StatusMessage: &s}
		encode, err2 := tctx.Encode(&response)
		if err2 != nil {
			return err2
		}
		err := tctx.Write(encode)
		if err != nil {
			b.log.Error("failed to write to topic", "topic", tctx.Header.CommandType, "error", err)
			return err
		}
		return nil

	}
	if err := b.pool.SendMessage(b.Context(), body.TopicName, body.PartitionId, body.Msg, b); err != nil {
		return err
	}
	response := datatypes.ProduceResponse{Status: datatypes.KafkaStatus_Accepted,
		StatusMessage: nil}
	encode, err2 := tctx.Encode(&response)
	if err2 != nil {
		return err2
	}
	err := tctx.Write(encode)
	return err

}

func (b *Broker) topicHandler(tctx *TCPContext, body *datatypes.TopicPayload) error {
	select {
	case <-b.followerCtx.Done():
		// todo Main logic
	default:
		if b.role == datatypes.Follower {
			err := &internal.FollowerModeError{}
			return &internal.RestrictedOpError{Wrapped: err}
		}
		return nil

	}

}

func (b *Broker) consumerHandler(tctx *TCPContext, body *datatypes.ConsumePayload) error {
	if body.TopicName == "" {
		return errors.New("topic name is empty")
	}
	var start, fin topic.Offset
	switch x := body.StartPosition.(type) {
	case *datatypes.ConsumePayload_StartOffset:
		start = topic.Offset{Value: x.StartOffset}
	case *datatypes.ConsumePayload_FromBeginning:
		start = topic.Offset{Tag: topic.FromBeginning}
	}
	switch x := body.GetFinPosition().(type) {
	case *datatypes.ConsumePayload_FinOffset:
		fin = topic.Offset{Value: x.FinOffset}
	case *datatypes.ConsumePayload_TillEnd:
		fin = topic.Offset{Tag: topic.TillEnd}
	}

	err, arrMsg, streamC := b.pool.ReadMessages(b.Context(), body.TopicName, int(body.PartitionID), start, fin)
	if err != nil {
		return err
	}
	if len(arrMsg) > 0 {
		responseList := &datatypes.ConsumeResponseList{
			Responses: make([]*datatypes.ConsumeResponse, 0, len(arrMsg)),
		}
		for _, msg := range arrMsg {
			unpackedMsg := &datatypes.ConsumeResponse{
				Timestamp: msg.Timestamp,
				Offset:    msg.Offset,
				Msg:       msg.Payload,
			}
			responseList.Responses = append(responseList.Responses, unpackedMsg)
		}
		encode, err := tctx.Encode(responseList)
		if err != nil {
			b.log.Error("Failed to marshal via Append", "error", err)
			return err
		}
		err = tctx.Write(encode)
		if err != nil {
			b.log.Error("Failed to write via Append", "error", err)
			return err
		}
	}

	if streamC != nil {
		b.log.Info("Client entered live-streaming log tailing mode", "topic", body.TopicName, "fromOffset", body.GetStartOffset())
		for {
			select {
			case <-b.Done(): // Если сработал Graceful Shutdown брокера,  закрываем сетевую сессию
				return nil
			case msg, ok := <-streamC:
				if !ok {
					return nil
				}

				responseList := &datatypes.ConsumeResponseList{
					Responses: []*datatypes.ConsumeResponse{
						{
							Timestamp: msg.Timestamp,
							Offset:    msg.Offset,
							Msg:       msg.Payload,
						},
					},
				}

				encode, err := tctx.Encode(responseList)
				if err != nil {
					b.log.Error("Failed to marshal live stream message via Protobuf", "error", err)
					return err
				}

				err = tctx.Write(encode)
				if err != nil {
					// Если у клиента оборвалась сеть, tctx.Write вернет ошибку.
					// Мы выходим из обработчика горутины, канал streamChan закроется автоматически
					b.log.Warn("Live stream consumer disconnected unexpectedly", "error", err)
					return err
				}
			}
		}
	}
	return nil
}

func (b *Broker) ReplicationLogHandler(ctx context.Context, req *datatypes.AppendEntriesRequest) (*datatypes.AppendEntriesResponse, error) {
	tp, err := b.pool.GetTopicProcessor(req.TopicName)
	if err != nil {
		return &datatypes.AppendEntriesResponse{Success: false}, nil
	}

	err, partProcessor := tp.GetPartition(int(req.PartitionId))
	if err != nil {
		return &datatypes.AppendEntriesResponse{Success: false}, nil
	}

	// Блокируем партицию на запись (Фолловер тоже должен писать потокобезопасно)
	partProcessor.mx.Lock()
	defer partProcessor.mx.Unlock()

	// Проверка: лог должен быть строго последовательным
	if req.TargetOffset != partProcessor.nextOffset {
		return &datatypes.AppendEntriesResponse{
			Success:     false,
			MatchOffset: partProcessor.nextOffset,
		}, nil
	}

	// Пишем чистые байты в файл сегмента Слейва
	finalOffset := partProcessor.PushQueue(req.Payload)
	return &datatypes.AppendEntriesResponse{
		Success:     true,
		MatchOffset: finalOffset,
	}, nil
}
func (b *Broker) SetEpoch(num int64) error {
	b.log.Info("setting epoch", "num", num)
	_ = b.epoch.Swap(num)

	return nil
}
func (b *Broker) SetNodeId(node int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.id = node
}
func (b *Broker) GetNodeId() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.id
}
func (b *Broker) ToController() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.followerModecancel()

	b.leaderctx, b.leaderModecancel = context.WithCancel(context.Background())
	b.role = datatypes.Controller
	return nil
}
func (b *Broker) ToFollower() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.leaderModecancel()
	b.followerCtx, b.followerModecancel = context.WithCancel(context.Background())
	b.role = datatypes.Follower
	return nil
}
func (b *Broker) ReadRole() datatypes.ClusterRole {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.role
}

func (b *Broker) RestoreData(ctx context.Context, i int64) error {

}

func (b *Broker) AddPeers(peers ...*datatypes.Peer) error {

}
