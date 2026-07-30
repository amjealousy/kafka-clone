package broker

import (
	"context"
	"errors"
	"fmt"
	"kafka-clone/server/datatypes"

	"kafka-clone/server/datatypes/encode"
	gen "kafka-clone/server/datatypes/proto-generated"
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
	GetGrpcClient(peerID int) (gen.ReplicationServiceClient, error) // Возвращает gRPC-клиент для слейва
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

	offsetCommitChan chan OffsetCommit
	// partitionLeader хранит id ноды-лидера для каждой (topic, partition),
	// чтобы отстающая реплика знала, у кого выкачивать лог.
	partitionLeaderMx sync.RWMutex
	partitionLeader   map[string]int // ключ "topic/partition" -> leaderNodeID
}

// partitionLeaderKey формирует ключ для карты partitionLeader.
func partitionLeaderKey(topicName string, partitionID int) string {
	return fmt.Sprintf("%s/%d", topicName, partitionID)
}

// SetPartitionLeader запоминает лидера партиции (данные из etcd).
func (b *Broker) SetPartitionLeader(topicName string, partitionID int, leaderNodeID int) {
	b.partitionLeaderMx.Lock()
	defer b.partitionLeaderMx.Unlock()
	b.partitionLeader[partitionLeaderKey(topicName, partitionID)] = leaderNodeID
}

// GetPartitionLeader возвращает id лидера партиции и флаг наличия записи.
func (b *Broker) GetPartitionLeader(topicName string, partitionID int) (int, bool) {
	b.partitionLeaderMx.RLock()
	defer b.partitionLeaderMx.RUnlock()
	id, ok := b.partitionLeader[partitionLeaderKey(topicName, partitionID)]
	return id, ok
}

func (b *Broker) GetOffsetCommitChan() chan OffsetCommit {
	return b.offsetCommitChan
}

// RunOffsetCommitConsumer вычитывает подтверждённые (закоммиченные) оффсеты,
// которые лидер публикует после успешной репликации, и сбрасывает их наружу
// (в лог/etcd). Запускается как фоновый воркер.

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
		peersMx:            &sync.RWMutex{},
		peers:              make(map[int]*datatypes.Peer, 0),
		etcdclient:         client,
		leaderctx:          lctx,
		leaderModecancel:   lcancel,
		followerCtx:        fctx,
		followerModecancel: fcancel,
		offsetCommitChan:   make(chan OffsetCommit, 1024),
		partitionLeader:    make(map[string]int),
	}
	pool := NewProcessorPool(b.Context(), logger)
	b.pool = pool

	return b
}

func (b *Broker) GetGrpcClient(peerID int) (gen.ReplicationServiceClient, error) {
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
		b.log.Debug("saving topic state", "name", t.Name, "partitions", len(t.Partitions))
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
	case encode.Topic:
		// Управление топиками (создание/описание) больше НЕ обслуживается по
		// TCP-протоколу — эта административная логика перенесена в отдельный
		// gRPC control-plane API (ControlService.DescribeTopic/CreateTopic,
		// см. cluster.NodeCoordinator). Здесь только вежливо сообщаем об этом.
		return b.topicHandler(ctx)
	case encode.Produce:
		payload := &gen.ProducePayload{}
		if err := encode.DecodeKafkaBody(body, payload); err != nil {
			return err
		} else {
			b.log.Debug("Kafka body", slog.Any("payload", payload))
			return b.producerHandler(ctx, payload)
		}
	case encode.Consume:
		payload := &gen.ConsumePayload{}
		if err := encode.DecodeKafkaBody(body, payload); err != nil {
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

	// Перекладываем данные в конфиг брокера
	b.config.lst = append(b.config.lst[:0], dbTopics...)
	for _, t := range dbTopics {
		b.log.Debug("topic loaded into broker memory",
			"name", t.Name,
			"partitions", len(t.Partitions),
		)
	}

	b.log.Info("successfully loaded topics configuration", "count", len(b.config.lst))
	return nil
}

func (b *Broker) producerHandler(tctx *TCPContext, body *gen.ProducePayload) error {
	if body.TopicName == "" {
		b.log.Error("topic name is empty")
		s := "topic name is empty"
		response := gen.ProduceResponse{Status: gen.KafkaStatus_Error,
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

	partitionID := int(body.PartitionId)

	// Писать разрешено ТОЛЬКО лидеру партиции. Клиент должен был узнать адрес
	// лидера заранее через ControlService.DescribeTopic; если он всё же
	// постучался не туда (устаревшие метаданные, гонка после failover) —
	// честно отказываем, а не пишем данные не туда, куда рассчитывает клиент.
	if !b.IsPartitionLeader(body.TopicName, partitionID) {
		b.log.Warn("rejected produce: not partition leader", "topic", body.TopicName, "partition", partitionID)
		s := fmt.Sprintf("node is not the leader for %s/%d; call ControlService.DescribeTopic to find the leader",
			body.TopicName, partitionID)
		response := gen.ProduceResponse{Status: gen.KafkaStatus_Error, StatusMessage: &s}
		encode, err2 := tctx.Encode(&response)
		if err2 != nil {
			return err2
		}
		return tctx.Write(encode)
	}

	if err := b.pool.SendMessage(b.Context(), body.TopicName, partitionID, body.Msg, b); err != nil {
		return err
	}
	response := gen.ProduceResponse{Status: gen.KafkaStatus_Accepted,
		StatusMessage: nil}
	encode, err2 := tctx.Encode(&response)
	if err2 != nil {
		return err2
	}
	err := tctx.Write(encode)
	return err

}

// topicHandler больше не выполняет никакой административной логики: создание
// и описание топиков перенесено в отдельный gRPC control-plane API
// (ControlService, см. cluster.NodeCoordinator.CreateTopic/DescribeTopic).
// TCP-обработчик оставлен только для того, чтобы клиенты со старым протоколом
// получили понятный ответ, куда обращаться дальше, вместо тишины/зависания.
func (b *Broker) topicHandler(tctx *TCPContext) error {
	response := gen.TopicResponse{
		Status: "topic management has moved to the ControlService gRPC control-plane API " +
			"(DescribeTopic/CreateTopic); this TCP command is no longer supported",
	}
	encode, err := tctx.Encode(&response)
	if err != nil {
		return err
	}
	return tctx.Write(encode)
}

// IsPartitionLeader сообщает, является ли эта нода лидером указанной
// партиции — именно это разрешает обрабатывать produce-запросы (запись).
func (b *Broker) IsPartitionLeader(topicName string, partitionID int) bool {
	leaderID, ok := b.GetPartitionLeader(topicName, partitionID)
	return ok && leaderID == b.GetNodeId()
}

// IsPartitionInSync сообщает, является ли локальная копия партиции in-sync —
// именно это разрешает обрабатывать consume-запросы (чтение). Реплика,
// которая ещё восстанавливает лог (StateReplicating), обслуживать чтение не
// может: клиент увидел бы неполный/устаревший лог.
func (b *Broker) IsPartitionInSync(topicName string, partitionID int) bool {
	tp, err := b.pool.GetTopicProcessor(topicName)
	if err != nil {
		return false
	}
	err, pp := tp.GetPartition(partitionID)
	if err != nil {
		return false
	}
	return pp.SyncState() == StateInSync
}

func (b *Broker) consumerHandler(tctx *TCPContext, body *gen.ConsumePayload) error {
	if body.TopicName == "" {
		return errors.New("topic name is empty")
	}

	partitionID := int(body.PartitionID)

	// Читать разрешено ТОЛЬКО с in-sync реплики (включая лидера — он всегда
	// in-sync сам с собой). Реплика, которая ещё восстанавливает лог
	// (StateReplicating), не может отдавать consume — клиент увидел бы
	// неполный лог. Клиент должен был узнать адрес in-sync реплики заранее
	// через ControlService.DescribeTopic.
	if !b.IsPartitionInSync(body.TopicName, partitionID) {
		b.log.Warn("rejected consume: not an in-sync replica", "topic", body.TopicName, "partition", partitionID)
		errMsg := fmt.Sprintf("node is not an in-sync replica for %s/%d; call ControlService.DescribeTopic to find an in-sync node",
			body.TopicName, partitionID)
		responseList := &gen.ConsumeResponseList{Error: &errMsg}
		encode, err := tctx.Encode(responseList)
		if err != nil {
			return err
		}
		return tctx.Write(encode)
	}

	var start, fin topic.Offset
	switch x := body.StartPosition.(type) {
	case *gen.ConsumePayload_StartOffset:
		start = topic.Offset{Value: x.StartOffset}
	case *gen.ConsumePayload_FromBeginning:
		start = topic.Offset{Tag: topic.FromBeginning}
	}
	switch x := body.GetFinPosition().(type) {
	case *gen.ConsumePayload_FinOffset:
		fin = topic.Offset{Value: x.FinOffset}
	case *gen.ConsumePayload_TillEnd:
		fin = topic.Offset{Tag: topic.TillEnd}
	}

	err, arrMsg, streamC := b.pool.ReadMessages(b.Context(), body.TopicName, partitionID, start, fin)
	if err != nil {
		return err
	}
	if len(arrMsg) > 0 {
		responseList := &gen.ConsumeResponseList{
			Responses: make([]*gen.ConsumeResponse, 0, len(arrMsg)),
		}
		for _, msg := range arrMsg {
			unpackedMsg := &gen.ConsumeResponse{
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

				responseList := &gen.ConsumeResponseList{
					Responses: []*gen.ConsumeResponse{
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

// ReplicationLogHandler обрабатывает входящий push (AppendEntries) от лидера
// партиции. Вся логика проверки состояния + последовательности + записи
// сосредоточена в PartitionProcessor.TryApplyPush — единой atomically-locked
// точке входа, чтобы гарантированно исключить конкурентную запись с catch-up
// процессом (см. комментарий к TryApplyPush/TryFinalizeInSync).
func (b *Broker) ReplicationLogHandler(ctx context.Context, req *gen.AppendEntriesRequest) (*gen.AppendEntriesResponse, error) {
	tp, err := b.pool.GetTopicProcessor(req.TopicName)
	if err != nil {
		return &gen.AppendEntriesResponse{Success: false}, nil
	}

	err, partProcessor := tp.GetPartition(int(req.PartitionId))
	if err != nil {
		return &gen.AppendEntriesResponse{Success: false}, nil
	}

	outcome, matchOffset := partProcessor.TryApplyPush(req.TargetOffset, req.Timestamp, req.Payload)
	switch outcome {
	case PushAccepted:
		return &gen.AppendEntriesResponse{Success: true, MatchOffset: matchOffset}, nil
	case PushRejectedNotInSync:
		b.log.Debug("rejected push: partition still catching up",
			"topic", req.TopicName, "partition", req.PartitionId, "matchOffset", matchOffset)
		return &gen.AppendEntriesResponse{Success: false, NotInSync: true, MatchOffset: matchOffset}, nil
	default: // PushRejectedGap
		return &gen.AppendEntriesResponse{Success: false, MatchOffset: matchOffset}, nil
	}
}

// FetchLogHandler отдаёт отстающей реплике пакет записей лога начиная с
// запрошенного оффсета. Вызывается на ноде-источнике (лидере партиции).
func (b *Broker) FetchLogHandler(ctx context.Context, req *gen.FetchLogRequest) (*gen.FetchLogResponse, error) {
	tp, err := b.pool.GetTopicProcessor(req.TopicName)
	if err != nil {
		return &gen.FetchLogResponse{Leader: b.role == datatypes.Controller}, nil
	}
	err, partProcessor := tp.GetPartition(int(req.PartitionId))
	if err != nil {
		return &gen.FetchLogResponse{Leader: b.role == datatypes.Controller}, nil
	}

	msgs, hw, readErr := partProcessor.ReadBatch(req.FromOffset, int(req.MaxMessages))
	if readErr != nil {
		b.log.Error("FetchLog read error", "topic", req.TopicName, "partition", req.PartitionId, "error", readErr)
	}

	entries := make([]*gen.LogEntry, 0, len(msgs))
	for _, m := range msgs {
		entries = append(entries, &gen.LogEntry{
			Offset:    m.Offset,
			Timestamp: m.Timestamp,
			Payload:   m.Payload,
		})
	}

	return &gen.FetchLogResponse{
		Entries:       entries,
		HighWatermark: hw,
		Leader:        b.role == datatypes.Controller,
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

// RestoreData проходит по всем локальным партициям и догоняет их лог до
// актуального состояния лидера. Вызывается при старте ноды и при восстановлении
// после потери сессии etcd. Параметр fallbackLeader используется, если лидер
// конкретной партиции неизвестен из etcd-метаданных.
func (b *Broker) RestoreData(ctx context.Context, fallbackLeader int64) error {
	topics := b.pool.ListTopics()
	for _, topicName := range topics {
		tp, err := b.pool.GetTopicProcessor(topicName)
		if err != nil {
			continue
		}
		for _, partID := range tp.ListPartitions() {
			leaderID, ok := b.GetPartitionLeader(topicName, partID)
			if !ok || leaderID == 0 {
				leaderID = int(fallbackLeader)
			}
			// Лидер сам себя не догоняет.
			if leaderID == 0 || leaderID == b.GetNodeId() {
				continue
			}
			if err := b.CatchUpPartition(ctx, topicName, partID, leaderID); err != nil {
				b.log.Error("catch-up failed", "topic", topicName, "partition", partID, "leader", leaderID, "error", err)
				return err
			}
		}
	}
	return nil
}

// CatchUpPartition выкачивает недостающий лог партиции у лидера (batch FetchLog)
// и применяет его локально, пока партиция не переходит в StateInSync.
//
// Пока эта функция работает, партиция находится в StateReplicating — push-путь
// (TryApplyPush) целиком отклоняет входящие AppendEntries, поэтому единственный
// писатель в лог всё это время — именно эта горутина (AppendEntry). Это и есть
// то самое "избежание конкурентной записи не по порядку", о котором речь:
// вместо того чтобы пытаться аккуратно чередовать push и pull-восстановление,
// мы полностью запрещаем push, пока restore не завершится.
//
// Атомарность перехода в in-sync достигается через TryFinalizeInSync: как
// только очередной батч применён, мы В ТОЙ ЖЕ функции (тот же вызов, тот же
// стек) под partition-локом сравниваем nextOffset с HW, полученным В ЭТОМ ЖЕ
// FetchLog-ответе. Поскольку никто другой не мог продвинуть nextOffset за это
// время (push заблокирован), переход корректен, даже если HW лидера к этому
// моменту успел уйти ещё дальше — тогда TryFinalizeInSync просто вернёт false,
// и цикл продолжит выкачивать следующий батч с новым (уже бо́льшим) HW.
func (b *Broker) CatchUpPartition(ctx context.Context, topicName string, partitionID int, leaderNodeID int) error {
	tp, err := b.pool.GetTopicProcessor(topicName)
	if err != nil {
		return err
	}
	err, pp := tp.GetPartition(partitionID)
	if err != nil {
		return err
	}

	// Уже in-sync (например, повторный вызов после short-lived session loss,
	// когда локальный лог и так не отстал) — восстанавливать нечего.
	if pp.SyncState() == StateInSync {
		return nil
	}

	client, err := b.GetGrpcClient(leaderNodeID)
	if err != nil {
		return fmt.Errorf("no replication client for leader %d: %w", leaderNodeID, err)
	}

	const batchSize = 500
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fromOffset := pp.GetNextOffset()
		req := &gen.FetchLogRequest{
			TopicName:   topicName,
			PartitionId: uint32(partitionID),
			FromOffset:  fromOffset,
			MaxMessages: batchSize,
		}

		rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := client.FetchLog(rpcCtx, req)
		cancel()
		if err != nil {
			return fmt.Errorf("FetchLog gRPC failed: %w", err)
		}

		for _, entry := range resp.Entries {
			if _, err := pp.AppendEntry(entry.Offset, entry.Timestamp, entry.Payload); err != nil {
				return fmt.Errorf("failed to apply fetched entry offset %d: %w", entry.Offset, err)
			}
		}

		// Пытаемся атомарно завершить catch-up относительно HW ИЗ ЭТОГО ответа.
		if pp.TryFinalizeInSync(resp.HighWatermark) {
			b.log.Info("partition caught up with leader and is now IN-SYNC",
				"topic", topicName, "partition", partitionID,
				"offset", pp.GetNextOffset(), "leader", leaderNodeID)
			return nil
		}

		// Лидер продолжает производить новые сообщения быстрее, чем мы успели
		// вычитать батч — HW ушёл вперёд, читаем следующий батч. Если пачка
		// оказалась пустой (нечего было выкачивать, но HW всё равно вырос
		// уже ПОСЛЕ формирования этого ответа), просто идём на новую итерацию.
	}
}

// AddPeers регистрирует gRPC-соединения с другими нодами кластера для репликации.
func (b *Broker) AddPeers(peers ...*datatypes.Peer) error {
	b.peersMx.Lock()
	defer b.peersMx.Unlock()
	for _, p := range peers {
		if p == nil {
			continue
		}
		b.peers[p.ID] = p
		b.log.Info("registered cluster peer", "peerID", p.ID, "addr", p.Addr)
	}
	return nil
}

// RemovePeer закрывает и удаляет соединение с нодой кластера.
func (b *Broker) RemovePeer(peerID int) {
	b.peersMx.Lock()
	defer b.peersMx.Unlock()
	if p, ok := b.peers[peerID]; ok {
		if p.GrpcConn != nil {
			_ = p.GrpcConn.Close()
		}
		delete(b.peers, peerID)
		b.log.Info("removed cluster peer", "peerID", peerID)
	}
}

// EnsureLocalPartition создаёт локально топик (если его нет) и партицию с
// заданным набором реплик. Возвращает процессор партиции и флаг created
// (была ли партиция создана именно сейчас, а не найдена уже существующей).
// При первом создании (created==true) состояние партиции персистится в
// MongoDB как "манифест" — чтобы при рестарте нода знала, какие
// topic/partition она когда-то у себя разместила (см. LoadLocalPartitionsFromMongo).
func (b *Broker) EnsureLocalPartition(ctx context.Context, topicName string, part *topic.Partition) (*PartitionProcessor, bool, error) {
	tp := b.pool.EnsureTopic(topic.Topic{Name: topicName, Partitions: nil})
	pp, created := tp.EnsurePartition(part)

	if created {
		rec := db.LocalPartitionRecord{
			NodeID:      b.GetNodeId(),
			Topic:       topicName,
			PartitionID: part.Id,
			RetentionNs: int64(part.Retention),
		}
		if err := b.clientDB.UpsertLocalPartition(ctx, rec); err != nil {
			// Не фатально: партиция уже создана и рабочая локально, просто при
			// следующем перезапуске ноде придётся заново узнать о ней из etcd.
			b.log.Error("failed to persist local partition manifest to mongodb",
				"topic", topicName, "partition", part.Id, "error", err)
		}
	}

	return pp, created, nil
}

// LoadLocalPartitionsFromMongo читает манифест локально размещённых на этой
// ноде topic/partition (сохранённый ранее через EnsureLocalPartition) и
// заново создаёт под них TopicProcessor/PartitionProcessor. Восстановление
// сегментов/оффсетов с диска происходит автоматически внутри
// NewPartitionProcessor -> recoverSegments(). Вызывается один раз при старте,
// до подключения к остальному кластеру, чтобы уже имеющиеся локальные данные
// были готовы и их offset можно было сразу сообщить в etcd.
func (b *Broker) LoadLocalPartitionsFromMongo(ctx context.Context) error {

	records, err := b.clientDB.ListLocalPartitions(ctx, b.GetNodeId())
	if err != nil {

		return fmt.Errorf("failed to load local partitions manifest: %w", err)
	}

	for _, rec := range records {
		tp := b.pool.EnsureTopic(topic.Topic{Name: rec.Topic})
		part := &topic.Partition{
			Id:        rec.PartitionID,
			Retention: time.Duration(rec.RetentionNs),
		}
		if _, created := tp.EnsurePartition(part); created {
			b.log.Info("restored local partition from mongodb manifest",
				"topic", rec.Topic, "partition", rec.PartitionID)
		}
	}
	return nil
}

// LocalPartitionOffset — снимок (topic, partition, offset) для одной локально
// имеющейся партиции, используемый для само-репорта в etcd.
type LocalPartitionOffset struct {
	Topic       string
	PartitionID int
	Offset      uint64
}

// ListLocalPartitionOffsets возвращает срез (topic, partition, offset) для
// ВСЕХ локально существующих партиций — вне зависимости от того, назначена
// ли эта нода на них прямо сейчас в etcd. Именно эта "избыточная" видимость
// (в т.ч. по уже не назначенным партициям) и позволяет контроллеру находить
// ноды с "остаточными" данными и приоритетно переиспользовать их.
func (b *Broker) ListLocalPartitionOffsets() []LocalPartitionOffset {
	var out []LocalPartitionOffset
	for _, topicName := range b.pool.ListTopics() {
		tp, err := b.pool.GetTopicProcessor(topicName)
		if err != nil {
			continue
		}
		for _, partID := range tp.ListPartitions() {
			err, pp := tp.GetPartition(partID)
			if err != nil {
				continue
			}
			out = append(out, LocalPartitionOffset{
				Topic:       topicName,
				PartitionID: partID,
				Offset:      pp.GetNextOffset(),
			})
		}
	}
	return out
}

// MarkPartitionReplicaInSync локально помечает реплику этой ноды как in-sync
// в списке реплик партиции (используется для отображения/учёта; фактическое
// разрешение принимать push регулируется PartitionProcessor.syncState через
// ForceInSync/TryFinalizeInSync).
func (b *Broker) MarkPartitionReplicaInSync(topicName string, partitionID int, replicaID int) {
	tp, err := b.pool.GetTopicProcessor(topicName)
	if err != nil {
		return
	}
	err, pp := tp.GetPartition(partitionID)
	if err != nil {
		return
	}
	pp.SetReplicaInSync(replicaID, true)
}
