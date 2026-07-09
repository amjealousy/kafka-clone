package broker

import (
	"context"
	"errors"
	"kafka-clone/server/datatypes"
	"kafka-clone/server/persistent"
	"kafka-clone/server/topic"
	"log/slog"
	"sync"
	"time"
)

type TopicsConfig struct {
	lst []topic.Topic
}

type Broker struct {
	id       int
	config   *TopicsConfig
	clientDB *persistent.MongoClient
	log      *slog.Logger
	pool     *ProcessorPool
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	mu       sync.Mutex // Защищает b.config при изменении оффсетов
}

func New(id int, dbClient *persistent.MongoClient, logger *slog.Logger) *Broker {
	brokerCtx, brokerCancel := context.WithCancel(context.Background())
	pool := NewProcessorPool(logger)
	t := topic.Topic{
		Id:          1,
		Name:        "test-topic",
		Retention:   time.Hour,
		StartOffset: 0,
	}
	pool.AddTopic(t) // todo add handling err when add topic going to be non in-memory
	return &Broker{
		id: id,
		config: &TopicsConfig{
			lst: make([]topic.Topic, 0),
		},
		pool:     pool,
		clientDB: dbClient,
		log:      logger,
		ctx:      brokerCtx,
		cancel:   brokerCancel,
	}
}
func (b *Broker) SetUp() error {
	if b.config.lst == nil {
		return errors.New("no topics configured")
	}

	for _, t := range b.config.lst {
		if err := b.pool.AddTopic(t); err != nil {
			b.log.Error("Error adding topic", "topic", t, "err", err)
		}

	}
	b.log.Info("Initializing topics: ", len(b.pool.processors))
	return nil

}

func (b *Broker) Shutdown(ctx context.Context) error {
	b.log.Info("initiating graceful shutdown...")

	// 1. Отменяем контекст брокера (сигнал всем воркерам завершаться)
	b.cancel()

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
	b.log.Info("handling command", "command", ctx.Header.CommandType)
	switch ctx.Header.CommandType {
	case datatypes.Topic:
		b.topicHander(body)
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

func (b *Broker) topicHander(body []byte) {

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
	if err := b.pool.SendMessage(body.TopicName, body.Msg); err != nil {
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
func extractFlags(body *datatypes.ConsumePayload) []bool {
	var flags []bool
	if body.StartFlag != nil {
		flags = append(flags, *body.StartFlag)
	}
	if body.EndFlag != nil {
		if body.StartFlag == nil {
			// Если startFlag не было, но endFlag есть,
			// нужно сохранить позицию (зависит от вашей бизнес-логики)
			flags = append(flags, false)
		}
		flags = append(flags, *body.EndFlag)
	}
	return flags
}

func (b *Broker) consumerHandler(tctx *TCPContext, body *datatypes.ConsumePayload) error {
	if body.TopicName == "" {
		return errors.New("topic name is empty")
	}

	err, arrMsg := b.pool.ReadMessages(body.TopicName, body.StartOffset, body.FinOffset, extractFlags(body)...)
	if err != nil {
		return err
	}
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
	return nil
}
