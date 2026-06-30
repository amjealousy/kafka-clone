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
	pool.AddTopic(t)
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
func (b *Broker) HandleCommand(cmd datatypes.Command, body []byte, respBuff []byte) (error, int) {
	b.log.Info("handling command", "command", cmd)
	switch cmd {
	case datatypes.Topic:
		b.topicHander(body)
		i := copy(respBuff, "Topic handler not allowed")
		return nil, i
	case datatypes.Produce:
		payload := &datatypes.ProducePayload{}
		if err := datatypes.DecodeKafkaBody(body, payload); err != nil {
			return err, 0
		} else {
			b.log.Info("Kafka body", slog.Any("payload", payload))
			handlerErr, wlen := b.producerHandler(payload, respBuff)
			return handlerErr, wlen

		}
	case datatypes.Consume:
		b.consumerHandler(body)
		i := copy(respBuff, "Consume handler not allowed")
		return nil, i
	default:
		return errors.New("unknown command"), 0
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

func (b *Broker) producerHandler(body *datatypes.ProducePayload, resp []byte) (error, int) {
	if body.TopicName() == "" {
		return errors.New("topic name is empty"), 0

	}
	if err := b.pool.SendMessage(body.TopicName(), body.Msg()); err != nil {
		return err, copy(resp, "Not Ok")
	}
	return nil, copy(resp, "Ok")

}

func (b *Broker) consumerHandler(body []byte) {

}
