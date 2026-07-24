package broker

import (
	"context"
	"errors"
	"kafka-clone/server/internal"
	"kafka-clone/server/topic"
	"log/slog"
	"sync"
)

type ProcessorPool struct {
	*internal.Lifecycle
	wg             sync.WaitGroup //todo: use when proccessor will be async
	mx             sync.RWMutex
	topicProcessor map[string]int
	processors     []*TopicProcessor
	nextId         int
	log            *slog.Logger
}

func NewProcessorPool(ctx context.Context, logger *slog.Logger) *ProcessorPool {
	logger.With("component", "ProcessorPool")

	return &ProcessorPool{
		topicProcessor: make(map[string]int),
		mx:             sync.RWMutex{},
		wg:             sync.WaitGroup{},
		processors:     make([]*TopicProcessor, 0),
		log:            logger,
		nextId:         0,
		Lifecycle:      internal.DeriveLifecycle(ctx),
	}
}
func (pool *ProcessorPool) ConfigureTopic(topic topic.Topic) error {
	pool.mx.Lock()
	defer pool.mx.Unlock()
	if _, ok := pool.topicProcessor[topic.Name]; ok {
		return errors.New("topic already exists")
	}

	pool.topicProcessor[topic.Name] = pool.nextId
	processor := NewTopicProcessor(pool.nextId, topic.Name, pool.log, topic.Partitions, pool.Context())
	pool.processors = append(pool.processors, processor)
	pool.log.Info("Added topic", "name", topic.Name)
	pool.nextId++
	return nil
}

func (pool *ProcessorPool) GetTopicProcessor(topic string) (*TopicProcessor, error) {
	pool.mx.RLock()
	defer pool.mx.RUnlock()
	var id int
	var ok bool
	if id, ok = pool.topicProcessor[topic]; !ok {
		return nil, errors.New("topic does not exist")
	}
	return pool.processors[id], nil
}

func (pool *ProcessorPool) RemoveTopic(topic topic.Topic) error {
	pool.mx.Lock()
	defer pool.mx.Unlock()
	var id int
	var ok bool
	if id, ok = pool.topicProcessor[topic.Name]; !ok {
		return errors.New("topic does not exist")
	}

	pool.topicProcessor[topic.Name] = -1
	pool.processors[id] = nil
	return nil
}
func (pool *ProcessorPool) SendMessage(ctx context.Context, topic string, partitionId int, message []byte, clusterNet ClusterPeerProvider) error {

	if processor, err := pool.GetTopicProcessor(topic); err != nil {
		return err
	} else {

		_, err2 := processor.ReplicateAndAppend(ctx, partitionId, message, clusterNet)
		if err2 != nil {
			return err2
		}
		return nil
	}
}
func (pool *ProcessorPool) ReadMessages(ctx context.Context, topic string, partitionId int, start, till topic.Offset) (error, []topic.Message, <-chan topic.Message) {

	if processor, err := pool.GetTopicProcessor(topic); err != nil {
		return err, nil, nil
	} else {
		err, partitionProcessor := processor.GetPartition(partitionId)
		if err != nil {
			return err, nil, nil
		}
		var stream bool
		if start.Tag == "Beginning" {
			start.Value = partitionProcessor.GetStartOffset()

		}
		if till.Tag == "End" {
			till.Value = partitionProcessor.GetLastOffset()
			stream = true
		}
		return partitionProcessor.readFrom(ctx, start.Value, int(till.Value-start.Value), stream)
	}

}
