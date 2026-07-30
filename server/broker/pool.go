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

// EnsureTopic возвращает процессор топика, создавая его при отсутствии.
func (pool *ProcessorPool) EnsureTopic(t topic.Topic) *TopicProcessor {
	pool.mx.Lock()
	defer pool.mx.Unlock()
	if id, ok := pool.topicProcessor[t.Name]; ok && id >= 0 && pool.processors[id] != nil {
		return pool.processors[id]
	}
	pool.topicProcessor[t.Name] = pool.nextId
	processor := NewTopicProcessor(pool.nextId, t.Name, pool.log, t.Partitions, pool.Context())
	pool.processors = append(pool.processors, processor)
	pool.log.Info("Ensured topic", "name", t.Name)
	pool.nextId++
	return processor
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

// ListTopics возвращает имена всех активных локальных топиков.
func (pool *ProcessorPool) ListTopics() []string {
	pool.mx.RLock()
	defer pool.mx.RUnlock()
	out := make([]string, 0, len(pool.topicProcessor))
	for name, id := range pool.topicProcessor {
		if id >= 0 && pool.processors[id] != nil {
			out = append(out, name)
		}
	}
	return out
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
