package broker

import (
	"context"
	"errors"
	"kafka-clone/server/topic"
	"log/slog"
	"sync"
	"time"
)

type TopicProcessor struct {
	id         int
	topicName  string
	mx         *sync.RWMutex
	baseOffset uint64
	nextOffset uint64
	messages   []topic.Message
	ctx        context.Context
	cancelF    context.CancelFunc
	ttl        time.Duration
	log        *slog.Logger
}

func NewTopicProcessor(id int, topicName string, offset uint64, log *slog.Logger, ttl time.Duration) *TopicProcessor {
	mx := new(sync.RWMutex)
	ctx, cancelF := context.WithCancel(context.Background())
	log.With("topic", topicName)
	tp := &TopicProcessor{
		id:         id,
		baseOffset: offset,
		nextOffset: offset,
		mx:         mx,
		topicName:  topicName,
		messages:   make([]topic.Message, 0),
		ctx:        ctx,
		cancelF:    cancelF,
		ttl:        ttl,
		log:        log,
	}
	go func() {
		tp.RunCleanWorker()
	}()
	return tp
}

func (p *TopicProcessor) RunCleanWorker() {
	period := time.NewTicker(10 * time.Second)
	p.log.Info("RunCleanWorker start", "topic-partition", p.topicName)
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-period.C:
			p.log.Info("RunCleanWorker triggered", "topic-partition", p.topicName)
			p.CleanOldMessages(p.ttl)
		}
	}
}

func (p *TopicProcessor) PushQueue(m []byte) uint64 {
	p.mx.Lock()         // Блокируем всё для записи
	defer p.mx.Unlock() //

	offset := p.nextOffset
	msg := topic.Message{
		Offset:    offset,
		Payload:   m,
		Timestamp: time.Now().UnixNano(),
	}
	p.log.Info("Message to topic", "offset", offset, "length", len(m))
	p.messages = append(p.messages, msg)
	p.nextOffset++
	p.log.Info("Added to topic", "topic", p.messages, "length", len(p.messages))
	return offset
}
func (p *TopicProcessor) readFrom(startOffset uint64, maxMessages int) (error, []topic.Message) {
	p.mx.RLock()
	defer p.mx.RUnlock()
	// Если консьюмер просит оффсет, который мы уже удалили
	if startOffset < p.baseOffset {
		return errors.New("requested offset was already deleted (out of range)"), nil
	}

	if startOffset >= p.nextOffset {
		return errors.New("no message for startoffset"), nil // Новых сообщений пока нет
	}

	sliceStartIdx := startOffset - p.baseOffset

	endIdx := int(sliceStartIdx) + maxMessages
	if endIdx > len(p.messages) {
		endIdx = len(p.messages)
	}

	result := make([]topic.Message, endIdx-int(sliceStartIdx))
	copy(result, p.messages[sliceStartIdx:endIdx])

	return nil, result
}
func (p *TopicProcessor) GetLastOffset() uint64 {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return p.nextOffset - 1
}
func (p *TopicProcessor) GetStartOffset() uint64 {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return p.baseOffset
}

func (p *TopicProcessor) CleanOldMessages(ttl time.Duration) {
	p.mx.Lock()
	defer p.mx.Unlock()

	now := time.Now().UnixNano()
	cutoffTime := now - ttl.Nanoseconds()
	deleteCount := 0
	for _, msg := range p.messages {
		if msg.Timestamp < cutoffTime {
			deleteCount++
		} else {
			break
		}
	}
	if deleteCount == 0 {
		return
	}

	// Сдвигаем базовый оффсет
	p.baseOffset += uint64(deleteCount)

	// Отрезаем старые сообщения из слайса
	// Чтобы помочь сборщику мусора  освободить память, можно занулить удаляемую часть
	for i := 0; i < deleteCount; i++ {
		p.messages[i] = topic.Message{} // Очищаем память, если там были указатели
	}
	p.messages = p.messages[deleteCount:]
}

type ProcessorPool struct {
	wg             sync.WaitGroup //todo: use when proccessor will be async
	mx             sync.RWMutex
	topicProcessor map[string]int
	processors     []*TopicProcessor
	nextId         int
	log            *slog.Logger
}

func NewProcessorPool(logger *slog.Logger) *ProcessorPool {
	logger.With("component", "ProcessorPool")

	return &ProcessorPool{
		topicProcessor: make(map[string]int),
		mx:             sync.RWMutex{},
		wg:             sync.WaitGroup{},
		processors:     make([]*TopicProcessor, 0),
		log:            logger,
		nextId:         0,
	}
}
func (pool *ProcessorPool) AddTopic(topic topic.Topic) error {

	pool.mx.Lock()

	defer pool.mx.Unlock()
	if _, ok := pool.topicProcessor[topic.Name]; ok {
		return errors.New("topic already exists")
	}

	pool.topicProcessor[topic.Name] = pool.nextId
	pool.log.Info("TopicProcessor", slog.Any("tp", pool.topicProcessor))
	processor := NewTopicProcessor(pool.nextId, topic.Name, topic.StartOffset, pool.log, topic.Retention)
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
		pool.log.Info("GetTopicProcessor", slog.Int("id", id))
		return new(TopicProcessor), errors.New("topic does not exist")
	}
	pool.log.Info("GetTopicProcessor", slog.Int("id", id))
	pool.log.Info("GetTopicProcessor length", slog.Int("len", len(pool.processors)))
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
func (pool *ProcessorPool) SendMessage(topic string, message []byte) error {
	if processor, err := pool.GetTopicProcessor(topic); err != nil {
		return err
	} else {
		processor.PushQueue(message)
		return nil
	}
}
func (pool *ProcessorPool) ReadMessages(topic string, start, till uint64, flags ...bool) (error, []topic.Message) {

	if processor, err := pool.GetTopicProcessor(topic); err != nil {
		return err, nil
	} else {
		if flags != nil {
			for id, flag := range flags {
				if id == 0 && flag {
					start = processor.GetStartOffset()
				}
				if id == 1 && flag {
					till = processor.GetLastOffset()
				}

			}

		}
		return processor.readFrom(start, int(till-start))

	}
}
