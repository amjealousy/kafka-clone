package broker

import (
	"context"
	"errors"
	"fmt"
	"kafka-clone/server/internal"
	"kafka-clone/server/topic"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type PartitionProcessor struct {
	*internal.Lifecycle
	id        int
	topicName string
	mx        *sync.RWMutex

	activeSegment  *internal.Segment
	closedSegments []*internal.Segment
	cond           *sync.Cond

	baseOffset uint64
	nextOffset uint64

	ttl            time.Duration
	log            *slog.Logger
	maxSegmentSize int64

	inSyncQuota int // how much ACKs we need to receive and replicate`s factor of message
	replicas    []topic.Replica
}

func NewPartitionProcessor(ctx context.Context, id int, topicName string, offset uint64, log *slog.Logger, ttl time.Duration, r []topic.Replica) *PartitionProcessor {
	mx := new(sync.RWMutex)
	log = log.With("topic", topicName).With("component", "[PartitionProcessor]").With("partId", id)

	tp := &PartitionProcessor{
		id:             id,
		baseOffset:     offset,
		nextOffset:     offset,
		mx:             mx,
		cond:           sync.NewCond(mx), // Инициализируем Cond, привязанный к нашему RWMutex
		topicName:      topicName,
		closedSegments: make([]*internal.Segment, 0),
		Lifecycle:      internal.DeriveLifecycle(ctx),
		ttl:            ttl,
		log:            log,
		maxSegmentSize: 10 * 1024 * 1024, // 10 МБ лимит на один файл-сегмент
		replicas:       r,
	}

	if err := tp.recoverSegments(); err != nil {
		log.Error("Failed to recover segments from disk", "error", err)
	}

	go func() {
		tp.RunCleanWorker()
	}()
	return tp
}
func (p *PartitionProcessor) GetId() int {
	return p.id
}
func (p *PartitionProcessor) recoverSegments() error {
	dirPath := filepath.Join("data", p.topicName)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return err
	}

	files, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	var logFiles []string
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".log") {
			logFiles = append(logFiles, f.Name())
		}
	}
	sort.Strings(logFiles)

	for _, fileName := range logFiles {
		var baseOffset uint64
		_, err := fmt.Sscanf(fileName, "%020d.log", &baseOffset)
		if err != nil {
			continue
		}

		seg, err := internal.NewSegment(dirPath, baseOffset)
		if err != nil {
			return err
		}
		p.closedSegments = append(p.closedSegments, seg)
	}

	if len(p.closedSegments) > 0 {
		lastIdx := len(p.closedSegments) - 1
		p.activeSegment = p.closedSegments[lastIdx]
		p.closedSegments = p.closedSegments[:lastIdx]
		p.nextOffset = p.activeSegment.NextOffset
	} else {
		seg, err := internal.NewSegment(dirPath, p.baseOffset)
		if err != nil {
			return err
		}
		p.activeSegment = seg
		p.nextOffset = p.baseOffset
	}

	if len(p.closedSegments) > 0 {
		p.baseOffset = p.closedSegments[0].BaseOffset
	} else {
		p.baseOffset = p.activeSegment.BaseOffset
	}

	return nil
}

func (p *PartitionProcessor) RunCleanWorker() {
	period := time.NewTicker(10 * time.Second)
	p.log.Info("RunCleanWorker start", "topic-partition", p.topicName)
	for {
		select {
		case <-p.Done():
			return
		case <-period.C:
			p.log.Info("RunCleanWorker triggered", "topic-partition", p.topicName)
			p.CleanOldMessages(p.ttl)
		}
	}
}

// PushQueue сохраняет Msg (чистые байты из Protobuf) в активный файловый сегмент
func (p *PartitionProcessor) PushQueue(m []byte) uint64 {
	p.mx.Lock()
	defer p.mx.Unlock()

	offset := p.nextOffset
	timestamp := time.Now().UnixNano()

	// Логика ротации сегментов
	if p.activeSegment.CurrentSize >= p.maxSegmentSize {
		p.log.Info("Rotating active segment due to size limit", "size", p.activeSegment.CurrentSize)
		p.closedSegments = append(p.closedSegments, p.activeSegment)

		dirPath := filepath.Join("data", p.topicName)
		newSeg, err := internal.NewSegment(dirPath, offset)
		if err != nil {
			p.log.Error("Critical error creating new log segment", "error", err)
			return offset
		}
		p.activeSegment = newSeg
	}

	_, err := p.activeSegment.Append(offset, timestamp, m)
	if err != nil {
		p.log.Error("Failed to append message to segment file", "error", err)
		return offset
	}

	p.nextOffset++

	// Оповещаем все горутины, застрявшие на чтении "конца файла" в getStream
	p.cond.Broadcast()
	return offset
}
func (p *PartitionProcessor) findSegment(offset uint64) *internal.Segment {
	for _, seg := range p.closedSegments {
		if offset >= seg.BaseOffset && offset < seg.NextOffset {
			return seg
		}
	}
	if p.activeSegment != nil && offset >= p.activeSegment.BaseOffset && offset < p.activeSegment.NextOffset {
		return p.activeSegment
	}
	return nil
}

func (p *PartitionProcessor) readFrom(ctx context.Context, startOffset uint64, maxMessages int, isStream bool) (error, []topic.Message, <-chan topic.Message) {
	p.mx.RLock()
	if startOffset < p.baseOffset {
		p.mx.RUnlock()
		return errors.New("requested offset was already deleted (out of range)"), nil, nil
	}

	// Если это запрос на стриминг, вычитываем историческую часть вплоть до текущего p.nextOffset
	endOffset := startOffset + uint64(maxMessages)
	if isStream {
		endOffset = p.nextOffset
	}
	p.mx.RUnlock()

	var result []topic.Message
	currentOffset := startOffset

	p.mx.RLock()
	for currentOffset < endOffset && currentOffset < p.nextOffset {
		if maxMessages > 0 && len(result) >= maxMessages && !isStream {
			break
		}
		seg := p.findSegment(currentOffset)
		if seg == nil {
			break
		}
		msg, err := seg.Read(currentOffset)
		if err != nil {
			p.log.Error("Error reading message from file segment", "offset", currentOffset, "error", err)
			break
		}
		result = append(result, *msg)
		currentOffset++
	}
	p.mx.RUnlock()

	var streamChan <-chan topic.Message
	if isStream {
		// Запускаем бесконечный стриминг с оффсета, на котором закончилась историческая пачка
		streamChan = p.getStream(ctx, currentOffset)
	}

	return nil, result, streamChan
}

func (p *PartitionProcessor) getStream(ctx context.Context, startOffset uint64) <-chan topic.Message {
	out := make(chan topic.Message, 100)

	go func() {
		defer close(out)
		currentOffset := startOffset

		for {
			select {
			case <-ctx.Done():
				return
			case <-p.Done():
				return
			default:
			}

			p.mx.Lock()
			// Если мы вычитали все сообщения до конца, засыпаем на условной переменной
			for currentOffset >= p.nextOffset {
				if ctx.Err() != nil {
					p.mx.Unlock()
					return
				}
				// cond.Wait атомарно отпускает p.mx и блокирует горутину.
				// При просыпании (через Broadcast) она снова захватывает p.mx.
				p.cond.Wait()
			}

			// Динамически определяем, в каком сегменте сейчас находится наш оффсет.
			// Если activeSegment за это время успел ротироваться, findSegment вернет новый файл прозрачно!
			seg := p.findSegment(currentOffset)
			var msg *topic.Message
			var err error
			if seg != nil {
				msg, err = seg.Read(currentOffset)
			} else {
				// Защита: если Retention воркер удалил сегмент прямо из-под нашего стрима
				if currentOffset < p.baseOffset {
					currentOffset = p.baseOffset
					p.mx.Unlock()
					continue
				}
			}
			p.mx.Unlock()

			if err != nil {
				p.log.Error("Stream error reading message from segment", "offset", currentOffset, "error", err)
				time.Sleep(10 * time.Millisecond)
				continue
			}

			if msg != nil {
				select {
				case <-ctx.Done():
					return
				case <-p.Done():
					return
				case out <- *msg:
					currentOffset++ // Шагаем к следующему оффсету
				}
			} else {
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	return out
}

func (p *PartitionProcessor) GetLastOffset() uint64 {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return p.nextOffset - 1
}
func (p *PartitionProcessor) GetStartOffset() uint64 {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return p.baseOffset
}

func (p *PartitionProcessor) CleanOldMessages(ttl time.Duration) {

	cutoffTime := time.Now().UnixNano() - ttl.Nanoseconds()
	var remainingClosed []*internal.Segment
	p.mx.Lock()
	defer p.mx.Unlock()
	for _, seg := range p.closedSegments {
		stat, err := seg.File.Stat()
		if err != nil {
			remainingClosed = append(remainingClosed, seg)
			continue
		}

		if stat.ModTime().UnixNano() < cutoffTime {
			seg.Close()
			if err := os.Remove(seg.Path); err != nil {
				p.log.Error("Failed to delete expired log segment", "path", seg.Path, "error", err)
				remainingClosed = append(remainingClosed, seg)
			} else {
				p.log.Info("Successfully deleted expired log segment file", "path", seg.Path)
			}
		} else {
			remainingClosed = append(remainingClosed, seg)
		}
	}

	p.closedSegments = remainingClosed

	if len(p.closedSegments) > 0 {
		p.baseOffset = p.closedSegments[0].BaseOffset
	} else {
		p.baseOffset = p.activeSegment.BaseOffset
	}
}
func (p *PartitionProcessor) GetReplicationFactor() int {
	return p.inSyncQuota
}

func (p *PartitionProcessor) GetReplicas() []topic.Replica {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return p.replicas
}
