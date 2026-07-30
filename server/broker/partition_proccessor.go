package broker

import (
	"context"
	"errors"
	"fmt"
	"kafka-clone/server/internal"
	"kafka-clone/server/persistent/aol"
	"kafka-clone/server/topic"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SyncState описывает готовность ЛОКАЛЬНОЙ партиции принимать пуш-репликацию
// от лидера.
//
//   - StateReplicating: партиция ещё не восстановила лог до high watermark
//     лидера (либо только что создана по назначению контроллера, либо
//     переживает пере-подключение). В этом состоянии партиция НЕ принимает
//     AppendEntries push от лидера — единственный писатель в этот период —
//     сам процесс catch-up (FetchLog + AppendEntry). Это гарантирует, что
//     сегменты лога никогда не получат запись не по порядку: пока идёт
//     восстановление, никто другой не может продвинуть nextOffset.
//   - StateInSync: лог полностью восстановлен (nextOffset догнал HW лидера
//     на момент перехода). С этого момента разрешён обычный push-путь.
type SyncState int32

const (
	StateReplicating SyncState = iota
	StateInSync
)

func (s SyncState) String() string {
	if s == StateInSync {
		return "IN_SYNC"
	}
	return "REPLICATING"
}

type PartitionState int

const (
	PartitionActive PartitionState = iota
	PartitionStopping
	PartitionDeleted
)

type PartitionProcessor struct {
	*internal.Lifecycle
	id        int
	topicName string
	mx        *sync.RWMutex

	activeSegment  *aol.Segment
	closedSegments []*aol.Segment
	cond           *sync.Cond

	baseOffset uint64
	nextOffset uint64

	ttl            time.Duration
	log            *slog.Logger
	maxSegmentSize int64

	inSyncQuota int // how much ACKs we need to receive and replicate`s factor of message
	replicas    []topic.Replica

	// syncState защищён тем же p.mx, что и все offset-чувствительные поля —
	// поэтому проверка состояния и запись в лог всегда атомарны друг
	// относительно друга (см. TryApplyPush / TryFinalizeInSync).
	syncState SyncState
	state     PartitionState
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
		closedSegments: make([]*aol.Segment, 0),
		Lifecycle:      internal.DeriveLifecycle(ctx),
		ttl:            ttl,
		log:            log,
		maxSegmentSize: 10 * 1024 * 1024, // 10 МБ лимит на один файл-сегмент
		replicas:       r,
		syncState:      StateReplicating, // безопасный дефолт: см. ForceInSync/TryFinalizeInSync
		state:          PartitionActive,
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
	dirPath := filepath.Join("data", p.topicName, strconv.Itoa(p.id))
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

		seg, err := aol.NewSegment(dirPath, baseOffset)
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
		seg, err := aol.NewSegment(dirPath, p.baseOffset)
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
	defer period.Stop()
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

// appendLocked физически дописывает запись (offset, timestamp, payload) в
// активный сегмент, включая ротацию сегмента при превышении лимита размера.
// ВАЖНО: вызывающий обязан удерживать p.mx и сам гарантировать, что offset
// строго равен текущему p.nextOffset — эта функция никаких проверок
// последовательности не делает, это ответственность вызывающего кода
// (PushQueue/AppendEntry/TryApplyPush).
func (p *PartitionProcessor) appendLocked(offset uint64, timestamp int64, payload []byte) error {
	if p.activeSegment.CurrentSize >= p.maxSegmentSize {
		p.log.Info("Rotating active segment due to size limit", "size", p.activeSegment.CurrentSize)
		p.closedSegments = append(p.closedSegments, p.activeSegment)

		dirPath := filepath.Join("data", p.topicName, strconv.Itoa(p.id))
		newSeg, err := aol.NewSegment(dirPath, offset)
		if err != nil {
			return err
		}
		p.activeSegment = newSeg
	}

	if _, err := p.activeSegment.Append(offset, timestamp, payload); err != nil {
		return err
	}

	p.nextOffset++
	// Оповещаем все горутины, застрявшие на чтении "конца файла" в getStream
	p.cond.Broadcast()
	return nil
}

// PushQueue сохраняет Msg (чистые байты из Protobuf) в активный файловый
// сегмент, назначая ему следующий по порядку offset и текущий timestamp.
// Используется ТОЛЬКО для собственной локальной записи лидера партиции —
// лидер всегда в состоянии StateInSync и является единственным источником
// правды по нумерации оффсетов.
func (p *PartitionProcessor) PushQueue(m []byte) uint64 {
	p.mx.Lock()
	defer p.mx.Unlock()

	offset := p.nextOffset
	timestamp := time.Now().UnixNano()
	if err := p.appendLocked(offset, timestamp, m); err != nil {
		p.log.Error("Failed to append message to segment file", "error", err)
	}
	return offset
}
func (p *PartitionProcessor) findSegment(offset uint64) *aol.Segment {
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
	var remainingClosed []*aol.Segment
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
	out := make([]topic.Replica, len(p.replicas))
	copy(out, p.replicas)
	return out
}

// SetReplicas полностью перезаписывает набор реплик партиции (по данным из etcd).
func (p *PartitionProcessor) SetReplicas(replicas []topic.Replica) {
	p.mx.Lock()
	defer p.mx.Unlock()
	p.replicas = replicas
}

// AddReplica добавляет реплику, если её ещё нет. Возвращает true, если добавлена.
func (p *PartitionProcessor) AddReplica(r topic.Replica) bool {
	p.mx.Lock()
	defer p.mx.Unlock()
	for _, existing := range p.replicas {
		if existing.Id == r.Id {
			return false
		}
	}
	p.replicas = append(p.replicas, r)
	return true
}

// SetReplicaInSync помечает реплику как in-sync (true) или отстающую (false).
func (p *PartitionProcessor) SetReplicaInSync(replicaID int, inSync bool) {
	p.mx.Lock()
	defer p.mx.Unlock()
	for i := range p.replicas {
		if p.replicas[i].Id == replicaID {
			p.replicas[i].InSync = inSync
			return
		}
	}
}

var ErrPartitionStopped = errors.New("partition stopped")

// AppendEntry записывает уже готовую запись лога с сохранением её оригинального
// оффсета и timestamp. Используется:
//   - catch-up процессом (FetchLog) — единственным легитимным писателем, пока
//     партиция в состоянии StateReplicating;
//   - лидером для собственного локального коммита после успешной репликации
//     (см. ReplicateAndAppend), чтобы offset/timestamp совпадали 1-в-1 с тем,
//     что было разослано репликам.
//
// Возвращает записанный оффсет и ошибку. Идемпотентна: повторная попытка
// записать уже применённый offset безопасно игнорируется (offset < nextOffset).
func (p *PartitionProcessor) AppendEntry(offset uint64, timestamp int64, payload []byte) (uint64, error) {
	p.mx.Lock()
	defer p.mx.Unlock()
	if p.state != PartitionActive {
		return p.nextOffset, ErrPartitionStopped
	}
	// Идемпотентность: если оффсет уже записан, пропускаем.
	if offset < p.nextOffset {
		return offset, nil
	}
	// Лог должен быть строго последовательным.
	if offset != p.nextOffset {
		return p.nextOffset, fmt.Errorf("non-contiguous append: got offset %d, expected %d", offset, p.nextOffset)
	}

	if err := p.appendLocked(offset, timestamp, payload); err != nil {
		return p.nextOffset, err
	}
	return offset, nil
}

// PushOutcome — результат попытки применить push-репликацию (AppendEntries)
// от лидера на стороне реплики.
type PushOutcome int

const (
	// PushAccepted — запись успешно применена, лог продвинулся на 1 offset.
	PushAccepted PushOutcome = iota
	// PushRejectedNotInSync — партиция ещё восстанавливает лог (StateReplicating)
	// и осознанно не принимает push, чтобы не нарушить последовательность
	// сегментов; данные будут получены через FetchLog в рамках catch-up.
	PushRejectedNotInSync
	// PushRejectedGap — партиция уже in-sync, но присланный offset не совпадает
	// с ожидаемым nextOffset (возможен временный лаг сети) — реплика сообщает
	// лидеру актуальный matchOffset, лидер может решить, что делать дальше.
	PushRejectedGap
)

// TryApplyPush — единая точка входа для push-репликации от лидера. Атомарно
// (под одним p.mx.Lock) проверяет состояние синхронизации И строгую
// последовательность оффсета, и только при обоих условиях пишет запись.
// Это специально СОВМЕЩЕНО в одном locked-блоке, а не в двух отдельных
// проверках снаружи, чтобы исключить гонку между проверкой состояния и
// записью (иначе можно было бы, например, проверить StateInSync, затем
// параллельно словить флип обратно в Replicating и всё равно записать).
func (p *PartitionProcessor) TryApplyPush(targetOffset uint64, timestamp int64, payload []byte) (PushOutcome, uint64) {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.syncState != StateInSync {
		// Пока идёт восстановление лога, единственный писатель — catch-up
		// (AppendEntry вызывается из CatchUpPartition). Пуш от лидера мы
		// сознательно отклоняем целиком, даже если по счастливой случайности
		// targetOffset совпал бы с текущим nextOffset: это устраняет саму
		// возможность второго конкурентного писателя в лог во время
		// восстановления и делает рассуждение о корректности тривиальным.
		return PushRejectedNotInSync, p.nextOffset
	}

	if targetOffset != p.nextOffset {
		return PushRejectedGap, p.nextOffset
	}

	if err := p.appendLocked(targetOffset, timestamp, payload); err != nil {
		p.log.Error("Failed to apply leader push", "offset", targetOffset, "error", err)
		return PushRejectedGap, p.nextOffset
	}
	return PushAccepted, targetOffset
}

// SyncState возвращает текущее состояние синхронизации партиции.
func (p *PartitionProcessor) SyncState() SyncState {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return p.syncState
}

// ForceInSync безусловно переводит партицию в состояние in-sync. Используется
// для партиций, где это состояние гарантировано и без catch-up:
//   - партиция, в которой данная нода — лидер;
//   - партиция, которую etcd уже считает in-sync для данной ноды (например,
//     после быстрого переподключения с сохранившимся локальным логом).
func (p *PartitionProcessor) ForceInSync() {
	p.mx.Lock()
	defer p.mx.Unlock()
	p.syncState = StateInSync
}

// TryFinalizeInSync — атомарная попытка завершить catch-up: если локальный
// nextOffset уже догнал observedHW (high watermark, полученный от лидера в
// последнем ответе FetchLog), партиция переводится в StateInSync и функция
// возвращает true. Иначе состояние не меняется и возвращается false — нужно
// продолжать выкачивать лог.
//
// Атомарность перехода обеспечивается тем, что ЭТА проверка и сам флип состояния
// выполняются под одним и тем же p.mx, которым также защищён push-путь
// (TryApplyPush) — а пока syncState == StateReplicating, push-путь полностью
// заблокирован (PushRejectedNotInSync) и НИКТО, кроме единственной горутины
// catch-up, не может продвинуть nextOffset. Значит, между моментом, когда
// catch-up прочитал observedHW у лидера, и моментом вызова этого метода,
// nextOffset мог измениться ТОЛЬКО за счёт самого catch-up (он однопоточный),
// то есть гонки с посторонним писателем в принципе не существует. Если к
// моменту вызова nextOffset >= observedHW — значит локальный лог содержит
// абсолютно ВСЕ записи, которые были у лидера на момент снятия HW, без пропусков.
// Любые сообщения, произведённые лидером ПОСЛЕ этого момента, будут просто
// приняты уже как обычный push (targetOffset совпадёт с нашим nextOffset).
func (p *PartitionProcessor) TryFinalizeInSync(observedHW uint64) bool {
	p.mx.Lock()
	defer p.mx.Unlock()
	if p.nextOffset < observedHW {
		return false
	}
	p.syncState = StateInSync
	return true
}

// ReadBatch читает до maxMessages записей начиная с fromOffset (потокобезопасно),
// возвращая их вместе с текущим high watermark (nextOffset). Используется
// серверной стороной FetchLog на лидере.
func (p *PartitionProcessor) ReadBatch(fromOffset uint64, maxMessages int) ([]topic.Message, uint64, error) {
	p.mx.RLock()
	defer p.mx.RUnlock()

	if maxMessages <= 0 {
		maxMessages = 500
	}

	result := make([]topic.Message, 0, maxMessages)
	current := fromOffset
	for current < p.nextOffset && len(result) < maxMessages {
		seg := p.findSegment(current)
		if seg == nil {
			break
		}
		msg, err := seg.Read(current)
		if err != nil {
			return result, p.nextOffset, err
		}
		result = append(result, *msg)
		current++
	}
	return result, p.nextOffset, nil
}

// GetNextOffset возвращает текущий next offset (high watermark партиции).
func (p *PartitionProcessor) GetNextOffset() uint64 {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return p.nextOffset
}

func (p *PartitionProcessor) StopAndRemove(force bool) error {
	p.mx.Lock()
	defer p.mx.Unlock()
	defer func() {
		p.state = PartitionDeleted
	}()
	p.Stop()
	p.CloseAllSegments()
	if force {
		dirPath := filepath.Join("data", p.topicName, strconv.Itoa(p.id))
		if err := os.RemoveAll(dirPath); err != nil {
			return err
		}
	}

	return nil
}
func (p *PartitionProcessor) CloseAllSegments() {
	p.activeSegment.Close()
	for i := range p.closedSegments {
		p.closedSegments[i].Close()
	}
}
