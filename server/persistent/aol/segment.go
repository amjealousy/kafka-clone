package aol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"kafka-clone/server/topic"
	"log/slog"
	"os"
	"path/filepath"
)

type Segment struct {
	File        *os.File
	Path        string
	BaseOffset  uint64
	NextOffset  uint64
	index       map[uint64]int64 // Смещение в памяти: Offset -> Позиция в файле (в байтах)
	CurrentSize int64
}

// NewSegment открывает существующий или создает новый файл сегмента лога
func NewSegment(dir string, baseOffset uint64) (*Segment, error) {
	fileName := fmt.Sprintf("%020d.log", baseOffset)
	path := filepath.Join(dir, fileName)

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	seg := &Segment{
		File:        file,
		Path:        path,
		BaseOffset:  baseOffset,
		NextOffset:  baseOffset,
		index:       make(map[uint64]int64),
		CurrentSize: stat.Size(),
	}

	// Если файл уже существовал и содержит данные, восстанавливаем индекс в памяти
	if seg.CurrentSize > 0 {
		if err := seg.recoverIndex(); err != nil {
			slog.Error("recover index error:", err)
			file.Close()
			return nil, err
		}
	}

	return seg, nil
}

// Append записывает сообщение в бинарном виде в конец файла сегмента
func (s *Segment) Append(offset uint64, timestamp int64, payload []byte) (int, error) {
	valLen := len(payload)
	// Бинарный формат записи: [4 байта: размер тела] [8 байт: offset] [8 байт: timestamp] [N байт: payload]
	msgBodySize := uint32(8 + 8 + valLen)
	totalMsgSize := 4 + msgBodySize

	buf := make([]byte, totalMsgSize)
	binary.BigEndian.PutUint32(buf[0:4], msgBodySize)
	binary.BigEndian.PutUint64(buf[4:12], offset)
	binary.BigEndian.PutUint64(buf[12:20], uint64(timestamp))
	copy(buf[20:], payload)

	// Запоминаем позицию начала сообщения для индекса
	pos := s.CurrentSize

	n, err := s.File.Write(buf)
	if err != nil {
		// Частичная запись (например, диск заполнился на середине буфера):
		// подрезаем файл обратно до последней целой записи, чтобы не оставить
		// "хвост" полу-записанного сообщения, который мог бы сбить recoverIndex
		// при следующем старте. recoverIndex тоже умеет такое подчищать сам,
		// но лучше не полагаться только на восстановление после рестарта.
		if n > 0 {
			_ = s.File.Truncate(pos)
		}
		return 0, err
	}

	// fsync: обеспечиваем, что запись реально дошла до диска, а не осталась
	// только в буфере ОС — иначе при падении/потере питания "успешно
	// записанное" сообщение может быть потеряно.
	if err := s.File.Sync(); err != nil {
		return 0, err
	}

	s.index[offset] = pos
	s.CurrentSize += int64(n)
	s.NextOffset = offset + 1

	return n, nil
}

// TruncateFrom физически удаляет из сегмента все записи с оффсетом >= offset:
// файл обрезается ровно до байтовой позиции первой удаляемой записи, а из
// in-memory индекса убираются соответствующие ключи. Используется для отката
// записи, которую лидер разослал репликам, но сам закоммитить не смог
// (см. PartitionProcessor.InvalidateOffset).
//
// Вызывающий обязан синхронизировать доступ к сегменту (в нашем случае —
// PartitionProcessor держит p.mx на запись).
func (s *Segment) TruncateFrom(offset uint64) error {
	// Записей с таким оффсетом в сегменте нет — откатывать нечего.
	if offset >= s.NextOffset {
		return nil
	}

	if offset < s.BaseOffset {
		return fmt.Errorf("truncate offset %d is below segment base offset %d", offset, s.BaseOffset)
	}

	pos, exists := s.index[offset]
	if !exists {
		return fmt.Errorf("offset %d not found in segment %s", offset, s.Path)
	}

	// Обрезаем по пути, а не через s.File.Truncate: файл открыт с O_APPEND, и
	// на Windows усечение такого дескриптора запрещено (Access is denied).
	// Сегменты никогда не переименовываются, поэтому путь однозначно указывает
	// на тот же файл. Дозапись через открытый дескриптор после этого
	// продолжается уже с новой (укороченной) позиции конца файла.
	if err := os.Truncate(s.Path, pos); err != nil {
		return err
	}
	// fsync: обрезание должно пережить падение процесса, иначе после рестарта
	// recoverIndex снова увидит откаченную запись как валидную.
	if err := s.File.Sync(); err != nil {
		return err
	}

	for o := range s.index {
		if o >= offset {
			delete(s.index, o)
		}
	}

	s.CurrentSize = pos
	s.NextOffset = offset
	return nil
}

// Read считывает конкретное сообщение по оффсету с помощью thread-safe системного вызова ReadAt
func (s *Segment) Read(offset uint64) (*topic.Message, error) {
	pos, exists := s.index[offset]
	if !exists {
		return nil, errors.New("offset not found in this segment")
	}

	// Сначала читаем 4 байта размера тела сообщения
	sizeBuf := make([]byte, 4)
	if _, err := s.File.ReadAt(sizeBuf, pos); err != nil {
		return nil, err
	}
	msgBodySize := binary.BigEndian.Uint32(sizeBuf)

	// Читаем все тело сообщения целиком
	bodyBuf := make([]byte, msgBodySize)
	if _, err := s.File.ReadAt(bodyBuf, pos+4); err != nil {
		return nil, err
	}

	msgOffset := binary.BigEndian.Uint64(bodyBuf[0:8])
	timestamp := int64(binary.BigEndian.Uint64(bodyBuf[8:16]))
	payload := bodyBuf[16:]

	return &topic.Message{
		Offset:    msgOffset,
		Timestamp: timestamp,
		Payload:   payload,
	}, nil
}

// recoverIndex парсит файл с начала до конца для восстановления карты индексов
// в RAM при старте. Если процесс упал посреди записи последнего сообщения
// (например, при обрыве питания), в хвосте файла может оказаться неполная
// запись — она НЕ добавляется в индекс, а сам файл обрезается ровно до
// границы последней целой записи, чтобы последующие Append'ы продолжались
// строго последовательно и без "дыр"/мусора в логе.
func (s *Segment) recoverIndex() error {
	var currentPos int64 = 0

	for currentPos < s.CurrentSize {
		// 1. Читаем 4-байтовый заголовок размера тела записи.
		sizeBuf := make([]byte, 4)
		n, err := s.File.ReadAt(sizeBuf, currentPos)
		if n < len(sizeBuf) {
			// Даже 4 байта заголовка размера не дописаны — обрубленная
			// запись в самом начале, останавливаемся здесь (обрежем ниже).
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return err
			}
			break
		}
		msgBodySize := binary.BigEndian.Uint32(sizeBuf)
		totalRecordSize := int64(4) + int64(msgBodySize)

		// 2. Если заявленный размер записи выходит за пределы фактического
		// размера файла — запись была прервана посреди записи тела/payload.
		if currentPos+totalRecordSize > s.CurrentSize {
			break
		}

		// 3. Читаем оффсет сообщения (первые 8 байт тела) для индекса.
		offsetBuf := make([]byte, 8)
		if n, err := s.File.ReadAt(offsetBuf, currentPos+4); n < len(offsetBuf) {
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return err
			}
			break
		}
		msgOffset := binary.BigEndian.Uint64(offsetBuf)

		s.index[msgOffset] = currentPos
		s.NextOffset = msgOffset + 1

		// Шагаем к следующей бинарной записи
		currentPos += totalRecordSize
	}

	// Если мы остановились раньше конца файла — значит хвост файла содержит
	// незавершённую (полу-записанную при крахе) запись. Отбрасываем её.
	if currentPos < s.CurrentSize {
		if err := s.File.Truncate(currentPos); err != nil {
			return err
		}
		s.CurrentSize = currentPos
	}

	return nil
}

func (s *Segment) Close() error {
	return s.File.Close()
}
