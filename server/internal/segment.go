package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"kafka-clone/server/topic"
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
		return 0, err
	}

	s.index[offset] = pos
	s.CurrentSize += int64(n)
	s.NextOffset = offset + 1

	return n, nil
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

// recoverIndex парсит файл с начала до конца для восстановления карты индексов в RAM при старте
func (s *Segment) recoverIndex() error {
	var currentPos int64 = 0

	for currentPos < s.CurrentSize {
		sizeBuf := make([]byte, 4)
		if _, err := s.File.ReadAt(sizeBuf, currentPos); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		msgBodySize := binary.BigEndian.Uint32(sizeBuf)

		// Читаем оффсет сообщения (первые 8 байт тела)
		offsetBuf := make([]byte, 8)
		if _, err := s.File.ReadAt(offsetBuf, currentPos+4); err != nil {
			return err
		}
		msgOffset := binary.BigEndian.Uint64(offsetBuf)

		s.index[msgOffset] = currentPos
		s.NextOffset = msgOffset + 1

		// Шагаем к следующей бинарной записи
		currentPos += 4 + int64(msgBodySize)
	}

	return nil
}

func (s *Segment) Close() error {
	return s.File.Close()
}
