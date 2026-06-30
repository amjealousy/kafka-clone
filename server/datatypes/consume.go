package datatypes

import (
	"encoding/binary"
	"errors"
)

type ConsumePayload struct {
	topicName   string
	startOffset int
	finOffset   int
}

func (c *ConsumePayload) TopicName() string { return c.topicName }
func (c *ConsumePayload) StartOffset() int  { return c.startOffset }
func (c *ConsumePayload) FinOffset() int    { return c.finOffset }

// Encode собирает ConsumePayload в бинарный сплошной буфер
func (c *ConsumePayload) Encode() []byte {
	topicLen := len(c.topicName)

	// Вычисляем точный размер:
	// 4 байта (длина строки) + N байт (строка) + 8 байт (startOffset) + 8 байт (finOffset)
	totalLen := 4 + topicLen + 8 + 8
	buf := make([]byte, totalLen)

	// 1. Кодируем topicName
	binary.BigEndian.PutUint32(buf[0:4], uint32(topicLen))
	copy(buf[4:4+topicLen], c.topicName)

	// 2. Кодируем startOffset (явно приводим int к uint64 для сети)
	offset := 4 + topicLen
	binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(c.startOffset))

	// 3. Кодируем finOffset
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(c.finOffset))

	return buf
}

// Decode парсит сырые байты сети в структуру ConsumePayload
func (c *ConsumePayload) Decode(payload []byte) error {
	if len(payload) < 4 {
		return errors.New("payload too short to read topic length")
	}

	// 1. Читаем топик
	topicLen := binary.BigEndian.Uint32(payload[0:4])
	if uint32(len(payload)) < 4+topicLen {
		return errors.New("payload too short for topic name")
	}
	c.topicName = string(payload[4 : 4+topicLen])

	// 2. Читаем startOffset (ожидаем 8 байт из сети)
	offset := 4 + topicLen
	if uint32(len(payload)) < offset+8 {
		return errors.New("payload too short for start offset")
	}
	// Читаем как uint64 и бережно конвертируем обратно в int вашего приложения
	c.startOffset = int(binary.BigEndian.Uint64(payload[offset : offset+8]))

	// 3. Читаем finOffset (ожидаем еще 8 байт)
	offset += 8
	if uint32(len(payload)) < offset+8 {
		return errors.New("payload too short for fin offset")
	}
	c.finOffset = int(binary.BigEndian.Uint64(payload[offset : offset+8]))

	return nil
}
