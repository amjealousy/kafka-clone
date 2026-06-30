package datatypes

import (
	"encoding/binary"
	"io"
	"log/slog"
)

type ProducePayload struct {
	topicName string
	key       string
	msg       []byte
}

func (p *ProducePayload) TopicName() string {
	return p.topicName
}
func (p *ProducePayload) Key() string {
	return p.key
}
func (p *ProducePayload) Msg() []byte {
	return p.msg
}

func (b *ProducePayload) Encode() []byte {
	topicLen := len(b.topicName)
	keyLen := len(b.key)
	msgLen := len(b.msg)

	// Вычисляем точный размер буфера: 3 поля по 4 байта на длину + сами данные
	totalLen := 4 + topicLen + 4 + keyLen + 4 + msgLen
	buf := make([]byte, totalLen)

	// 1. Кодируем topicName
	binary.BigEndian.PutUint32(buf[0:4], uint32(topicLen))
	copy(buf[4:4+topicLen], b.topicName)

	// 2. Кодируем key
	offset := 4 + topicLen
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(keyLen))
	copy(buf[offset+4:offset+4+keyLen], b.key)

	// 3. Кодируем msg
	offset += 4 + keyLen
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(msgLen))
	copy(buf[offset+4:offset+4+msgLen], b.msg)

	return buf
}

// Decode восстанавливает поля структуры из сырых байт (необходим для работы DecodeKafkaBody)
func (b *ProducePayload) Decode(msg []byte) error {
	if len(msg) < 4 {
		return io.ErrUnexpectedEOF
	}

	// 1. Читаем topicName
	topicLen := binary.BigEndian.Uint32(msg[0:4])
	if uint32(len(msg)) < 4+topicLen {
		return io.ErrUnexpectedEOF
	}
	b.topicName = string(msg[4 : 4+topicLen])

	// 2. Читаем key
	offset := 4 + topicLen
	if uint32(len(msg)) < offset+4 {
		return io.ErrUnexpectedEOF
	}
	keyLen := binary.BigEndian.Uint32(msg[offset : offset+4])
	if uint32(len(msg)) < offset+4+keyLen {
		return io.ErrUnexpectedEOF
	}
	b.key = string(msg[offset+4 : offset+4+keyLen])

	// 3. Читаем msg
	offset += 4 + keyLen
	if uint32(len(msg)) < offset+4 {
		return io.ErrUnexpectedEOF
	}
	msgLen := binary.BigEndian.Uint32(msg[offset : offset+4])
	if uint32(len(msg)) < offset+4+msgLen {
		return io.ErrUnexpectedEOF
	}
	b.msg = make([]byte, msgLen)
	copy(b.msg, msg[offset+4:offset+4+msgLen])

	return nil
}

func DecodeKafkaBody[T Payload](payload []byte, dest T) error {

	if err := dest.Decode(payload); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}
