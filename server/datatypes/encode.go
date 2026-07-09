package datatypes

import "log/slog"

type KafkaHeader struct {
	MessageSize   uint32
	CorrelationID uint32
	CommandType   Command
}

type Command = uint32

const (
	Topic Command = iota
	Produce
	Consume
)

type KafkaMessage[T Payload] struct {
	Header KafkaHeader
	Body   T
}

type Payload interface {
	Encode() []byte
	Decode([]byte) error
}

func DecodeKafkaBody[T Payload](payload []byte, dest T) error {

	if err := dest.Decode(payload); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}
