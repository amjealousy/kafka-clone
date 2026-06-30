package datatypes

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
