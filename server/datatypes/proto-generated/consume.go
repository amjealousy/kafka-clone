package proto_generated

import (
	"google.golang.org/protobuf/proto"
)

// Encode собирает ConsumePayload в бинарный сплошной буфер
func (c *ConsumePayload) Encode() []byte {
	data, err := proto.Marshal(c)
	if err != nil {
		return nil
	}
	return data
}

func (c *ConsumePayload) Decode(payload []byte) error {
	return proto.Unmarshal(payload, c)
}

type ConsumerFunc func(*ConsumePayload)
