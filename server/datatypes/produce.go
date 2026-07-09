package datatypes

import (
	"google.golang.org/protobuf/proto"
)

func (p *ProducePayload) Encode() []byte {
	data, err := proto.Marshal(p)
	if err != nil {
		return nil
	}
	return data
}

func (p *ProducePayload) Decode(msg []byte) error {
	return proto.Unmarshal(msg, p)
}
