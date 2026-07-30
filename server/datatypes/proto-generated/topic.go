package proto_generated

import (
	"google.golang.org/protobuf/proto"
)

func (p *TopicPayload) Encode() []byte {
	data, err := proto.Marshal(p)
	if err != nil {
		return nil
	}
	return data
}

func (p *TopicPayload) Decode(msg []byte) error {
	return proto.Unmarshal(msg, p)
}
