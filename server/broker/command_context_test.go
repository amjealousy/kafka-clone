package broker

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"kafka-clone/server/datatypes/encode"
	gen "kafka-clone/server/datatypes/proto-generated"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type jsonCommandContext struct {
	command  encode.Command
	response []byte
}

func (c *jsonCommandContext) Context() context.Context {
	return context.Background()
}

func (c *jsonCommandContext) CommandType() encode.Command {
	return c.command
}

func (c *jsonCommandContext) Decode(body []byte, message proto.Message) error {
	return protojson.Unmarshal(body, message)
}

func (c *jsonCommandContext) Respond(message proto.Message) error {
	response, err := protojson.Marshal(message)
	if err != nil {
		return err
	}
	c.response = response
	return nil
}

func TestHandleCommandSupportsJSONContext(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	broker := &Broker{log: logger}
	ctx := &jsonCommandContext{command: encode.Produce}

	if err := broker.HandleCommand(ctx, []byte(`{"topicName":""}`)); err != nil {
		t.Fatalf("HandleCommand returned error: %v", err)
	}

	response := &gen.ProduceResponse{}
	if err := protojson.Unmarshal(ctx.response, response); err != nil {
		t.Fatalf("response is not valid protobuf JSON: %v", err)
	}
	if response.GetStatus() != gen.KafkaStatus_Error || response.GetStatusMessage() != "topic name is empty" {
		t.Fatalf("unexpected response: %v", response)
	}
}
