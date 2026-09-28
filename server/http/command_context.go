package http

import (
	"bufio"
	"context"
	"errors"
	"sync"
	"time"

	brokerAPI "kafka-clone/server/datatypes/broker"
	"kafka-clone/server/datatypes/encode"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type protobufResponder func(proto.Message) error

// JSONCommandContext декодирует protobuf JSON и передаёт сериализацию ответа
// HTTP-адаптеру. Для обычного запроса responder пишет JSON, для consume — SSE.
type JSONCommandContext struct {
	ctx       context.Context
	command   encode.Command
	responder protobufResponder
}

var _ brokerAPI.CommandContext = (*JSONCommandContext)(nil)

func newJSONCommandContext(ctx context.Context, command encode.Command, responder protobufResponder) *JSONCommandContext {
	return &JSONCommandContext{ctx: ctx, command: command, responder: responder}
}

func (c *JSONCommandContext) Context() context.Context {
	return c.ctx
}

func (c *JSONCommandContext) CommandType() encode.Command {
	return c.command
}

func (c *JSONCommandContext) Decode(body []byte, message proto.Message) error {
	return protojson.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(body, message)
}

func (c *JSONCommandContext) Respond(message proto.Message) error {
	if c.responder == nil {
		return errors.New("JSON command responder is not configured")
	}
	return c.responder(message)
}

func marshalProtoJSON(message proto.Message) ([]byte, error) {
	return protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(message)
}

type sseResponder struct {
	writer *bufio.Writer
	cancel context.CancelFunc
	mu     sync.Mutex
}

func (r *sseResponder) respond(message proto.Message) error {
	payload, err := marshalProtoJSON(message)
	if err != nil {
		return err
	}
	return r.writeEvent("message", payload)
}

func (r *sseResponder) writeEvent(event string, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err := r.writer.WriteString("event: " + event + "\n"); err != nil {
		r.cancel()
		return err
	}
	if _, err := r.writer.WriteString("data: "); err != nil {
		r.cancel()
		return err
	}
	if _, err := r.writer.Write(payload); err != nil {
		r.cancel()
		return err
	}
	if _, err := r.writer.WriteString("\n\n"); err != nil {
		r.cancel()
		return err
	}
	if err := r.writer.Flush(); err != nil {
		r.cancel()
		return err
	}
	return nil
}

func (r *sseResponder) writeHeartbeat() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.writer.WriteString(": ping\n\n"); err != nil {
		r.cancel()
		return err
	}
	if err := r.writer.Flush(); err != nil {
		r.cancel()
		return err
	}
	return nil
}

func (r *sseResponder) startHeartbeat(ctx context.Context, interval time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.writeHeartbeat(); err != nil {
					return
				}
			}
		}
	}()
	return done
}
