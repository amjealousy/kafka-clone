package broker

import (
	"encoding/binary"
	"io"
	"net"
	"testing"

	"kafka-clone/server/datatypes/encode"
	gen "kafka-clone/server/datatypes/proto-generated"

	"google.golang.org/protobuf/proto"
)

func TestTCPContextRespondPreservesProtobufBody(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	buf := &PooledBuffer{
		Body:  make([]byte, MaxBodySize),
		Reply: make([]byte, MaxBodySize+8),
	}
	ctx := NewTCPContext(serverConn, buf, encode.KafkaHeader{CorrelationID: 42}, func() {})
	want := &gen.ConsumeResponseList{Responses: []*gen.ConsumeResponse{{Offset: 7, Msg: []byte("hello")}}}
	writeErr := make(chan error, 1)
	go func() { writeErr <- ctx.Respond(want) }()

	sizeBytes := make([]byte, 4)
	if _, err := io.ReadFull(clientConn, sizeBytes); err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, binary.BigEndian.Uint32(sizeBytes))
	if _, err := io.ReadFull(clientConn, frame); err != nil {
		t.Fatal(err)
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}

	if correlationID := binary.BigEndian.Uint32(frame[:4]); correlationID != 42 {
		t.Fatalf("correlation id = %d, want 42", correlationID)
	}
	got := &gen.ConsumeResponseList{}
	if err := proto.Unmarshal(frame[4:], got); err != nil {
		t.Fatalf("response protobuf is corrupted: %v", err)
	}
	if len(got.Responses) != 1 || got.Responses[0].Offset != 7 || string(got.Responses[0].Msg) != "hello" {
		t.Fatalf("unexpected response: %v", got)
	}
}

func TestTCPContextCloseIsIdempotent(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	buf := &PooledBuffer{
		Body:  make([]byte, MaxBodySize),
		Reply: make([]byte, MaxBodySize+8),
	}
	flushCount := 0
	ctx := NewTCPContext(serverConn, buf, encode.KafkaHeader{}, func() {
		flushCount++
	})

	ctx.Close()
	ctx.Close()

	if flushCount != 1 {
		t.Fatalf("flush count = %d, want 1", flushCount)
	}
}
