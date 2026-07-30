package main

import (
	"io"
	"log/slog"
	"net"
	"testing"

	"kafka-clone/server/broker"
)

// BenchmarkHandleConnection прогоняет реальный TCP-приём через публичный
// ReadLoop сервера (handleConnection приватный и недоступен из пакета main).
func BenchmarkHandleConnection(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := broker.NewTCPServer(logger)
	server.MainHandler = func(ctx *broker.TCPContext, body []byte) {
		ctx.Close()
	}

	requestData := append([]byte{0, 0, 0, 9, 0, 0, 0, 42}, []byte("Hello")...)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	go func() { _ = server.ReadLoop(nil, lis) }()

	addr := lis.Addr().String()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		_, _ = conn.Write(requestData)
		reply := make([]byte, 8)
		_, _ = conn.Read(reply)
		conn.Close()
	}
}
