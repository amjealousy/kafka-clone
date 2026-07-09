package main

import (
	"kafka-clone/server/broker"
	"net"
	"testing"
)

func BenchmarkHandleConnection(b *testing.B) {
	server := broker.NewTCPServer()

	requestData := append([]byte{0, 0, 0, 9, 0, 0, 0, 42}, []byte("Hello")...)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Создаем виртуальное соединение в памяти
		b.StopTimer()
		clientConn, serverConn := net.Pipe()

		// В фоновом режиме пишем данные от лица "клиента"
		go func() {
			_, _ = clientConn.Write(requestData)
			// Читаем ответ сервера, чтобы закрыть цепочку
			reply := make([]byte, 8)
			_, _ = clientConn.Read(reply)
			clientConn.Close()
		}()

		// Сервер обрабатывает это соединение синхронно
		b.StartTimer()
		server.handleConnection(serverConn)
	}
}
