// Package cmd содержит подкоманды простого CLI-клиента для kafka-clone.
//
// Этот файл — общие низкоуровневые хелперы, которыми пользуются остальные
// подкоманды (topic.go, produce.go, consume.go):
//   - подключение к gRPC control-plane API брокера (ControlService);
//   - отправка/чтение сообщений по кастомному TCP-протоколу produce/consume,
//     который реализован в server/broker/tcp.go.
package cmd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"kafka-clone/server/datatypes/encode"
	"net"
	"time"

	gen "kafka-clone/server/datatypes/proto-generated"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// dialTimeout — таймаут на установление TCP/gRPC соединения с брокером.
const dialTimeout = 5 * time.Second

// dialControlPlane открывает gRPC-соединение к control-plane API узла
// кластера (адрес вида host:port — см. флаг -control у server/main.go).
func dialControlPlane(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// controlClient возвращает готовый к использованию ControlServiceClient и
// функцию закрытия соединения.
func controlClient(addr string) (gen.ControlServiceClient, func() error, error) {
	conn, err := dialControlPlane(addr)
	if err != nil {
		return nil, nil, fmt.Errorf("dial control-plane %s: %w", addr, err)
	}
	return gen.NewControlServiceClient(conn), conn.Close, nil
}

// writeRequest пишет в соединение один запрос по протоколу produce/consume:
// 12-байтный заголовок (MessageSize, CorrelationID, CommandType — все
// big-endian uint32) и protobuf-закодированное тело. MessageSize считается
// по тем же правилам, что и на сервере (server/broker/tcp.go
// handleConnection): 8 + len(body).
func writeRequest(w io.Writer, correlationID uint32, cmd encode.Command, payload encode.Payload) error {
	body := payload.Encode()

	header := make([]byte, 12)
	binary.BigEndian.PutUint32(header[0:4], uint32(8+len(body)))
	binary.BigEndian.PutUint32(header[4:8], correlationID)
	binary.BigEndian.PutUint32(header[8:12], cmd)

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if len(body) > 0 {
		if _, err := w.Write(body); err != nil {
			return fmt.Errorf("write body: %w", err)
		}
	}
	return nil
}

// readResponse читает один кадр ответа брокера: 4 байта размера
// (MessageSize = 4 + len(body), см. TCPContext.ProtocolClosure в
// server/broker/tcp.go), 4 байта CorrelationID и protobuf-тело.
func readResponse(r io.Reader) (correlationID uint32, body []byte, err error) {
	sizeBuf := make([]byte, 4)
	if _, err = io.ReadFull(r, sizeBuf); err != nil {
		return 0, nil, fmt.Errorf("read response size: %w", err)
	}
	messageSize := binary.BigEndian.Uint32(sizeBuf)
	if messageSize < 4 {
		return 0, nil, errors.New("invalid response: message size smaller than correlation id field")
	}

	rest := make([]byte, messageSize)
	if _, err = io.ReadFull(r, rest); err != nil {
		return 0, nil, fmt.Errorf("read response body: %w", err)
	}

	correlationID = binary.BigEndian.Uint32(rest[0:4])
	return correlationID, rest[4:], nil
}

// sendTCPRequest — удобный "запрос-ответ в одно соединение" хелпер для
// produce: открывает TCP-соединение с брокером, отправляет запрос и
// возвращает protobuf-тело единственного ответа. Именно так работает сервер
// для produce (см. Broker.HandleCommand: defer ctx.Close() закрывает
// соединение сразу после одного ответа).
func sendTCPRequest(addr string, cmd encode.Command, payload encode.Payload) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial broker %s: %w", addr, err)
	}
	defer conn.Close()

	if err := writeRequest(conn, 1, cmd, payload); err != nil {
		return nil, err
	}

	_, body, err := readResponse(conn)
	return body, err
}

// decodeInto — обёртка над proto.Unmarshal с более понятной ошибкой.
func decodeInto(body []byte, msg proto.Message) error {
	if err := proto.Unmarshal(body, msg); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// isClosedConnErr сообщает, что ошибка вызвана закрытием соединения нами же
// (например, пользователь нажал Ctrl+C во время --follow у consume).
func isClosedConnErr(err error) bool {
	return errors.Is(err, net.ErrClosed)
}
