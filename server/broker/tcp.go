package broker

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"kafka-clone/server/datatypes"
	"log/slog"
	"net"
	"runtime/debug"
	"sync"

	"google.golang.org/protobuf/proto"
)

type TCPServer struct {
	Address     string
	Port        string
	headerPool  sync.Pool
	reqbodyPool sync.Pool
	logger      *slog.Logger
	Kbroker     *Broker
	MainHandler func(*TCPContext, []byte)
}
type PooledBuffer struct {
	Body   []byte
	Header [12]byte // Массив для заголовка (чтение)
	Reply  []byte   // Массив для ответа (запись)
}

func NewTCPServer(log *slog.Logger) *TCPServer {
	t := &TCPServer{}
	t.headerPool = sync.Pool{
		New: func() any { return bufio.NewReader(nil) },
	}
	log.With("component", "TCP")
	t.logger = log
	t.reqbodyPool = sync.Pool{
		New: func() any {
			return &PooledBuffer{
				Body:  make([]byte, MaxBodySize),
				Reply: make([]byte, MaxBodySize),
			}
		},
	}
	return t
}

func CreateListener(server *TCPServer, addr, port string) net.Listener {
	server.Address = addr
	server.Port = port

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", addr, port))
	if err != nil {
		server.logger.Error("Failed to bind to port ", server.Port)
		panic(err)
	}

	return l
}
func (t *TCPServer) SetBroker(b *Broker) {
	t.Kbroker = b
}

func (t *TCPServer) ReadLoop(ctx context.Context, l net.Listener) error {
	t.logger.Info("Starting readloop")
	for {
		accept, tcpErr := l.Accept()
		if tcpErr != nil {
			return errors.New("failed to accept tcp connection in read loop")
		}
		go func() {
			defer func() {
				if r := recover(); r != nil {

					t.logger.Error("[CRITICAL] panic in tcp handler: %v\nStack trace::\n%s\n", r, debug.Stack())
				}
			}()
			t.logger.Info("[INFO] new tcp connection from ", "ip", accept.RemoteAddr())

			t.handleConnection(accept)
		}()
	}
}

const MaxBodySize = 64 * 1024

func (t *TCPServer) handleConnection(conn net.Conn) (handlelvlerr error) {
	defer func() {
		if handlelvlerr != nil && handlelvlerr.Error() != "" {
			conn.Write([]byte("unexpected error while handling connection\n"))
			conn.Write([]byte("err - " + handlelvlerr.Error() + "\n"))
		}
		conn.Close()
	}()

	// 1. Берем Reader из пула и привязываем его к текущему соединению
	connReader := t.headerPool.Get().(*bufio.Reader)
	connReader.Reset(conn)
	// Возвращаем Reader в пул. Сброс структуры произойдет при следующем Get+Reset
	defer t.headerPool.Put(connReader)

	// 2. Нам гарантированно нужно 12 байт для заголовка (4 на size + 4 на correlation_id+CMD)
	buf := t.reqbodyPool.Get().(*PooledBuffer)

	// io.ReadFull блокирует поток до тех пор, пока не прочитает ровно 12 байт (или не случится ошибка)
	// Это защищает от проблемы "частичного чтения" по TCP
	_, err := io.ReadFull(connReader, buf.Header[:])
	if err != nil && err != io.EOF {

		return errors.New(err.Error())
	}

	// 3. Парсим размер сообщения , correlation_id и cmd
	header := datatypes.KafkaHeader{}
	header.MessageSize = binary.BigEndian.Uint32(buf.Header[0:4])
	header.CorrelationID = binary.BigEndian.Uint32(buf.Header[4:8])
	header.CommandType = binary.BigEndian.Uint32(buf.Header[8:12])

	fmt.Printf("Получен заголовок. Message Size: %d, Correlation ID: %d, Command type: %d \n", header.MessageSize, header.CorrelationID, header.CommandType)
	fmt.Printf("Hex лог заголовка:\n%s", hex.Dump(buf.Header[:]))

	// 4. Если у сообщения есть тело (messageSize > 4), читаем его дальше
	// (4 байта мы уже забрали под correlation_id, если размер включает в себя заголовок)
	if header.MessageSize > 4 {
		bodySize := header.MessageSize - 8
		if bodySize > 0 {
			if bodySize > MaxBodySize {
				return errors.New("error: размер тела превышает максимально допустимый")
			}

			// Отрезаем от него слайс нужного нам размера
			bodyBuf := buf.Body[:bodySize]
			if _, err := io.ReadFull(connReader, bodyBuf); err != nil {
				return errors.New(err.Error())
			}

			// В этой точке bodyBuf содержит чистые данные тела
			flush := func() {
				t.reqbodyPool.Put(buf)
			}
			tcpContext := NewTCPContext(conn, buf, header, flush)
			t.MainHandler(tcpContext, bodyBuf)

		}
	}

	return err
}

type TCPContext struct {
	con       net.Conn
	Header    datatypes.KafkaHeader
	buf       *PooledBuffer
	flushFunc func()
}

func NewTCPContext(con net.Conn, buf *PooledBuffer, header datatypes.KafkaHeader, flushF func()) *TCPContext {
	return &TCPContext{
		con:       con,
		buf:       buf,
		Header:    header,
		flushFunc: flushF,
	}

}
func (ctx *TCPContext) Close() {
	ctx.con.Close()
	if cap(ctx.buf.Reply) > MaxBodySize || cap(ctx.buf.Body) > MaxBodySize {
		return
	}
	ctx.flushFunc()
}

func (ctx *TCPContext) Write(b []byte) error {
	_, err := ctx.con.Write(ctx.ProtocolClosure(len(b)))
	if err != nil {
		return err
	}
	return nil
}
func (ctx *TCPContext) Encode(str proto.Message) ([]byte, error) {
	options := proto.MarshalOptions{}
	out, err := options.MarshalAppend(ctx.buf.Reply[:0], str)
	return out, err
}
func (ctx *TCPContext) Decode(b []byte, message proto.Message) error {

	err := proto.Unmarshal(b, message)
	if err != nil {
		return err
	}
	return nil
}

func (ctx *TCPContext) ProtocolClosure(written int) []byte {
	bodyLen := written
	// 2. Вычисляем общий размер сообщения
	// 4 байта (Correlation ID) + длина нашего тела
	totalMessageSize := uint32(4 + bodyLen)

	// 3. Собираем ответ в нашем пулированном буфере
	// Записываем размер ответа (первые 4 байта)
	binary.BigEndian.PutUint32(ctx.buf.Reply[0:4], totalMessageSize)

	// Записываем Correlation ID (следующие 4 байта).
	// По правилам Kafka он должен совпадать с тем, что прислал клиент,
	// но если у вас по заданию жестко 7, пишем 7.
	binary.BigEndian.PutUint32(ctx.buf.Reply[4:8], ctx.Header.CorrelationID)

	// 5. Вычисляем итоговую длину всего пакета (8 байт заголовка + длина тела)
	finalPacketSize := 8 + bodyLen

	return ctx.buf.Reply[:finalPacketSize]
}
