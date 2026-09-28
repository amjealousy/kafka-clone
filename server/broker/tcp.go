package broker

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"kafka-clone/server/datatypes/encode"
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
	MainHandler func(CommandContext, []byte) error
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
				Reply: make([]byte, MaxBodySize+8),
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
		server.logger.Error("Failed to bind to port", "port", server.Port)
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
					t.logger.Error("panic in tcp handler", "panic", r, "stack", string(debug.Stack()))
				}
			}()
			t.logger.Info("[INFO] new tcp connection from ", "ip", accept.RemoteAddr())

			t.handleConnection(ctx, accept)

		}()
	}
}

const MaxBodySize = 64 * 1024

func (t *TCPServer) handleConnection(requestCtx context.Context, conn net.Conn) (handlelvlerr error) {
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
	header := encode.KafkaHeader{}
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
			tcpContext := NewTCPContextWithContext(requestCtx, conn, buf, header, flush)
			defer tcpContext.Close()
			if err := t.MainHandler(tcpContext, bodyBuf); err != nil {
				t.logger.Error("command handler failed", "command", header.CommandType, "error", err)
			}

		}
	}

	return err
}

type TCPContext struct {
	ctx       context.Context
	con       net.Conn
	Header    encode.KafkaHeader
	buf       *PooledBuffer
	flushFunc func()
	closeOnce sync.Once
}

var _ CommandContext = (*TCPContext)(nil)

func NewTCPContext(con net.Conn, buf *PooledBuffer, header encode.KafkaHeader, flushF func()) *TCPContext {
	return NewTCPContextWithContext(context.Background(), con, buf, header, flushF)
}

func NewTCPContextWithContext(ctx context.Context, con net.Conn, buf *PooledBuffer, header encode.KafkaHeader, flushF func()) *TCPContext {
	if ctx == nil {
		ctx = context.Background()
	}
	return &TCPContext{
		ctx:       ctx,
		con:       con,
		buf:       buf,
		Header:    header,
		flushFunc: flushF,
	}

}
func (ctx *TCPContext) Close() {
	ctx.closeOnce.Do(func() {
		_ = ctx.con.Close()
		if cap(ctx.buf.Reply) > MaxBodySize+8 || cap(ctx.buf.Body) > MaxBodySize {
			return
		}
		ctx.flushFunc()
	})
}

func (ctx *TCPContext) Write(b []byte) error {
	if len(b) > MaxBodySize {
		return errors.New("response body exceeds maximum size")
	}

	// Encode кладёт protobuf в начало Reply. Перед записью освобождаем первые
	// 8 байт под заголовок ответа. copy корректно работает с пересекающимися
	// срезами и не затирает начало protobuf, в отличие от прежней реализации.
	packet := ctx.buf.Reply[:8+len(b)]
	copy(packet[8:], b)
	binary.BigEndian.PutUint32(packet[0:4], uint32(4+len(b)))
	binary.BigEndian.PutUint32(packet[4:8], ctx.Header.CorrelationID)

	for len(packet) > 0 {
		n, err := ctx.con.Write(packet)
		if err != nil {
			return err
		}
		packet = packet[n:]
	}
	return nil
}

func (ctx *TCPContext) CommandType() encode.Command {
	return ctx.Header.CommandType
}

func (ctx *TCPContext) Context() context.Context {
	return ctx.ctx
}

// Respond кодирует protobuf-сообщение и записывает его в TCP frame с тем же
// correlation ID, который был получен в запросе.
func (ctx *TCPContext) Respond(message proto.Message) error {
	body, err := ctx.Encode(message)
	if err != nil {
		return err
	}
	return ctx.Write(body)
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
