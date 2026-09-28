package http

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"testing"

	brokerAPI "kafka-clone/server/datatypes/broker"
	web "kafka-clone/server/datatypes/broker/dto"
	gen "kafka-clone/server/datatypes/proto-generated"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
)

type apiBrokerStub struct {
	produceRequest web.ProduceRequest
	consumeRequest web.ConsumeRequest
}

func (b *apiBrokerStub) Produce(ctx brokerAPI.CommandContext, request web.ProduceRequest) error {
	b.produceRequest = request
	return ctx.Respond(&gen.ProduceResponse{Status: gen.KafkaStatus_Accepted})
}

func (b *apiBrokerStub) Consume(ctx brokerAPI.CommandContext, request web.ConsumeRequest) error {
	b.consumeRequest = request
	if err := ctx.Respond(&gen.ConsumeResponseList{
		Responses: []*gen.ConsumeResponse{{Offset: 1, Msg: []byte("first")}},
	}); err != nil {
		return err
	}
	return ctx.Respond(&gen.ConsumeResponseList{
		Responses: []*gen.ConsumeResponse{{Offset: 2, Msg: []byte("second")}},
	})
}

func TestProducerHandlerWritesProtoJSON(t *testing.T) {
	broker := &apiBrokerStub{}
	server := &HttpServer{broker: broker, logger: testHTTPLogger()}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodPost)
	ctx.Request.SetBodyString(`{"topic_name":"orders","partition_id":3,"key":"id","msg":"aGVsbG8="}`)

	server.producerHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, body = %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if broker.produceRequest.TopicName != "orders" || broker.produceRequest.PartitionID != 3 || string(broker.produceRequest.Msg) != "hello" {
		t.Fatalf("unexpected request: %+v", broker.produceRequest)
	}
	if !strings.Contains(string(ctx.Response.Body()), `"status":"Accepted"`) {
		t.Fatalf("unexpected response: %s", ctx.Response.Body())
	}
}

func TestBrokerRouterDispatchesProduce(t *testing.T) {
	broker := &apiBrokerStub{}
	server := &HttpServer{broker: broker, logger: testHTTPLogger()}
	router := router.New()
	server.SetupRouter(router)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodPost)
	ctx.Request.SetRequestURI("/broker/v1/produce")
	ctx.Request.SetBodyString(`{"topic_name":"orders","partition_id":0,"msg":"b2s="}`)

	router.Handler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK || broker.produceRequest.TopicName != "orders" {
		t.Fatalf("status = %d, request = %+v, body = %s", ctx.Response.StatusCode(), broker.produceRequest, ctx.Response.Body())
	}
}

func TestConsumeHandlerStreamsEachResponseAsSSE(t *testing.T) {
	broker := &apiBrokerStub{}
	server := &HttpServer{broker: broker, logger: testHTTPLogger()}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodGet)
	ctx.Request.SetRequestURI("/broker/v1/consume?topic_name=orders&partition_id=3&from_beginning=true&till_end=true")

	server.consumeHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, body = %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	var body bytes.Buffer
	if err := ctx.Response.BodyWriteTo(&body); err != nil {
		t.Fatalf("write response stream: %v", err)
	}
	stream := body.String()
	if strings.Count(stream, "event: message\n") != 2 {
		t.Fatalf("expected two SSE events, got: %q", stream)
	}
	if !strings.Contains(stream, `"offset":"1"`) || !strings.Contains(stream, `"offset":"2"`) {
		t.Fatalf("unexpected SSE stream: %q", stream)
	}
	if broker.consumeRequest.TopicName != "orders" || !broker.consumeRequest.FromBeginning || !broker.consumeRequest.TillEnd {
		t.Fatalf("unexpected request: %+v", broker.consumeRequest)
	}
}

func TestConsumeHandlerRejectsAmbiguousOffsets(t *testing.T) {
	server := &HttpServer{broker: &apiBrokerStub{}, logger: testHTTPLogger()}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodGet)
	ctx.Request.SetRequestURI("/broker/v1/consume?topic_name=orders&partition_id=0&from_beginning=true&start_offset=0&till_end=true")

	server.consumeHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusBadRequest {
		t.Fatalf("status = %d, want 400", ctx.Response.StatusCode())
	}
}

func TestBrokerRouterDispatchesConsumeGet(t *testing.T) {
	broker := &apiBrokerStub{}
	server := &HttpServer{broker: broker, logger: testHTTPLogger()}
	router := router.New()
	server.SetupRouter(router)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodGet)
	ctx.Request.SetRequestURI("/broker/v1/consume?topic_name=orders&partition_id=2&from_beginning=true&till_end=true")

	router.Handler(ctx)
	var body bytes.Buffer
	if err := ctx.Response.BodyWriteTo(&body); err != nil {
		t.Fatalf("write response stream: %v", err)
	}
	if ctx.Response.StatusCode() != fasthttp.StatusOK || broker.consumeRequest.PartitionID != 2 {
		t.Fatalf("status = %d, request = %+v, body = %s", ctx.Response.StatusCode(), broker.consumeRequest, body.String())
	}
}

func testHTTPLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
