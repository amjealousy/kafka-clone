package http

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"errors"
	"io/fs"
	"strconv"
	"time"

	"kafka-clone/server/datatypes"
	brokerAPI "kafka-clone/server/datatypes/broker"
	web "kafka-clone/server/datatypes/broker/dto"
	"kafka-clone/server/datatypes/encode"

	"github.com/valyala/fasthttp"
	"google.golang.org/protobuf/proto"
)

func (s *HttpServer) BrokeringRouter(ctx *fasthttp.RequestCtx) {
	action, _ := ctx.UserValue("action").(string)
	switch action {
	case "produce":
		if ctx.IsPost() {
			s.producerHandler(ctx)
		} else {
			s.writeHttpError(ctx, fasthttp.StatusMethodNotAllowed, "invalid_request", "method not allowed")
		}
	case "consume":
		if ctx.IsGet() {
			s.consumeHandler(ctx)
		} else {
			s.writeHttpError(ctx, fasthttp.StatusMethodNotAllowed, "invalid_request", "method not allowed")
		}
	default:
		s.writeHttpError(ctx, fasthttp.StatusNotFound, "invalid_request", "endpoint not found")
	}
}

// LivenessHandler — проба живости процесса (/healthz).
//
// Намеренно НЕ проверяет etcd. Liveness-проба, завязанная на внешнюю
// зависимость, приводит к тому, что при кратковременной недоступности etcd
// оркестратор перезапускает разом весь кластер брокеров, превращая мигание
// сети в полноценную аварию. За "нода бесполезна для клиента" отвечает
// /readyz, который лишь убирает её из балансировки.
func (s *HttpServer) LivenessHandler(ctx *fasthttp.RequestCtx) {
	s.writeJSON(ctx, fasthttp.StatusOK, map[string]any{"status": "ok"})
}

// ReadinessHandler — проба готовности (/readyz): 200, только если нода
// прочитала etcd И видит себя в составе кластера. Именно это снимает
// отвалившуюся ноду с балансировщика, и именно этого клиент не может
// определить сам.
func (s *HttpServer) ReadinessHandler(ctx *fasthttp.RequestCtx) {
	if s.controller == nil {
		s.writeJSON(ctx, fasthttp.StatusServiceUnavailable, brokerAPI.NodeReadiness{
			Reason: "cluster control-plane is not configured",
		})
		return
	}

	readiness := s.controller.GetNodeReadiness(ctx)
	status := fasthttp.StatusOK
	if !readiness.Ready {
		status = fasthttp.StatusServiceUnavailable
		s.logger.Warn("node is not ready", "reason", readiness.Reason)
	}
	s.writeJSON(ctx, status, readiness)
}

// ClusterOverviewHandler отдаёт согласованный снимок кластера: ноды,
// действующий контроллер и топики. Данные читаются из etcd на момент запроса,
// поэтому нода, потерявшая связь с кластером, вернёт 503, а не устаревший
// состав, который клиент принял бы за актуальный.
func (s *HttpServer) ClusterOverviewHandler(ctx *fasthttp.RequestCtx) {
	if s.controller == nil {
		s.writeHttpError(ctx, fasthttp.StatusServiceUnavailable, "cluster_unavailable", "cluster control-plane is not configured")
		return
	}
	overview, err := s.controller.GetClusterOverview(ctx)
	if err != nil {
		s.writeClusterReadError(ctx, "cluster overview read failed", err)
		return
	}
	s.writeJSON(ctx, fasthttp.StatusOK, overview)
}

// ClusterNodesHandler отдаёт только состав кластера и контроллера.
func (s *HttpServer) ClusterNodesHandler(ctx *fasthttp.RequestCtx) {
	if s.controller == nil {
		s.writeHttpError(ctx, fasthttp.StatusServiceUnavailable, "cluster_unavailable", "cluster control-plane is not configured")
		return
	}
	snapshot, err := s.controller.GetClusterNodeList(ctx)
	if err != nil {
		s.writeClusterReadError(ctx, "cluster nodes read failed", err)
		return
	}
	s.writeJSON(ctx, fasthttp.StatusOK, snapshot)
}

// ClusterTopicsHandler отдаёт топики с разрешёнными адресами лидеров партиций.
func (s *HttpServer) ClusterTopicsHandler(ctx *fasthttp.RequestCtx) {
	if s.controller == nil {
		s.writeHttpError(ctx, fasthttp.StatusServiceUnavailable, "cluster_unavailable", "cluster control-plane is not configured")
		return
	}
	topics, err := s.controller.GetClusterTopicsList(ctx)
	if err != nil {
		s.writeClusterReadError(ctx, "cluster topics read failed", err)
		return
	}
	s.writeJSON(ctx, fasthttp.StatusOK, map[string]any{"topics": topics})
}

// DescribeTopicHandler описывает один топик. Читать метаданные может любая
// нода — обращаться к контроллеру для этого не требуется.
func (s *HttpServer) DescribeTopicHandler(ctx *fasthttp.RequestCtx) {
	if s.controller == nil {
		s.writeHttpError(ctx, fasthttp.StatusServiceUnavailable, "cluster_unavailable", "cluster control-plane is not configured")
		return
	}
	name, _ := ctx.UserValue("name").(string)
	if name == "" {
		s.writeHttpError(ctx, fasthttp.StatusBadRequest, "invalid_request", "topic name is required")
		return
	}

	topic, found, err := s.controller.GetClusterTopic(ctx, name)
	if err != nil {
		s.writeClusterReadError(ctx, "describe topic failed", err)
		return
	}
	if !found {
		s.writeHttpError(ctx, fasthttp.StatusNotFound, "topic_not_found", "topic "+name+" does not exist")
		return
	}
	s.writeJSON(ctx, fasthttp.StatusOK, topic)
}

// CreateTopicHandler создаёт топик. Запрос принимает ЛЮБАЯ нода: если она не
// контроллер, она сама проксирует вызов контроллеру по control-plane gRPC.
// Клиенту не нужно знать топологию кластера и повторять запрос по другому
// адресу — см. NodeCoordinator.CreateTopicRouted.
func (s *HttpServer) CreateTopicHandler(ctx *fasthttp.RequestCtx) {
	if s.controller == nil {
		s.writeHttpError(ctx, fasthttp.StatusServiceUnavailable, "cluster_unavailable", "cluster control-plane is not configured")
		return
	}

	request := web.CreateTopicRequest{}
	if err := json.Unmarshal(ctx.PostBody(), &request); err != nil {
		s.writeHttpError(ctx, fasthttp.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if err := request.Validate(); err != nil {
		s.writeHttpError(ctx, fasthttp.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	response, err := s.controller.CreateTopicRouted(ctx, datatypes.CreateTopicRequest{
		TopicName:         request.TopicName,
		NumPartitions:     request.NumPartitions,
		ReplicationFactor: request.ReplicationFactor,
	})
	if err != nil {
		s.logger.Error("create topic failed", "topic", request.TopicName, "error", err)
		s.writeHttpError(ctx, fasthttp.StatusServiceUnavailable, "cluster_unavailable", err.Error())
		return
	}

	if !response.Success {
		status, code := createTopicFailureStatus(response)
		s.writeHttpError(ctx, status, code, errorText(response.Error))
		return
	}

	s.writeJSON(ctx, fasthttp.StatusCreated, map[string]any{
		"success":    true,
		"topic_name": request.TopicName,
	})
}

// createTopicFailureStatus переводит доменный отказ в HTTP-статус.
// not_controller здесь означает, что контроллера не нашлось ВООБЩЕ (идут
// выборы) — проксирование уже было попробовано, поэтому это 503 и повтор, а не
// указание клиенту идти на другой адрес.
func createTopicFailureStatus(response datatypes.CreateTopicResponse) (int, string) {
	switch {
	case response.NotController:
		return fasthttp.StatusServiceUnavailable, "controller_unavailable"
	case errors.Is(response.Error, datatypes.ErrTopicAlreadyExists):
		return fasthttp.StatusConflict, "topic_exists"
	case errors.Is(response.Error, datatypes.ErrRaceCondition):
		return fasthttp.StatusConflict, "concurrent_modification"
	case errors.Is(response.Error, datatypes.ErrNotEnoughAliveNodes):
		return fasthttp.StatusConflict, "not_enough_nodes"
	default:
		return fasthttp.StatusBadRequest, "create_topic_failed"
	}
}

func errorText(err error) string {
	if err == nil {
		return "create topic failed"
	}
	return err.Error()
}

// writeClusterReadError — единая точка отказа для discovery-ручек. Оба исхода
// означают для клиента одно и то же ("уходи на другую ноду"), но коды разные,
// чтобы при разборе инцидента было видно, что именно случилось:
//
//	node_evicted        — нода читает etcd, но сама исключена из кластера;
//	cluster_unavailable — нода потеряла связь с etcd.
func (s *HttpServer) writeClusterReadError(ctx *fasthttp.RequestCtx, message string, err error) {
	if errors.Is(err, datatypes.ErrNodeEvictedFromCluster) {
		s.logger.Warn(message, "error", err)
		s.writeHttpError(ctx, fasthttp.StatusServiceUnavailable, "node_evicted", err.Error())
		return
	}
	s.logger.Error(message, "error", err)
	s.writeHttpError(ctx, fasthttp.StatusServiceUnavailable, "cluster_unavailable", err.Error())
}

func (s *HttpServer) indexHandler(ctx *fasthttp.RequestCtx) {
	body, err := fs.ReadFile(s.templatesFS, "index.html")
	if err != nil {
		s.writeHttpError(ctx, fasthttp.StatusInternalServerError, "ui_unavailable", err.Error())
		return
	}
	ctx.SetContentType("text/html; charset=utf-8")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody(body)
}

func (s *HttpServer) producerHandler(ctx *fasthttp.RequestCtx) {
	if s.broker == nil {
		s.writeHttpError(ctx, fasthttp.StatusServiceUnavailable, "broker_unavailable", "broker is not configured")
		return
	}

	request := web.ProduceRequest{}
	if err := json.Unmarshal(ctx.PostBody(), &request); err != nil {
		s.writeHttpError(ctx, fasthttp.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if err := request.Validate(); err != nil {
		s.writeHttpError(ctx, fasthttp.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	responded := false
	commandCtx := newJSONCommandContext(ctx, encode.Produce, func(message proto.Message) error {
		body, err := marshalProtoJSON(message)
		if err != nil {
			return err
		}
		ctx.SetContentType("application/json; charset=utf-8")
		ctx.Response.Header.Set("Cache-Control", "no-store")
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetBody(body)
		responded = true
		return nil
	})
	if err := s.broker.Produce(commandCtx, request); err != nil {
		s.writeHttpError(ctx, fasthttp.StatusBadRequest, "produce_failed", err.Error())
		return
	}
	if !responded {
		s.writeHttpError(ctx, fasthttp.StatusInternalServerError, "empty_response", "broker did not return a response")
	}
}
func (s *HttpServer) consumeHandler(ctx *fasthttp.RequestCtx) {
	if s.broker == nil {
		s.writeHttpError(ctx, fasthttp.StatusServiceUnavailable, "broker_unavailable", "broker is not configured")
		return
	}

	request, err := consumeRequestFromQuery(ctx.QueryArgs())
	if err != nil {
		s.writeHttpError(ctx, fasthttp.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := request.Validate(); err != nil {
		s.writeHttpError(ctx, fasthttp.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("text/event-stream; charset=utf-8")
	ctx.Response.Header.Set("Cache-Control", "no-cache")
	ctx.Response.Header.Set("Connection", "keep-alive")
	ctx.Response.Header.Set("X-Accel-Buffering", "no")
	ctx.Response.ImmediateHeaderFlush = true

	broker := s.broker
	logger := s.logger
	ctx.SetBodyStreamWriter(func(writer *bufio.Writer) {
		streamCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		responder := &sseResponder{writer: writer, cancel: cancel}
		heartbeatDone := responder.startHeartbeat(streamCtx, 15*time.Second)
		commandCtx := newJSONCommandContext(streamCtx, encode.Consume, responder.respond)

		err := broker.Consume(commandCtx, request)
		if err != nil && !errors.Is(err, context.Canceled) {
			payload, marshalErr := json.Marshal(map[string]string{"error": err.Error()})
			if marshalErr == nil {
				_ = responder.writeEvent("error", payload)
			}
			logger.Error("consume stream failed", "error", err)
		}
		cancel()
		<-heartbeatDone
	})
}

// consumeRequestFromQuery собирает контракт consume из URL, потому что
// браузерный EventSource умеет отправлять только GET без тела запроса.
func consumeRequestFromQuery(args *fasthttp.Args) (web.ConsumeRequest, error) {
	request := web.ConsumeRequest{TopicName: string(args.Peek("topic_name"))}

	partitionRaw := string(args.Peek("partition_id"))
	if partitionRaw == "" {
		return request, errors.New("partition_id is required")
	}
	partitionID, err := strconv.ParseInt(partitionRaw, 10, 64)
	if err != nil {
		return request, errors.New("partition_id must be an integer")
	}
	request.PartitionID = partitionID

	if value := args.Peek("start_offset"); len(value) > 0 {
		offset, parseErr := strconv.ParseUint(string(value), 10, 64)
		if parseErr != nil {
			return request, errors.New("start_offset must be a non-negative integer")
		}
		request.StartOffset = &offset
	}
	if value := args.Peek("from_beginning"); len(value) > 0 {
		fromBeginning, parseErr := strconv.ParseBool(string(value))
		if parseErr != nil {
			return request, errors.New("from_beginning must be a boolean")
		}
		request.FromBeginning = fromBeginning
	}
	if value := args.Peek("fin_offset"); len(value) > 0 {
		offset, parseErr := strconv.ParseUint(string(value), 10, 64)
		if parseErr != nil {
			return request, errors.New("fin_offset must be a non-negative integer")
		}
		request.FinOffset = &offset
	}
	if value := args.Peek("till_end"); len(value) > 0 {
		tillEnd, parseErr := strconv.ParseBool(string(value))
		if parseErr != nil {
			return request, errors.New("till_end must be a boolean")
		}
		request.TillEnd = tillEnd
	}

	return request, nil
}
