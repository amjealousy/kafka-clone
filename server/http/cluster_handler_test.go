package http

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"kafka-clone/server/datatypes"
	brokerAPI "kafka-clone/server/datatypes/broker"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
)

type clusterAdminStub struct {
	overview    brokerAPI.ClusterOverview
	overviewErr error

	topic      brokerAPI.ClusterTopic
	topicFound bool
	topicErr   error

	createRequest  datatypes.CreateTopicRequest
	createResponse datatypes.CreateTopicResponse
	createErr      error

	readiness brokerAPI.NodeReadiness
}

func (s *clusterAdminStub) GetNodeReadiness(context.Context) brokerAPI.NodeReadiness {
	return s.readiness
}

func (s *clusterAdminStub) DescribeTopic(context.Context, datatypes.DescribeTopicRequest) (datatypes.DescribeTopicResponse, error) {
	return datatypes.DescribeTopicResponse{}, nil
}

func (s *clusterAdminStub) CreateTopic(context.Context, datatypes.CreateTopicRequest) (datatypes.CreateTopicResponse, error) {
	return datatypes.CreateTopicResponse{}, nil
}

func (s *clusterAdminStub) GetClusterOverview(context.Context) (brokerAPI.ClusterOverview, error) {
	return s.overview, s.overviewErr
}

func (s *clusterAdminStub) GetClusterNodeList(context.Context) (brokerAPI.ClusterSnapshot, error) {
	return brokerAPI.ClusterSnapshot{Nodes: s.overview.Nodes, Controller: s.overview.Controller}, s.overviewErr
}

func (s *clusterAdminStub) GetClusterTopicsList(context.Context) ([]brokerAPI.ClusterTopic, error) {
	return s.overview.Topics, s.overviewErr
}

func (s *clusterAdminStub) GetClusterTopic(_ context.Context, _ string) (brokerAPI.ClusterTopic, bool, error) {
	return s.topic, s.topicFound, s.topicErr
}

func (s *clusterAdminStub) FindControllerAddress(context.Context) (string, error) {
	if s.overview.Controller == nil {
		return "", s.overviewErr
	}
	return s.overview.Controller.ControlAddress, s.overviewErr
}

func (s *clusterAdminStub) CreateTopicRouted(_ context.Context, req datatypes.CreateTopicRequest) (datatypes.CreateTopicResponse, error) {
	s.createRequest = req
	return s.createResponse, s.createErr
}

func testOverview() brokerAPI.ClusterOverview {
	controller := brokerAPI.ClusterNode{
		ID: 1, Role: "controller", IsController: true,
		TCPAddress: "10.0.0.1:5090", HTTPAddress: "10.0.0.1:8090", ControlAddress: "10.0.0.1:7090",
	}
	return brokerAPI.ClusterOverview{
		Nodes:           []brokerAPI.ClusterNode{controller, {ID: 2, Role: "follower", HTTPAddress: "10.0.0.2:8090"}},
		Controller:      &controller,
		ControllerEpoch: 4,
		Topics: []brokerAPI.ClusterTopic{{
			Name:              "orders",
			ReplicationFactor: 3,
			Partitions: []brokerAPI.ClusterPartition{{
				PartitionID: 0, LeaderNodeID: 2, LeaderTCPAddress: "10.0.0.2:5090", LeaderAlive: true,
				Replicas: []brokerAPI.ClusterReplica{{NodeID: 2, InSync: true, Alive: true}},
			}},
		}},
	}
}

func newClusterServer(admin datatypes.IClusterAdmin) *HttpServer {
	return &HttpServer{controller: admin, logger: testHTTPLogger()}
}

func TestClusterOverviewHandlerSerializesSnapshot(t *testing.T) {
	server := newClusterServer(&clusterAdminStub{overview: testOverview()})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodGet)

	server.ClusterOverviewHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, body = %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	body := string(ctx.Response.Body())
	for _, want := range []string{
		`"id":"1"`, `"is_controller":true`, `"http_address":"10.0.0.1:8090"`,
		`"controller_epoch":"4"`, `"leader_node_id":"2"`, `"leader_alive":true`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("response body must contain %s, got: %s", want, body)
		}
	}
}

// Нода, потерявшая связь с etcd, обязана отвечать ошибкой, а не отдавать
// последний известный состав кластера: иначе клиент считает её здоровой.
func TestClusterOverviewHandlerReturns503WhenEtcdUnavailable(t *testing.T) {
	server := newClusterServer(&clusterAdminStub{overviewErr: errors.New("context deadline exceeded")})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodGet)

	server.ClusterOverviewHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body = %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if !strings.Contains(string(ctx.Response.Body()), "cluster_unavailable") {
		t.Fatalf("unexpected body: %s", ctx.Response.Body())
	}
}

func TestClusterOverviewRouteIsRegistered(t *testing.T) {
	server := newClusterServer(&clusterAdminStub{overview: testOverview()})
	r := router.New()
	server.SetupRouter(r)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodGet)
	ctx.Request.SetRequestURI("/cluster/v1/overview")
	r.Handler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, body = %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
}

// Нода, исключённая из кластера по истёкшей аренде, обязана отдавать
// отдельный код: данные она прочитала корректные, но точкой входа быть уже не
// может, и клиент должен уйти на другую ноду.
func TestClusterOverviewHandlerReportsEvictedNode(t *testing.T) {
	server := newClusterServer(&clusterAdminStub{
		overviewErr: fmt.Errorf("%w: node 3", datatypes.ErrNodeEvictedFromCluster),
	})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodGet)

	server.ClusterOverviewHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", ctx.Response.StatusCode())
	}
	if !strings.Contains(string(ctx.Response.Body()), "node_evicted") {
		t.Fatalf("body = %s, want node_evicted", ctx.Response.Body())
	}
}

func TestReadinessHandlerReturns200WhenNodeIsInCluster(t *testing.T) {
	server := newClusterServer(&clusterAdminStub{readiness: brokerAPI.NodeReadiness{
		NodeID: 1, Ready: true, InCluster: true, IsController: true, ClusterSize: 3, ControllerID: 1,
	}})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodGet)

	server.ReadinessHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, body = %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if !strings.Contains(string(ctx.Response.Body()), `"cluster_size":3`) {
		t.Fatalf("unexpected body: %s", ctx.Response.Body())
	}
}

func TestReadinessHandlerReturns503WhenNodeIsEvicted(t *testing.T) {
	server := newClusterServer(&clusterAdminStub{readiness: brokerAPI.NodeReadiness{
		NodeID: 1, Ready: false, InCluster: false, Reason: "lease expired",
	}})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodGet)

	server.ReadinessHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", ctx.Response.StatusCode())
	}
	if !strings.Contains(string(ctx.Response.Body()), "lease expired") {
		t.Fatalf("unexpected body: %s", ctx.Response.Body())
	}
}

// Liveness не должна зависеть от etcd: иначе мигание сети приводит к
// перезапуску всего кластера брокеров разом.
func TestLivenessHandlerIgnoresClusterState(t *testing.T) {
	server := newClusterServer(&clusterAdminStub{
		overviewErr: errors.New("etcd unreachable"),
		readiness:   brokerAPI.NodeReadiness{Ready: false, Reason: "etcd unreachable"},
	})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodGet)

	server.LivenessHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, want 200", ctx.Response.StatusCode())
	}
}

func TestDescribeTopicHandlerReturns404ForUnknownTopic(t *testing.T) {
	server := newClusterServer(&clusterAdminStub{topicFound: false})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodGet)
	ctx.SetUserValue("name", "ghost")

	server.DescribeTopicHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusNotFound {
		t.Fatalf("status = %d, want 404, body = %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
}

func TestDescribeTopicHandlerReturnsTopic(t *testing.T) {
	overview := testOverview()
	server := newClusterServer(&clusterAdminStub{topic: overview.Topics[0], topicFound: true})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodGet)
	ctx.SetUserValue("name", "orders")

	server.DescribeTopicHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status = %d, body = %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if !strings.Contains(string(ctx.Response.Body()), `"name":"orders"`) {
		t.Fatalf("unexpected body: %s", ctx.Response.Body())
	}
}

func TestCreateTopicHandlerRoutesRequestToController(t *testing.T) {
	admin := &clusterAdminStub{createResponse: datatypes.CreateTopicResponse{Success: true}}
	server := newClusterServer(admin)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodPost)
	ctx.Request.SetBodyString(`{"topic_name":"orders","num_partitions":4,"replication_factor":3}`)

	server.CreateTopicHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusCreated {
		t.Fatalf("status = %d, body = %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	want := datatypes.CreateTopicRequest{TopicName: "orders", NumPartitions: 4, ReplicationFactor: 3}
	if admin.createRequest != want {
		t.Fatalf("forwarded request = %+v, want %+v", admin.createRequest, want)
	}
}

func TestCreateTopicHandlerRejectsInvalidRequest(t *testing.T) {
	admin := &clusterAdminStub{}
	server := newClusterServer(admin)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodPost)
	ctx.Request.SetBodyString(`{"topic_name":"orders","num_partitions":0}`)

	server.CreateTopicHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusBadRequest {
		t.Fatalf("status = %d, want 400", ctx.Response.StatusCode())
	}
	if admin.createRequest.TopicName != "" {
		t.Fatal("invalid request must not reach the cluster layer")
	}
}

// Проксирование уже было выполнено на сервере, поэтому not_controller тут
// означает "контроллера нет вообще" — это 503 и повтор, а не приглашение
// клиенту сходить на другой адрес.
func TestCreateTopicHandlerMapsMissingControllerTo503(t *testing.T) {
	server := newClusterServer(&clusterAdminStub{createResponse: datatypes.CreateTopicResponse{
		NotController: true,
		Error:         datatypes.ErrControllerUnavailable,
	}})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodPost)
	ctx.Request.SetBodyString(`{"topic_name":"orders","num_partitions":1}`)

	server.CreateTopicHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body = %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if !strings.Contains(string(ctx.Response.Body()), "controller_unavailable") {
		t.Fatalf("unexpected body: %s", ctx.Response.Body())
	}
}

func TestCreateTopicHandlerMapsDomainErrorsToStatuses(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{"exists", datatypes.ErrTopicAlreadyExists, fasthttp.StatusConflict, "topic_exists"},
		{"race", datatypes.ErrRaceCondition, fasthttp.StatusConflict, "concurrent_modification"},
		{"nodes", datatypes.ErrNotEnoughAliveNodes, fasthttp.StatusConflict, "not_enough_nodes"},
		{"other", datatypes.ErrTopicCredential, fasthttp.StatusBadRequest, "create_topic_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newClusterServer(&clusterAdminStub{
				createResponse: datatypes.CreateTopicResponse{Error: tc.err},
			})
			ctx := &fasthttp.RequestCtx{}
			ctx.Request.Header.SetMethod(fasthttp.MethodPost)
			ctx.Request.SetBodyString(`{"topic_name":"orders","num_partitions":1}`)

			server.CreateTopicHandler(ctx)

			if ctx.Response.StatusCode() != tc.wantStatus {
				t.Fatalf("status = %d, want %d", ctx.Response.StatusCode(), tc.wantStatus)
			}
			if !strings.Contains(string(ctx.Response.Body()), tc.wantCode) {
				t.Fatalf("body = %s, want code %s", ctx.Response.Body(), tc.wantCode)
			}
		})
	}
}

// Ошибки доменного слоя (etcd недоступен) не должны превращаться в 500.
func TestCreateTopicHandlerMapsTransportErrorTo503(t *testing.T) {
	server := newClusterServer(&clusterAdminStub{createErr: errors.New("etcd unreachable")})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodPost)
	ctx.Request.SetBodyString(`{"topic_name":"orders","num_partitions":1}`)

	server.CreateTopicHandler(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", ctx.Response.StatusCode())
	}
}
