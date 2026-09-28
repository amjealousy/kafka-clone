package grpc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"

	"kafka-clone/server/datatypes"
	gen "kafka-clone/server/datatypes/proto-generated"
)

type controllerStub struct {
	describeRequest datatypes.DescribeTopicRequest
	createRequest   datatypes.CreateTopicRequest
	describeResult  datatypes.DescribeTopicResponse
	createResult    datatypes.CreateTopicResponse
	err             error
}

func (s *controllerStub) DescribeTopic(_ context.Context, req datatypes.DescribeTopicRequest) (datatypes.DescribeTopicResponse, error) {
	s.describeRequest = req
	return s.describeResult, s.err
}

func (s *controllerStub) CreateTopic(_ context.Context, req datatypes.CreateTopicRequest) (datatypes.CreateTopicResponse, error) {
	s.createRequest = req
	return s.createResult, s.err
}

func TestControlGrpcServerDescribeTopicMapsTransportTypes(t *testing.T) {
	controller := &controllerStub{
		describeResult: datatypes.DescribeTopicResponse{
			Found:     true,
			TopicName: "orders",
			Partitions: []datatypes.PartitionInfo{{
				PartitionID:   7,
				LeaderNodeID:  11,
				LeaderAddress: "broker-11:9092",
				Replicas: []datatypes.ReplicaInfo{{
					NodeID:  12,
					Address: "broker-12:9092",
					InSync:  true,
				}},
			}},
		},
	}
	server := NewControlGrpcServer(controller, testLogger())

	response, err := server.DescribeTopic(context.Background(), &gen.DescribeTopicRequest{TopicName: "orders"})
	if err != nil {
		t.Fatalf("DescribeTopic returned error: %v", err)
	}
	if controller.describeRequest.TopicName != "orders" {
		t.Fatalf("mapped request topic = %q, want orders", controller.describeRequest.TopicName)
	}
	if !response.GetFound() || response.GetTopicName() != "orders" {
		t.Fatalf("unexpected response: %+v", response)
	}
	partitions := response.GetPartitions()
	if len(partitions) != 1 || partitions[0].GetPartitionId() != 7 || partitions[0].GetLeaderNodeId() != 11 || partitions[0].GetLeaderAddress() != "broker-11:9092" {
		t.Fatalf("unexpected partitions: %+v", partitions)
	}
	replicas := partitions[0].GetReplicas()
	if len(replicas) != 1 || replicas[0].GetNodeId() != 12 || replicas[0].GetAddress() != "broker-12:9092" || !replicas[0].GetInSync() {
		t.Fatalf("unexpected replicas: %+v", replicas)
	}
}

func TestControlGrpcServerCreateTopicMapsTransportTypes(t *testing.T) {
	controller := &controllerStub{
		createResult: datatypes.CreateTopicResponse{
			Error:             errors.New("this node is not the cluster controller"),
			NotController:     true,
			ControllerAddress: "controller:9094",
		},
	}
	server := NewControlGrpcServer(controller, testLogger())

	response, err := server.CreateTopic(context.Background(), &gen.CreateTopicRequest{
		TopicName:         "orders",
		NumPartitions:     4,
		ReplicationFactor: 3,
	})
	if err != nil {
		t.Fatalf("CreateTopic returned error: %v", err)
	}
	wantRequest := datatypes.CreateTopicRequest{
		TopicName:         "orders",
		NumPartitions:     4,
		ReplicationFactor: 3,
	}
	if !reflect.DeepEqual(controller.createRequest, wantRequest) {
		t.Fatalf("mapped request = %+v, want %+v", controller.createRequest, wantRequest)
	}
	if response.GetSuccess() || response.GetError() != "this node is not the cluster controller" || !response.GetNotController() || response.GetControllerAddress() != "controller:9094" {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestControlGrpcServerPropagatesControllerError(t *testing.T) {
	wantErr := errors.New("storage unavailable")
	server := NewControlGrpcServer(&controllerStub{err: wantErr}, testLogger())

	_, err := server.DescribeTopic(context.Background(), &gen.DescribeTopicRequest{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("DescribeTopic error = %v, want %v", err, wantErr)
	}

	_, err = server.CreateTopic(context.Background(), &gen.CreateTopicRequest{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("CreateTopic error = %v, want %v", err, wantErr)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
