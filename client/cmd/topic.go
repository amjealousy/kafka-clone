package cmd

import (
	"context"
	"fmt"
	gen "kafka-clone/server/datatypes/proto-generated"
	"time"

	"github.com/spf13/cobra"
)

var (
	topicCreateControlAddr string
	topicCreateName        string
	topicCreatePartitions  int32
	topicCreateReplication int32

	topicDescribeControlAddr string
	topicDescribeName        string
)

// TopicCmd — родительская команда для управления топиками (create/describe)
// через gRPC control-plane API (ControlService), см. server/datatypes/control.proto.
var TopicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Управление топиками через control-plane gRPC API (create/describe)",
}

var topicCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Создать топик (ControlService.CreateTopic)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return createTopic(topicCreateControlAddr, topicCreateName, topicCreatePartitions, topicCreateReplication)
	},
}

var topicDescribeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Показать метаданные топика: лидеров и реплики партиций (ControlService.DescribeTopic)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return describeTopic(topicDescribeControlAddr, topicDescribeName)
	},
}

func init() {
	topicCreateCmd.Flags().StringVar(&topicCreateControlAddr, "control", "127.0.0.1:7090", "адрес control-plane gRPC API любой ноды кластера")
	topicCreateCmd.Flags().StringVar(&topicCreateName, "topic", "", "имя топика (обязательно)")
	topicCreateCmd.Flags().Int32Var(&topicCreatePartitions, "partitions", 1, "число партиций")
	topicCreateCmd.Flags().Int32Var(&topicCreateReplication, "replication-factor", 1, "фактор репликации")
	_ = topicCreateCmd.MarkFlagRequired("topic")

	topicDescribeCmd.Flags().StringVar(&topicDescribeControlAddr, "control", "127.0.0.1:7090", "адрес control-plane gRPC API любой ноды кластера")
	topicDescribeCmd.Flags().StringVar(&topicDescribeName, "topic", "", "имя топика (обязательно)")
	_ = topicDescribeCmd.MarkFlagRequired("topic")

	TopicCmd.AddCommand(topicCreateCmd, topicDescribeCmd)
}

// createTopic вызывает ControlService.CreateTopic. CreateTopic обслуживает
// только текущий контроллер кластера — если мы попали на другую ноду, она
// вернёт NotController=true и ControllerAddress; в этом случае один раз
// повторяем запрос уже на контроллере.
func createTopic(controlAddr, name string, partitions, replication int32) error {
	client, closeFn, err := controlClient(controlAddr)
	if err != nil {
		return err
	}
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &gen.CreateTopicRequest{
		TopicName:         name,
		NumPartitions:     partitions,
		ReplicationFactor: replication,
	}

	resp, err := client.CreateTopic(ctx, req)
	if err != nil {
		return fmt.Errorf("CreateTopic rpc failed on %s: %w", controlAddr, err)
	}

	if resp.NotController && resp.ControllerAddress != "" {
		fmt.Printf("%s is not the controller, retrying on controller %s\n", controlAddr, resp.ControllerAddress)
		client2, closeFn2, err := controlClient(resp.ControllerAddress)
		if err != nil {
			return err
		}
		defer closeFn2()
		resp, err = client2.CreateTopic(ctx, req)
		if err != nil {
			return fmt.Errorf("CreateTopic rpc failed on controller %s: %w", resp.ControllerAddress, err)
		}
	}

	if !resp.Success {
		return fmt.Errorf("create topic failed: %s", resp.Error)
	}

	fmt.Printf("topic %q created (partitions=%d, replication_factor=%d)\n", name, partitions, replication)
	return nil
}

// describeTopic вызывает ControlService.DescribeTopic и печатает лидера и
// список реплик (с их in_sync статусом) для каждой партиции.
func describeTopic(controlAddr, name string) error {
	client, closeFn, err := controlClient(controlAddr)
	if err != nil {
		return err
	}
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.DescribeTopic(ctx, &gen.DescribeTopicRequest{TopicName: name})
	if err != nil {
		return fmt.Errorf("DescribeTopic rpc failed on %s: %w", controlAddr, err)
	}
	if !resp.Found {
		return fmt.Errorf("topic %q not found: %s", name, resp.Error)
	}

	fmt.Printf("topic: %s\n", resp.TopicName)
	for _, p := range resp.Partitions {
		fmt.Printf("  partition %d: leader=node-%d (%s)\n", p.PartitionId, p.LeaderNodeId, p.LeaderAddress)
		for _, r := range p.Replicas {
			fmt.Printf("    replica node-%d %s in_sync=%v\n", r.NodeId, r.Address, r.InSync)
		}
	}
	return nil
}

// discoverLeader находит TCP-адрес лидера партиции — именно туда нужно
// отправлять produce-запросы (только лидер принимает запись, см.
// Broker.IsPartitionLeader).
func discoverLeader(controlAddr, topicName string, partitionID int64) (string, error) {
	client, closeFn, err := controlClient(controlAddr)
	if err != nil {
		return "", err
	}
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.DescribeTopic(ctx, &gen.DescribeTopicRequest{TopicName: topicName})
	if err != nil {
		return "", fmt.Errorf("DescribeTopic rpc failed on %s: %w", controlAddr, err)
	}
	if !resp.Found {
		return "", fmt.Errorf("topic %q not found: %s", topicName, resp.Error)
	}
	for _, p := range resp.Partitions {
		if p.PartitionId == partitionID {
			if p.LeaderAddress == "" {
				return "", fmt.Errorf("partition %d has no leader currently", partitionID)
			}
			return p.LeaderAddress, nil
		}
	}
	return "", fmt.Errorf("partition %d not found for topic %q", partitionID, topicName)
}

// discoverInSyncReplica находит TCP-адрес любой in-sync реплики партиции —
// только с in-sync реплики (лидер тоже in-sync сам с собой) можно
// обслуживать consume-запросы (см. Broker.IsPartitionInSync).
func discoverInSyncReplica(controlAddr, topicName string, partitionID int64) (string, error) {
	client, closeFn, err := controlClient(controlAddr)
	if err != nil {
		return "", err
	}
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.DescribeTopic(ctx, &gen.DescribeTopicRequest{TopicName: topicName})
	if err != nil {
		return "", fmt.Errorf("DescribeTopic rpc failed on %s: %w", controlAddr, err)
	}
	if !resp.Found {
		return "", fmt.Errorf("topic %q not found: %s", topicName, resp.Error)
	}
	for _, p := range resp.Partitions {
		if p.PartitionId != partitionID {
			continue
		}
		for _, r := range p.Replicas {
			if r.InSync {
				return r.Address, nil
			}
		}
		return "", fmt.Errorf("no in-sync replica found for partition %d", partitionID)
	}
	return "", fmt.Errorf("partition %d not found for topic %q", partitionID, topicName)
}
