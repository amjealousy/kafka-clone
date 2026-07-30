package cmd

import (
	"fmt"
	"kafka-clone/server/datatypes/encode"
	gen "kafka-clone/server/datatypes/proto-generated"

	"github.com/spf13/cobra"
)

var (
	produceControlAddr string
	produceBrokerAddr  string
	produceTopic       string
	producePartition   int64
	produceKey         string
	produceMessage     string
)

// ProduceCmd отправляет одно сообщение в топик по TCP produce-протоколу
// брокера (server/broker/broker.go producerHandler).
var ProduceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Отправить сообщение в топик (Produce)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runProduce()
	},
}

func init() {
	ProduceCmd.Flags().StringVar(&produceControlAddr, "control", "127.0.0.1:7090",
		"адрес control-plane gRPC API для автоматического поиска лидера партиции (используется, если --broker не задан)")
	ProduceCmd.Flags().StringVar(&produceBrokerAddr, "broker", "",
		"TCP-адрес брокера-лидера партиции (host:port); если пусто — определяется автоматически через --control")
	ProduceCmd.Flags().StringVar(&produceTopic, "topic", "", "имя топика (обязательно)")
	ProduceCmd.Flags().Int64Var(&producePartition, "partition", 0, "номер партиции")
	ProduceCmd.Flags().StringVar(&produceKey, "key", "", "ключ сообщения")
	ProduceCmd.Flags().StringVar(&produceMessage, "message", "", "тело сообщения (обязательно)")
	_ = ProduceCmd.MarkFlagRequired("topic")
	_ = ProduceCmd.MarkFlagRequired("message")
}

func runProduce() error {
	brokerAddr := produceBrokerAddr
	if brokerAddr == "" {
		addr, err := discoverLeader(produceControlAddr, produceTopic, producePartition)
		if err != nil {
			return fmt.Errorf("discover partition leader: %w", err)
		}
		brokerAddr = addr
		fmt.Printf("leader for %s/%d is %s\n", produceTopic, producePartition, brokerAddr)
	}

	payload := &gen.ProducePayload{
		TopicName:   produceTopic,
		Key:         produceKey,
		PartitionId: producePartition,
		Msg:         []byte(produceMessage),
	}

	body, err := sendTCPRequest(brokerAddr, encode.Produce, payload)
	if err != nil {
		return err
	}

	resp := &gen.ProduceResponse{}
	if err := decodeInto(body, resp); err != nil {
		return err
	}

	if resp.Status != gen.KafkaStatus_Accepted {
		msg := ""
		if resp.StatusMessage != nil {
			msg = *resp.StatusMessage
		}
		return fmt.Errorf("produce rejected: %s", msg)
	}

	fmt.Printf("message accepted: topic=%s partition=%d key=%q\n", produceTopic, producePartition, produceKey)
	return nil
}
