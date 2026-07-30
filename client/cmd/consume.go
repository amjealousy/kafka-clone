package cmd

import (
	"errors"
	"fmt"
	"io"
	"kafka-clone/server/datatypes/encode"
	"math"
	"net"
	"os"
	"os/signal"
	"syscall"

	gen "kafka-clone/server/datatypes/proto-generated"

	"github.com/spf13/cobra"
)

var (
	consumeControlAddr   string
	consumeBrokerAddr    string
	consumeTopic         string
	consumePartition     int64
	consumeFromBeginning bool
	consumeStartOffset   uint64
	consumeFollow        bool
	consumeToOffset      uint64
)

// ConsumeCmd читает сообщения из партиции топика по TCP consume-протоколу
// брокера (server/broker/broker.go consumerHandler).
var ConsumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Прочитать сообщения из топика (Consume)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runConsume()
	},
}

func init() {
	ConsumeCmd.Flags().StringVar(&consumeControlAddr, "control", "127.0.0.1:7090",
		"адрес control-plane gRPC API для автоматического поиска in-sync реплики (используется, если --broker не задан)")
	ConsumeCmd.Flags().StringVar(&consumeBrokerAddr, "broker", "",
		"TCP-адрес брокера (in-sync реплика партиции); если пусто — определяется автоматически через --control")
	ConsumeCmd.Flags().StringVar(&consumeTopic, "topic", "", "имя топика (обязательно)")
	ConsumeCmd.Flags().Int64Var(&consumePartition, "partition", 0, "номер партиции")
	ConsumeCmd.Flags().BoolVar(&consumeFromBeginning, "from-beginning", true, "читать с начала партиции")
	ConsumeCmd.Flags().Uint64Var(&consumeStartOffset, "offset", 0, "офсет, с которого начинать чтение (действует при --from-beginning=false)")
	ConsumeCmd.Flags().Uint64Var(&consumeToOffset, "to-offset", math.MaxInt64,
		"конечный офсет (не включительно); по умолчанию читать все доступные сообщения")
	ConsumeCmd.Flags().BoolVar(&consumeFollow, "follow", false,
		"не закрывать соединение после чтения истории, а продолжать стримить новые сообщения (аналог tail -f)")
	_ = ConsumeCmd.MarkFlagRequired("topic")
}

func runConsume() error {
	brokerAddr := consumeBrokerAddr
	if brokerAddr == "" {
		addr, err := discoverInSyncReplica(consumeControlAddr, consumeTopic, consumePartition)
		if err != nil {
			return fmt.Errorf("discover in-sync replica: %w", err)
		}
		brokerAddr = addr
		fmt.Printf("reading %s/%d from %s\n", consumeTopic, consumePartition, brokerAddr)
	}

	payload := &gen.ConsumePayload{
		TopicName:   consumeTopic,
		PartitionID: consumePartition,
	}
	if consumeFromBeginning {
		payload.StartPosition = &gen.ConsumePayload_FromBeginning{FromBeginning: true}
	} else {
		payload.StartPosition = &gen.ConsumePayload_StartOffset{StartOffset: consumeStartOffset}
	}
	if consumeFollow {
		payload.FinPosition = &gen.ConsumePayload_TillEnd{TillEnd: true}
	} else {
		payload.FinPosition = &gen.ConsumePayload_FinOffset{FinOffset: consumeToOffset}
	}

	conn, err := net.DialTimeout("tcp", brokerAddr, dialTimeout)
	if err != nil {
		return fmt.Errorf("dial broker %s: %w", brokerAddr, err)
	}
	defer conn.Close()

	if err := writeRequest(conn, 1, encode.Consume, payload); err != nil {
		return err
	}

	// В режиме --follow соединение остаётся открытым и сервер стримит новые
	// сообщения бесконечно (см. consumerHandler: цикл по streamC). Ctrl+C
	// закрывает наше соединение, что аккуратно останавливает чтение.
	if consumeFollow {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-stop
			fmt.Println("stopping, closing connection...")
			conn.Close()
		}()
	}

	for {
		_, body, err := readResponse(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || isClosedConnErr(err) {
				if !consumeFollow {
					fmt.Println("no messages in requested range")
				}
				return nil
			}
			return err
		}

		list := &gen.ConsumeResponseList{}
		if err := decodeInto(body, list); err != nil {
			return err
		}
		if list.Error != nil {
			return fmt.Errorf("consume rejected: %s", *list.Error)
		}
		for _, m := range list.Responses {
			fmt.Printf("offset=%d timestamp=%d payload=%q\n", m.Offset, m.Timestamp, string(m.Msg))
		}

		if !consumeFollow {
			return nil
		}
	}
}
