package main

import (
	"context"
	"kafka-clone/server/broker"
	"kafka-clone/server/persistent"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Контекст для инициализации
	initCtx, initCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer initCancel()

	// 1. Подключаемся к базе
	//dbClient, err := persistent.NewMongoClient(initCtx, "mongodb://localhost:27017", "kafka_clone", "topics")
	dbClient, err := persistent.MockNewMongoClient()
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}

	// 2. Создаем и инициализируем брокер
	myBroker := broker.New(1, dbClient, logger)
	//if err := myBroker.InitConfig(initCtx); err != nil {
	//	logger.Error("failed to init config", "error", err)
	//	os.Exit(1)
	//}

	//if brokerErr := myBroker.SetUp(); brokerErr != nil {
	//	logger.Error("failed to set up", "error", err)
	//	os.Exit(1)
	//}

	// запуск сервера
	tcp := broker.NewTCPServer(logger)
	listener := broker.CreateListener(tcp, "127.0.0.1", "5090")
	kafkaHandler := func(ctx *broker.TCPContext, body []byte) {
		myBroker.HandleCommand(ctx, body)
	}
	tcp.MainHandler = kafkaHandler
	tcp.SetBroker(myBroker)
	go tcp.ReadLoop(initCtx, listener)

	// 3. Создаем канал для перехвата сигналов ОС
	shutdownSig := make(chan os.Signal, 1)
	// SIGINT - это Ctrl+C в консоли, SIGTERM - стандартный сигнал остановки (например, в Docker/Kubernetes)
	signal.Notify(shutdownSig, os.Interrupt, syscall.SIGTERM)

	logger.Info("broker is running. Press Ctrl+C to stop.")

	// Блокируемся и ждем сигнала остановки
	sig := <-shutdownSig
	logger.Info("received shutdown signal", "signal", sig.String())

	// 4. Начинаем плавную остановку. Даем брокеру жесткий дедлайн в 15 секунд
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	// Вызываем наш Shutdown метод
	if err := myBroker.Shutdown(shutdownCtx); err != nil {
		logger.Error("broker shutdown finished with error", "error", err)
	}

	// 5. И в самом конце закрываем коннект к БД (Правило LIFO)
	logger.Info("closing mongodb connection...")
	if err := dbClient.Close(shutdownCtx); err != nil {
		logger.Error("failed to close mongo client cleanly", "error", err)
	}

	logger.Info("broker stopped cleanly. Bye!")
}
