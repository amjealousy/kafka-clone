package main

import (
	"context"
	"flag"
	grpcserver "kafka-clone/server/grpc"
	httpserver "kafka-clone/server/http"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"kafka-clone/server/broker"
	"kafka-clone/server/cluster"

	gen "kafka-clone/server/datatypes/proto-generated"
	"kafka-clone/server/persistent/db"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func main() {
	// Параметры ноды (для локального запуска нескольких брокеров).
	nodeID := flag.Int64("id", 1, "уникальный id ноды в кластере")
	tcpAddr := flag.String("tcp", "127.0.0.1:5090", "адрес TCP-сервера для produce/consume клиентов")
	grpcAddr := flag.String("grpc", "127.0.0.1:6090", "адрес gRPC-сервера репликации (AppendEntries/FetchLog)")
	controlAddr := flag.String("control", "127.0.0.1:7090", "адрес gRPC control-plane API (DescribeTopic/CreateTopic)")
	httpAddr := flag.String("http", "127.0.0.1:8090", "адрес HTTP API и web UI")
	etcdEndpoints := flag.String("etcd", "127.0.0.1:2379", "список endpoint'ов etcd через запятую")

	// Advertise-адреса — то, что нода публикует в etcd и по чему к ней
	// обращаются остальные. Их обязательно отделять от bind-адресов: в
	// контейнере нода слушает 0.0.0.0, а анонсировать должна имя, которое
	// резолвится из кластера (например broker-0.brokers.svc). Если флаг пуст,
	// анонсируется bind-адрес — привычное поведение для локального запуска.
	advertiseTCP := flag.String("advertise-tcp", "", "TCP-адрес, публикуемый в etcd (по умолчанию --tcp)")
	advertiseGRPC := flag.String("advertise-grpc", "", "gRPC-адрес репликации, публикуемый в etcd (по умолчанию --grpc)")
	advertiseControl := flag.String("advertise-control", "", "gRPC control-plane адрес, публикуемый в etcd (по умолчанию --control)")
	advertiseHTTP := flag.String("advertise-http", "", "HTTP-адрес, публикуемый в etcd (по умолчанию --http)")

	// Без CORS браузер не сможет переключиться на другую ноду при падении той,
	// с которой загрузилась админка: разные порты — уже разные origin.
	corsOrigins := flag.String("cors-origins", "", "разрешённые origin'ы через запятую (\"*\" — любой, пусто — CORS выключен)")
	flag.Parse()
	logFile, err2 := os.Create("./broker.log")
	if err2 != nil {
		panic(err2)
	}
	defer logFile.Close()
	if err := syscall.Dup2(int(logFile.Fd()), int(os.Stderr.Fd())); err != nil {
		panic(err)
	}

	logger := slog.New(slog.NewTextHandler(logFile, &slog.HandlerOptions{Level: slog.LevelDebug}))
	logger = logger.With("nodeID", *nodeID)
	slog.SetDefault(logger)
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic in main goroutine", "panic", r, "stack", string(debug.Stack()))
			panic(r)
		}
	}()

	// Контекст жизни всей ноды.
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	// 1. Подключаемся к базе (mock, пока persistence API не готов).

	dbClient, err := db.MockNewMongoClient()
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}

	// 2. Подключаемся к etcd.
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(*etcdEndpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Error("etcd connection failed", "error", err)
		os.Exit(1)
	}
	defer etcdClient.Close()

	etcdCheckCtx, etcdCheckCancel := context.WithTimeout(rootCtx, 5*time.Second)
	_, err = etcdClient.Get(etcdCheckCtx, cluster.StatePath)
	etcdCheckCancel()

	if err != nil {
		logger.Error(
			"etcd is unavailable, broker startup aborted",
			"endpoints",
			*etcdEndpoints,
			"error",
			err,
		)
		return
	}
	// 3. Создаём брокер.
	myBroker := broker.NewBroker(int(*nodeID), dbClient, logger, rootCtx, etcdClient)
	myBroker.SetNodeId(int(*nodeID))

	// 3.1 Восстанавливаем локально принадлежавшие этой ноде topic/partition из
	// манифеста MongoDB (см. Broker.EnsureLocalPartition). Каждая найденная
	// запись поднимает TopicProcessor/PartitionProcessor, что автоматически
	// восстанавливает сегменты и offset с диска (recoverSegments). Делаем это
	// ДО подключения к остальному кластеру, чтобы к моменту первого
	// само-репорта в etcd (NodeLocalStateStore) у нас уже были все офсеты.
	loadCtx, loadCancel := context.WithTimeout(rootCtx, 15*time.Second)
	if err := myBroker.LoadLocalPartitionsFromMongo(loadCtx); err != nil {
		logger.Error("failed to restore local partitions manifest from mongodb", "error", err)
	}
	loadCancel()

	// 4. Поднимаем gRPC-сервер репликации (сюда приходят AppendEntries и FetchLog).
	grpcLis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		logger.Error("failed to bind gRPC port", "addr", *grpcAddr, "error", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer()
	replSrv := grpcserver.NewReplicationGrpcServer(myBroker, logger)
	gen.RegisterReplicationServiceServer(grpcSrv, replSrv)
	go func() {
		logger.Info("replication gRPC server listening", "addr", *grpcAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			logger.Error("gRPC server stopped", "error", err)
		}
	}()

	// 5. Фоновый воркер сброса закоммиченных оффсетов.
	// вероятно был нужен до появления polling send commit in etcd
	// через метод NodeCoordinator::runLocalStateReportLoop
	//go myBroker.RunOffsetCommitConsumer(rootCtx)

	// 6. Создаём координатор кластера (etcd-регистрация, выборы, watch-лупы) —
	//    он же реализует datatypes.IController для control-plane gRPC API ниже.
	coordinator := cluster.NewNodeCoordinator(*nodeID, logger, myBroker, etcdClient)

	// 6.1 Поднимаем ОТДЕЛЬНЫЙ gRPC-сервер control-plane API (DescribeTopic
	// доступен на любой ноде; CreateTopic обслуживает только контроллер —
	// остальные ответят not_controller с адресом актуального контроллера).
	// Порт намеренно отличается и от TCP produce/consume, и от gRPC-репликации.
	controlLis, err := net.Listen("tcp", *controlAddr)
	if err != nil {
		logger.Error("failed to bind control-plane gRPC port", "addr", *controlAddr, "error", err)
		os.Exit(1)
	}
	controlSrv := grpc.NewServer()
	controlAPI := grpcserver.NewControlGrpcServer(coordinator, logger)
	gen.RegisterControlServiceServer(controlSrv, controlAPI)
	go func() {
		logger.Info("control-plane gRPC server listening", "addr", *controlAddr)
		if err := controlSrv.Serve(controlLis); err != nil {
			logger.Error("control-plane gRPC server stopped", "error", err)
		}
	}()

	// 6.2 Запускаем координатор: регистрация в etcd (Unroled), выборы,
	//    watch-лупы топиков/нод, авто-реконфигурация партиций.
	coordinator.Start(cluster.NodeAddresses{
		Replication: advertised(*advertiseGRPC, *grpcAddr),
		TCP:         advertised(*advertiseTCP, *tcpAddr),
		Control:     advertised(*advertiseControl, *controlAddr),
		HTTP:        advertised(*advertiseHTTP, *httpAddr),
	})

	// 7. Запускаем HTTP API: JSON produce и streaming SSE consume.
	webRouter := router.New()
	webAPI := httpserver.NewHttpServer(dbClient, logger, coordinator, myBroker)
	if *corsOrigins != "" {
		webAPI.SetAllowedOrigins(strings.Split(*corsOrigins, ","))
	}
	webAPI.SetupRouter(webRouter)
	webSrv := &fasthttp.Server{
		Handler:     webRouter.Handler,
		Name:        "kafka-clone-http",
		ReadTimeout: 15 * time.Second,
		IdleTimeout: 60 * time.Second,
	}
	go func() {
		logger.Info("HTTP server listening", "addr", *httpAddr)
		if err := webSrv.ListenAndServe(*httpAddr); err != nil {
			logger.Error("HTTP server stopped", "error", err)
		}
	}()

	// 8. Запускаем TCP-сервер для продюсеров/консьюмеров.
	tcp := broker.NewTCPServer(logger)
	host, port := splitHostPort(*tcpAddr)
	listener := broker.CreateListener(tcp, host, port)
	tcp.MainHandler = myBroker.HandleCommand
	tcp.SetBroker(myBroker)
	go tcp.ReadLoop(rootCtx, listener)

	// 9. Ожидаем сигнала остановки.
	shutdownSig := make(chan os.Signal, 1)
	signal.Notify(shutdownSig, os.Interrupt, syscall.SIGTERM)
	logger.Info("broker is running. Press Ctrl+C to stop.", "nodeID", *nodeID, "grpc", *grpcAddr, "tcp", *tcpAddr, "http", *httpAddr)

	sig := <-shutdownSig
	logger.Info("received shutdown signal", "signal", sig.String())

	// 10. Плавная остановка.
	rootCancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	grpcSrv.GracefulStop()
	controlSrv.GracefulStop()
	// Координатор живёт на собственном контексте (не на rootCtx), поэтому его
	// фоновые циклы надо останавливать явно — иначе они переживают shutdown.
	if err := coordinator.Close(); err != nil {
		logger.Error("coordinator shutdown finished with error", "error", err)
	}
	if err := webSrv.ShutdownWithContext(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown finished with error", "error", err)
	}
	if err := myBroker.Shutdown(shutdownCtx); err != nil {
		logger.Error("broker shutdown finished with error", "error", err)
	}

	logger.Info("closing mongodb connection...")
	if err := dbClient.Close(shutdownCtx); err != nil {
		logger.Error("failed to close mongo client cleanly", "error", err)
	}

	logger.Info("broker stopped cleanly. Bye!")
}

// advertised выбирает адрес для публикации в etcd: явно заданный advertise
// либо, если он пуст, адрес прослушивания.
func advertised(advertise, bind string) string {
	if advertise != "" {
		return advertise
	}
	return bind
}

// splitHostPort разбивает "host:port" на составляющие для CreateListener.
func splitHostPort(addr string) (string, string) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, ""
	}
	return host, port
}
