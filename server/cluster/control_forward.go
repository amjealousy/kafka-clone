package cluster

import (
	"context"
	"errors"
	"log/slog"

	"kafka-clone/server/datatypes"
	brokertypes "kafka-clone/server/datatypes/broker"
	gen "kafka-clone/server/datatypes/proto-generated"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	// ControllerUnavailableError — контроллер сейчас не избран; клиенту следует
	// повторить запрос позже.
	ControllerUnavailableError = datatypes.ErrControllerUnavailable
	// ControllerAddressMissingError — контроллер найден, но не опубликовал
	// control-plane адрес (нода зарегистрирована старой версией бинаря).
	ControllerAddressMissingError = errors.New("cluster controller has no control-plane address")
	// ErrNodeEvictedFromCluster — эта нода читает etcd, но её собственного
	// ключа в /kafka/nodes/ уже нет: аренда истекла, кластер её похоронил.
	// Обслуживать клиента она больше не имеет права.
	ErrNodeEvictedFromCluster = datatypes.ErrNodeEvictedFromCluster
)

// controlClient возвращает переиспользуемый gRPC-клиент control-plane API по
// адресу. grpc.NewClient создаёт соединение лениво, поэтому здесь нет сетевых
// операций под мьютексом.
func (n *NodeCoordinator) controlClient(addr string) (gen.ControlServiceClient, error) {
	n.controlMx.Lock()
	defer n.controlMx.Unlock()

	if conn, ok := n.controlConns[addr]; ok {
		return gen.NewControlServiceClient(conn), nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	if n.controlConns == nil {
		n.controlConns = make(map[string]*grpc.ClientConn)
	}
	n.controlConns[addr] = conn
	return gen.NewControlServiceClient(conn), nil
}

// CreateTopicRouted реализует datatypes.ITopicRouter: выполняет создание топика
// на контроллере независимо от того, на какую ноду пришёл запрос.
//
// Зачем форвард на сервере, а не редирект на клиенте: браузер не может ходить
// напрямую по внутрикластерным адресам нод (они не резолвятся снаружи, это
// cross-origin и отдельный TLS-сертификат на каждую ноду). Поэтому топологию
// разруливает нода, а UI всегда говорит с тем адресом, с которого загрузился.
//
// Рекурсии тут нет: проксированный вызов попадает в ControlService.CreateTopic,
// то есть в обычный NodeCoordinator.CreateTopic, который сам никуда не
// форвардит, а честно отвечает not_controller.
func (n *NodeCoordinator) CreateTopicRouted(ctx context.Context, req datatypes.CreateTopicRequest) (datatypes.CreateTopicResponse, error) {
	if n.broker.ReadRole() == brokertypes.Controller {
		return n.CreateTopic(ctx, req)
	}

	// Свежее чтение без кэша: решение "кто контроллер" ведёт к записи, здесь
	// устаревшие на треть секунды данные означали бы запись в бывшего лидера.
	view, err := n.readClusterView(ctx, 0)
	if err != nil {
		return datatypes.CreateTopicResponse{}, err
	}
	if view.controller == nil {
		return datatypes.CreateTopicResponse{
			Success:       false,
			Error:         ControllerUnavailableError,
			NotController: true,
		}, nil
	}

	// etcd уже считает контроллером нас, а локальная роль просто не успела
	// примениться (electionLoop опрашивает состояние раз в несколько секунд).
	if view.controller.ID == n.nodeID {
		return n.CreateTopic(ctx, req)
	}

	addr := view.controller.ControlAddress
	if addr == "" {
		return datatypes.CreateTopicResponse{
			Success:       false,
			Error:         ControllerAddressMissingError,
			NotController: true,
		}, nil
	}

	client, err := n.controlClient(addr)
	if err != nil {
		return datatypes.CreateTopicResponse{}, err
	}

	n.logger.Info("проксируем CreateTopic на контроллер",
		slog.String("topic", req.TopicName), slog.Int64("controller", view.controller.ID), slog.String("addr", addr))

	resp, err := client.CreateTopic(ctx, &gen.CreateTopicRequest{
		TopicName:         req.TopicName,
		NumPartitions:     int32(req.NumPartitions),
		ReplicationFactor: int32(req.ReplicationFactor),
	})
	if err != nil {
		return datatypes.CreateTopicResponse{}, err
	}

	out := datatypes.CreateTopicResponse{
		Success:           resp.GetSuccess(),
		NotController:     resp.GetNotController(),
		ControllerAddress: resp.GetControllerAddress(),
	}
	if msg := resp.GetError(); msg != "" {
		out.Error = errors.New(msg)
	}
	if out.Success {
		n.invalidateClusterView()
	}
	return out, nil
}
