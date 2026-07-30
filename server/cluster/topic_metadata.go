package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TopicMetaStore инкапсулирует чтение/запись конфигурации топиков в etcd
// по ключу вида "/kafka/topic/<name>".
type TopicMetaStore struct {
	cli *clientv3.Client
}

func NewTopicMetaStore(cli *clientv3.Client) *TopicMetaStore {
	return &TopicMetaStore{cli: cli}
}

func topicKey(name string) string {
	return fmt.Sprintf("%s%s", TopicPath, name)
}

// GetTopic возвращает параметры топика и его текущую ModRevision (для CAS).
// Если топика нет, возвращает (nil, 0, nil).
func (s *TopicMetaStore) GetTopic(ctx context.Context, name string) (*TopicParams, int64, error) {
	resp, err := s.cli.Get(ctx, topicKey(name))
	if err != nil {
		return nil, 0, err
	}
	if len(resp.Kvs) == 0 {
		return nil, 0, nil
	}
	var params TopicParams
	if err := json.Unmarshal(resp.Kvs[0].Value, &params); err != nil {
		return nil, 0, err
	}
	return &params, resp.Kvs[0].ModRevision, nil
}

// ListTopics возвращает все топики кластера.
func (s *TopicMetaStore) ListTopics(ctx context.Context) ([]TopicParams, error) {
	resp, err := s.cli.Get(ctx, TopicPath, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]TopicParams, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var params TopicParams
		if err := json.Unmarshal(kv.Value, &params); err != nil {
			continue
		}
		out = append(out, params)
	}
	return out, nil
}

// PutTopicCAS атомарно записывает параметры топика, только если его ModRevision
// не изменилась с момента чтения (expectedRev). Если expectedRev == 0 —
// ожидается, что ключа ещё не существует.
func (s *TopicMetaStore) PutTopicCAS(ctx context.Context, params TopicParams, expectedRev int64) (bool, error) {
	val, err := json.Marshal(params)
	if err != nil {
		return false, err
	}
	key := topicKey(params.Name)

	var cmp clientv3.Cmp
	if expectedRev == 0 {
		cmp = clientv3.Compare(clientv3.Version(key), "=", 0)
	} else {
		cmp = clientv3.Compare(clientv3.ModRevision(key), "=", expectedRev)
	}

	txn := s.cli.Txn(ctx).
		If(cmp).
		Then(clientv3.OpPut(key, string(val)))

	resp, err := txn.Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

var ErrTopicNotFound = errors.New("topic not found in etcd")

// mutateTopic читает топик, применяет функцию-мутатор и пишет обратно через CAS.
// При гонке повторяет попытку несколько раз.
func (s *TopicMetaStore) mutateTopic(ctx context.Context, name string, mutate func(*TopicParams) error) error {
	const maxRetries = 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		params, rev, err := s.GetTopic(ctx, name)
		if err != nil {
			return err
		}
		if params == nil {
			return ErrTopicNotFound
		}
		if err := mutate(params); err != nil {
			return err
		}
		ok, err := s.PutTopicCAS(ctx, *params, rev)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		// Проиграли гонку — небольшая задержка и повтор
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return errors.New("failed to mutate topic after retries: CAS contention")
}

// AddReplicaToPartition добавляет ноду в набор реплик партиции (но НЕ в ISR).
// Нода начинает получать новые сообщения от лидера сразу, но становится in-sync
// только после того как догонит лог (см. MarkNodeInSync).
func (s *TopicMetaStore) AddReplicaToPartition(ctx context.Context, topicName string, partitionID int64, nodeID int64) error {
	return s.mutateTopic(ctx, topicName, func(params *TopicParams) error {
		for i := range params.Partitions {
			if params.Partitions[i].PartitionId != partitionID {
				continue
			}
			if params.Partitions[i].ContainsNode(nodeID) {
				return nil // уже назначена
			}
			params.Partitions[i].ReplicasNodeId = append(params.Partitions[i].ReplicasNodeId, nodeID)
			return nil
		}
		return fmt.Errorf("partition %d not found in topic %s", partitionID, topicName)
	})
}

// MarkNodeInSync переводит ноду из состояния "просто реплика" в in-sync реплику
// (добавляет её в IsrNodeId), после того как она догнала лог.
func (s *TopicMetaStore) MarkNodeInSync(ctx context.Context, topicName string, partitionID int64, nodeID int64) error {
	return s.mutateTopic(ctx, topicName, func(params *TopicParams) error {
		for i := range params.Partitions {
			if params.Partitions[i].PartitionId != partitionID {
				continue
			}
			if !params.Partitions[i].ContainsNode(nodeID) {
				return fmt.Errorf("node %d is not a replica of partition %d", nodeID, partitionID)
			}
			if params.Partitions[i].IsInSync(nodeID) {
				return nil
			}
			params.Partitions[i].IsrNodeId = append(params.Partitions[i].IsrNodeId, nodeID)
			return nil
		}
		return fmt.Errorf("partition %d not found in topic %s", partitionID, topicName)
	})
}

// RemoveNodeFromPartition убирает ноду из реплик и ISR партиции (например, когда
// нода выбыла из кластера).
func (s *TopicMetaStore) RemoveNodeFromPartition(ctx context.Context, topicName string, partitionID int64, nodeID int64) error {
	return s.mutateTopic(ctx, topicName, func(params *TopicParams) error {
		for i := range params.Partitions {
			if params.Partitions[i].PartitionId != partitionID {
				continue
			}
			params.Partitions[i].ReplicasNodeId = removeInt64(params.Partitions[i].ReplicasNodeId, nodeID)
			params.Partitions[i].IsrNodeId = removeInt64(params.Partitions[i].IsrNodeId, nodeID)
			return nil
		}
		return nil
	})
}

func removeInt64(s []int64, v int64) []int64 {
	out := s[:0]
	for _, x := range s {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}
