package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// NodeLocalStatePath — префикс, под которым каждая нода само-репортит в etcd
// список topic/partition/offset, которые у неё физически есть на диске ПРЯМО
// СЕЙЧАС — вне зависимости от того, назначена ли она на эти партиции в данный
// момент. Это позволяет Controller'у при выборе кандидата на пустующую
// реплику приоритетно брать ноду, у которой уже есть (пусть и "осиротевшие")
// данные по нужной партиции — ей нужно будет докачать через FetchLog гораздо
// меньше, чем полностью пустой ноде.
const NodeLocalStatePath = "/kafka/node-local-state/"

// LocalPartitionState — одна запись само-репорта: "у меня локально есть
// топик Topic, партиция PartitionId, до оффсета Offset (не включительно)".
type LocalPartitionState struct {
	Topic       string `json:"topic"`
	PartitionId int64  `json:"partition_id"`
	Offset      uint64 `json:"offset"`
}

// NodeLocalStateStore инкапсулирует чтение/запись само-репортов нод в etcd.
type NodeLocalStateStore struct {
	cli *clientv3.Client
}

func NewNodeLocalStateStore(cli *clientv3.Client) *NodeLocalStateStore {
	return &NodeLocalStateStore{cli: cli}
}

func nodeLocalStateKey(nodeID int64) string {
	return fmt.Sprintf("%s%d", NodeLocalStatePath, nodeID)
}

// Put полностью перезаписывает само-репорт данной ноды.
func (s *NodeLocalStateStore) Put(ctx context.Context, nodeID int64, entries []LocalPartitionState) error {
	val, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	_, err = s.cli.Put(ctx, nodeLocalStateKey(nodeID), string(val))
	return err
}

// ListAll возвращает само-репорты всех известных нод: nodeID -> его записи.
// Используется Controller'ом при выборе кандидата для назначения на партицию.
func (s *NodeLocalStateStore) ListAll(ctx context.Context) (map[int64][]LocalPartitionState, error) {
	resp, err := s.cli.Get(ctx, NodeLocalStatePath, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	out := make(map[int64][]LocalPartitionState, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		idStr := strings.TrimPrefix(string(kv.Key), NodeLocalStatePath)
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			continue
		}
		var entries []LocalPartitionState
		if err := json.Unmarshal(kv.Value, &entries); err != nil {
			continue
		}
		out[id] = entries
	}
	return out, nil
}

// runLocalStateReportLoop периодически публикует в etcd список всех локально
// присутствующих (topic, partition, offset) — включая партиции, на которые
// нода прямо сейчас НЕ назначена (например, "осиротевшие" после падения и
// восстановленные из манифеста MongoDB). Это и есть то, что описано как
// "нода сообщает в etcd оффсеты, которые у неё есть локально", а Controller
// читает это в pickReplicaCandidate, отдавая таким нодам приоритет.
func (n *NodeCoordinator) runLocalStateReportLoop(ctx context.Context) {
	report := func() {
		local := n.broker.ListLocalPartitionOffsets()
		entries := make([]LocalPartitionState, 0, len(local))
		for _, e := range local {
			entries = append(entries, LocalPartitionState{
				Topic:       e.Topic,
				PartitionId: int64(e.PartitionID),
				Offset:      e.Offset,
			})
		}
		putCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := n.localState.Put(putCtx, n.nodeID, entries); err != nil {
			slog.Error("[LocalState] Не удалось опубликовать локальное состояние в etcd", slog.String("err", err.Error()))
		}
	}

	// Публикуем сразу при старте (в т.ч. по данным, восстановленным из Mongo),
	// не дожидаясь первого тика — чтобы Controller мог узнать о нас как можно раньше.
	report()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report()
		}
	}
}
