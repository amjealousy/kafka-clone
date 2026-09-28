package cluster

import (
	"context"
	"io"
	brokertypes "kafka-clone/server/datatypes/broker"
	"log/slog"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

func testClusterLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func members(states ...NodeState) map[int64]NodeState {
	out := make(map[int64]NodeState, len(states))
	for _, state := range states {
		out[state.ID] = state
	}
	return out
}

func ids(states []NodeState) []int64 {
	out := make([]int64, 0, len(states))
	for _, state := range states {
		out = append(out, state.ID)
	}
	return out
}

func equalIDs(got []int64, want ...int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// Главный сценарий ради которого вводился OnSnapshot: пока наблюдение было
// разорвано, нода 3 выбыла, и события об этом мы не видели. Простое применение
// снимка как набора join'ов оставило бы её в памяти навсегда.
func TestDiffMembersDetectsNodesLostDuringWatchOutage(t *testing.T) {
	previous := members(
		NodeState{ID: 1, Role: brokertypes.Controller},
		NodeState{ID: 2, Role: brokertypes.Follower},
		NodeState{ID: 3, Role: brokertypes.Follower},
	)
	current := members(
		NodeState{ID: 1, Role: brokertypes.Controller},
		NodeState{ID: 2, Role: brokertypes.Follower},
	)

	left, joined := diffMembers(previous, current)

	if !equalIDs(ids(left), 3) {
		t.Fatalf("left = %v, want [3]", ids(left))
	}
	if len(joined) != 0 {
		t.Fatalf("joined = %v, want empty", ids(joined))
	}
}

func TestDiffMembersDetectsNewNodes(t *testing.T) {
	previous := members(NodeState{ID: 1})
	current := members(NodeState{ID: 1}, NodeState{ID: 2}, NodeState{ID: 5})

	left, joined := diffMembers(previous, current)

	if len(left) != 0 {
		t.Fatalf("left = %v, want empty", ids(left))
	}
	// Порядок детерминированный — иначе реконфигурацию не воспроизвести.
	if !equalIDs(ids(joined), 2, 5) {
		t.Fatalf("joined = %v, want [2 5]", ids(joined))
	}
}

// Смена роли и переезд адреса обязаны считаться изменением: иначе кластер не
// узнает о новом контроллере и продолжит слать репликацию на старый адрес.
func TestDiffMembersDetectsChangedState(t *testing.T) {
	previous := members(NodeState{ID: 1, Role: brokertypes.Follower, Address: "10.0.0.1:6090"})

	roleChanged := members(NodeState{ID: 1, Role: brokertypes.Controller, Address: "10.0.0.1:6090"})
	if _, joined := diffMembers(previous, roleChanged); !equalIDs(ids(joined), 1) {
		t.Fatalf("смена роли должна попадать в joined, got %v", ids(joined))
	}

	addrChanged := members(NodeState{ID: 1, Role: brokertypes.Follower, Address: "10.0.0.9:6090"})
	if _, joined := diffMembers(previous, addrChanged); !equalIDs(ids(joined), 1) {
		t.Fatalf("смена адреса должна попадать в joined, got %v", ids(joined))
	}

	unchanged := members(NodeState{ID: 1, Role: brokertypes.Follower, Address: "10.0.0.1:6090"})
	if left, joined := diffMembers(previous, unchanged); len(left) != 0 || len(joined) != 0 {
		t.Fatalf("идентичный состав не должен давать событий: left=%v joined=%v", ids(left), ids(joined))
	}
}

func TestDiffMembersOnEmptyPrevious(t *testing.T) {
	current := members(NodeState{ID: 2}, NodeState{ID: 1})

	left, joined := diffMembers(nil, current)

	if len(left) != 0 {
		t.Fatalf("left = %v, want empty", ids(left))
	}
	if !equalIDs(ids(joined), 1, 2) {
		t.Fatalf("joined = %v, want [1 2]", ids(joined))
	}
}

func TestDecodeNodeState(t *testing.T) {
	kv := nodeKV(t, NodeState{ID: 7, Address: "10.0.0.7:6090", Role: brokertypes.Follower})
	state, ok := decodeNodeState(kv)
	if !ok || state.ID != 7 || state.Address != "10.0.0.7:6090" {
		t.Fatalf("decode = %+v, ok = %v", state, ok)
	}

	if _, ok := decodeNodeState(&mvccpb.KeyValue{Key: []byte(NodesDiscoveryPath + "9"), Value: []byte("{broken")}); ok {
		t.Fatal("битое значение не должно разбираться")
	}

	// Ключ без числового id восстановить невозможно — такую запись пропускаем,
	// иначе в составе кластера появится нода с id 0.
	if _, ok := decodeNodeState(&mvccpb.KeyValue{Key: []byte(NodesDiscoveryPath + "abc"), Value: []byte("{}")}); ok {
		t.Fatal("нечисловой id не должен приниматься")
	}
}

// enqueueRebalance вызывается прямо из цикла наблюдения за кластером.
// Блокирующая отправка остановила бы обработку членства целиком, поэтому
// переполнение очереди обязано терять задачу (её подхватит reconcile-цикл),
// а не ждать места.
func TestEnqueueRebalanceNeverBlocks(t *testing.T) {
	n := &NodeCoordinator{
		rebalanceCh: make(chan int64, 2),
		logger:      testClusterLogger(),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for id := range int64(10) {
			n.enqueueRebalance(id)
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("enqueueRebalance заблокировался на переполненной очереди")
	}

	if len(n.rebalanceCh) != 2 {
		t.Fatalf("queued = %d, want 2 (остальные отброшены)", len(n.rebalanceCh))
	}
}

func TestRebalanceWorkerStopsWithContext(t *testing.T) {
	n := &NodeCoordinator{
		rebalanceCh: make(chan int64, 1),
		logger:      testClusterLogger(),
	}
	ctx, cancel := context.WithCancel(context.Background())

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		n.runRebalanceWorker(ctx)
	}()

	cancel()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("runRebalanceWorker не завершился по отмене контекста")
	}
}

func TestSleepCtxReturnsFalseOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if sleepCtx(ctx, time.Hour) {
		t.Fatal("sleepCtx must return false for a cancelled context")
	}
	if !sleepCtx(context.Background(), time.Millisecond) {
		t.Fatal("sleepCtx must return true after the delay")
	}
}

// Джиттер обязан оставаться в пределах [d/2, 3d/2): без разброса все ноды,
// потерявшие наблюдение из-за одной компакции, переподключатся синхронно.
func TestJitterStaysInBounds(t *testing.T) {
	const d = time.Second
	for range 200 {
		got := jitter(d)
		if got < d/2 || got >= d/2+d {
			t.Fatalf("jitter(%v) = %v, out of bounds", d, got)
		}
	}
	if jitter(0) != 0 {
		t.Fatal("jitter(0) must be 0")
	}
}
