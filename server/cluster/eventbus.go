package cluster

import (
	"context"
	brokertypes "kafka-clone/server/datatypes/broker"
	"sync"
	"sync/atomic"
)

type EventBus struct {
	ingress     chan brokertypes.ClusterEvent
	subscribe   chan subscribeRequest
	unsubscribe chan uint64
	overflow    chan struct{}
	nextID      atomic.Uint64
}
type subscribeRequest struct {
	id     uint64
	events chan brokertypes.ClusterEvent
	done   chan struct{}
}

func NewClusterEventBus(ctx context.Context) *EventBus {
	bus := &EventBus{
		ingress:     make(chan brokertypes.ClusterEvent, 256),
		subscribe:   make(chan subscribeRequest),
		unsubscribe: make(chan uint64),
		overflow:    make(chan struct{}, 1),
	}

	go bus.run(ctx)

	return bus
}
func (b *EventBus) Publish(event brokertypes.ClusterEvent) bool {
	select {
	case b.ingress <- event:
		return true

	default:
		// Сигнализируем dispatcher, что последовательность потеряна.
		select {
		case b.overflow <- struct{}{}:
		default:
		}

		return false
	}
}

func (b *EventBus) run(ctx context.Context) {
	subscribers := make(map[uint64]chan brokertypes.ClusterEvent)

	closeSubscriber := func(id uint64) {
		if ch, exists := subscribers[id]; exists {
			delete(subscribers, id)
			close(ch)
		}
	}

	for {
		select {
		case <-ctx.Done():
			for id := range subscribers {
				closeSubscriber(id)
			}
			return

		case request := <-b.subscribe:
			subscribers[request.id] = request.events
			close(request.done)

		case id := <-b.unsubscribe:
			closeSubscriber(id)

		case event := <-b.ingress:
			for id, subscriber := range subscribers {
				select {
				case subscriber <- event:
				default:
					// Медленный клиент отключается и получит новый
					// snapshot после автоматического SSE reconnect.
					closeSubscriber(id)
				}
			}

		case <-b.overflow:
			// Были потеряны входящие события. Текущим подписчикам
			// больше нельзя доверять — заставляем их переподключиться.
			for id := range subscribers {
				closeSubscriber(id)
			}
		}
	}
}

type Subscription struct {
	Events      <-chan brokertypes.ClusterEvent
	Unsubscribe func()
}

func (b *EventBus) Subscribe(ctx context.Context) (Subscription, error) {
	id := b.nextID.Add(1)
	events := make(chan brokertypes.ClusterEvent, 64)
	done := make(chan struct{})

	request := subscribeRequest{
		id:     id,
		events: events,
		done:   done,
	}

	select {
	case b.subscribe <- request:
	case <-ctx.Done():
		return Subscription{}, ctx.Err()
	}

	select {
	case <-done:
	case <-ctx.Done():
		return Subscription{}, ctx.Err()
	}

	var once sync.Once

	return Subscription{
		Events: events,
		Unsubscribe: func() {
			once.Do(func() {
				select {
				case b.unsubscribe <- id:
				case <-ctx.Done():
				}
			})
		},
	}, nil
}
