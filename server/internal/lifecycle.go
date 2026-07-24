package internal

import (
	"context"
	"sync"
)

type Lifecycle struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// DeriveLifecycle отпочковывает дочерний жизненный цикл от родительского контекста
func DeriveLifecycle(parentCtx context.Context) *Lifecycle {
	ctx, cancel := context.WithCancel(parentCtx)
	return &Lifecycle{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (l *Lifecycle) Context() context.Context { return l.ctx }
func (l *Lifecycle) Done() <-chan struct{}    { return l.ctx.Done() }

// Go безопасно запускает фоновый воркер, привязанный к контексту этого компонента
func (l *Lifecycle) Go(worker func(ctx context.Context)) {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		worker(l.ctx) // Передаем воркеру изолированный контекст компонента
	}()
}

// Stop сигнализирует об отмене контекста и жестко ждет завершения всех воркеров (LIFO/FIFO)
func (l *Lifecycle) Stop() {
	l.cancel()  // Каскадно отменяет этот контекст и все контексты, отпочкованные от него ниже
	l.wg.Wait() // Ждет, пока завершатся локальные горутины этого компонента
}
