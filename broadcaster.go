package parallel

import (
	"context"
	"sync"
)

type Clonable interface {
	Clone() any
}

type Broadcaster[T Clonable] struct {
	Consumer[Data[T]]
	in    <-chan Data[T]
	outs  []chan Data[T]
	nexts []Runnable
	wg    *sync.WaitGroup
}

func NewBroadcaster[T Clonable](cs ...Consumer[Data[T]]) *Broadcaster[T] {
	if len(cs) == 0 {
		return nil
	}
	b := &Broadcaster[T]{
		outs:  make([]chan Data[T], 0, len(cs)),
		nexts: make([]Runnable, 0, len(cs)),
		wg:    &sync.WaitGroup{},
	}
	for i := range cs {
		out := make(chan Data[T])
		cs[i].In(out)
		b.outs = append(b.outs, out)
		b.nexts = append(b.nexts, cs[i])
	}
	return b
}

func (b *Broadcaster[T]) In(in <-chan Data[T]) {
	b.in = in
}

func (b *Broadcaster[T]) Run(ctx context.Context) {
	if b.in == nil {
		return
	}
	b.wg.Add(1)
	go func() {
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case in, ok := <-b.in:
				if !ok {
					break loop
				}
				// FIXME: check if you can write into out.
				for _, out := range b.outs {
					out <- NewData(in.Value().Clone().(T), in.Err())
				}
			}
		}
		b.wg.Done()
	}()
	go func() {
		b.wg.Wait()
		for i := range b.outs {
			close(b.outs[i])
		}
	}()
	for i := range b.nexts {
		b.nexts[i].Run(ctx)
	}
}
