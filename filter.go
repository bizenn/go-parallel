package parallel

import (
	"context"
	"sync"
)

type Filter[T any] struct {
	Processor[Data[T], Data[T]]
	in         <-chan Data[T]
	out        chan Data[T]
	concurrent int
	predicate  Predicate[T]
	wg         *sync.WaitGroup
}

type Predicate[T any] func(context.Context, T) (bool, error)

func NewFilter[T any](concurrent int, pred Predicate[T]) *Filter[T] {
	out := make(chan Data[T])
	return &Filter[T]{
		out:        out,
		concurrent: concurrent,
		predicate:  pred,
		wg:         &sync.WaitGroup{},
	}
}

func (filter *Filter[T]) Join(c Consumer[Data[T]]) {
	c.In(filter.Out())
}

func (filter *Filter[T]) In(in <-chan Data[T]) {
	filter.in = in
}

func (filter *Filter[T]) Out() <-chan Data[T] {
	return filter.out
}

func (filter *Filter[T]) Run(ctx context.Context) {
	for i := 0; i < filter.concurrent; i++ {
		filter.wg.Add(1)
		go func() {
		loop:
			for {
				select {
				case <-ctx.Done():
					break loop
				case in, ok := <-filter.in:
					if !ok {
						break loop
					}
					if in.Err() != nil {
						filter.out <- in
					} else if cond, err := filter.predicate(ctx, in.Value()); err != nil {
						filter.out <- NewData(in.Value(), err)
					} else if cond {
						filter.out <- in
					}
				}
			}
			filter.wg.Done()
		}()
		go func() {
			filter.wg.Wait()
			close(filter.out)
		}()
	}
}
