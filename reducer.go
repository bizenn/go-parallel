package parallel

import (
	"context"
	"sync"
)

type Reducer[IN any, OUT any] struct {
	Processor[Data[IN], Data[OUT]]
	in         <-chan Data[IN]
	out        chan Data[OUT]
	concurrent int
	proc       ReduceFunc[IN, OUT]
	wg         *sync.WaitGroup
	next       Runnable
}

type ReduceFunc[IN any, OUT any] func(context.Context, IN, OUT) (OUT, bool, error)

func NewReducer[IN any, OUT any](concurrent int, f ReduceFunc[IN, OUT]) *Reducer[IN, OUT] {
	out := make(chan Data[OUT])
	return &Reducer[IN, OUT]{
		out:        out,
		concurrent: concurrent,
		proc:       f,
		wg:         &sync.WaitGroup{},
	}
}

func (reducer *Reducer[IN, OUT]) Join(c Consumer[Data[OUT]]) {
	c.In(reducer.Out())
	reducer.next = c
}

func (reducer *Reducer[IN, OUT]) In(in <-chan Data[IN]) {
	reducer.in = in
}

func (reducer *Reducer[IN, OUT]) Out() <-chan Data[OUT] {
	return reducer.out
}

func (reducer *Reducer[IN, OUT]) Run(ctx context.Context) {
	for i := 0; i < reducer.concurrent; i++ {
		reducer.wg.Add(1)
		go func() {
			var (
				out   OUT
				flush bool
				err   error
			)
		loop:
			for {
				select {
				case <-ctx.Done():
					break loop
				case in, ok := <-reducer.in:
					if !ok {
						if flush {
							reducer.out <- NewData(out, nil)
						}
						break loop
					}
					var init OUT
					if in.Err() != nil {
						reducer.out <- NewData(init, in.Err())
						continue loop
					}
					out, flush, err = reducer.proc(ctx, in.Value(), out)
					if flush {
						reducer.out <- NewData(out, err)
						out = init
						flush = false
					} else {
						if err != nil {
							reducer.out <- NewData(init, err)
						}
						flush = true
					}
				}
			}
			reducer.wg.Done()
		}()
	}
	go func() {
		reducer.wg.Wait()
		close(reducer.out)
	}()
	if reducer.next != nil {
		reducer.next.Run(ctx)
	}
}
