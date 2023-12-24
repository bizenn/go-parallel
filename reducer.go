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
					if in.Err() != nil {
						var out OUT
						reducer.out <- NewData(out, in.Err())
					} else {
						out, flush, err = reducer.proc(ctx, in.Value(), out)
						if flush || err != nil {
							reducer.out <- NewData(out, err)
						} else {
							flush = true
						}
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
}
