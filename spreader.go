package parallel

import (
	"context"
	"sync"
)

type Spreader[IN any, OUT any] struct {
	Processor[Data[IN], Data[OUT]]
	in         <-chan Data[IN]
	out        chan Data[OUT]
	concurrent int
	proc       SpreadFunc[IN, OUT]
	wg         *sync.WaitGroup
	next       Runnable
}

type SpreadFunc[IN any, OUT any] func(context.Context, IN) ([]OUT, error)

func NewSpreader[IN any, OUT any](concurrent int, f SpreadFunc[IN, OUT]) *Spreader[IN, OUT] {
	out := make(chan Data[OUT])
	return &Spreader[IN, OUT]{
		out:        out,
		concurrent: concurrent,
		proc:       f,
		wg:         &sync.WaitGroup{},
	}
}

func (spreader *Spreader[IN, OUT]) Join(c Consumer[Data[OUT]]) {
	c.In(spreader.Out())
	spreader.next = c
}

func (spreader *Spreader[IN, OUT]) In(in <-chan Data[IN]) {
	spreader.in = in
}

func (spreader *Spreader[IN, OUT]) Out() <-chan Data[OUT] {
	return spreader.out
}

func (spreader *Spreader[IN, OUT]) Run(ctx context.Context) {
	if spreader.in == nil {
		return
	}
	for i := 0; i < spreader.concurrent; i++ {
		spreader.wg.Add(1)
		go func() {
		loop:
			for {
				select {
				case <-ctx.Done():
					break loop
				case in, ok := <-spreader.in:
					if !ok {
						break loop
					}
					var out OUT
					if in.Err() != nil {
						spreader.out <- NewData(out, in.Err())
					} else {
						vs, err := spreader.proc(ctx, in.Value())
						if err != nil {
							spreader.out <- NewData(out, err)
						} else {
							for _, v := range vs {
								spreader.out <- NewData(v, nil)
							}
						}
					}
				}
			}
			spreader.wg.Done()
		}()
	}
	go func() {
		spreader.wg.Wait()
		close(spreader.out)
	}()
	if spreader.next != nil {
		spreader.next.Run(ctx)
	}
}
