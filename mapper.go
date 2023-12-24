package parallel

import (
	"context"
	"sync"
)

type Mapper[IN any, OUT any] struct {
	Processor[Data[IN], Data[OUT]]
	in         <-chan Data[IN]
	out        chan Data[OUT]
	concurrent int
	proc       MapFunc[IN, OUT]
	wg         *sync.WaitGroup
}

type MapFunc[IN any, OUT any] func(context.Context, IN) (OUT, error)

func NewMapper[IN any, OUT any](concurrent int, f MapFunc[IN, OUT]) *Mapper[IN, OUT] {
	out := make(chan Data[OUT])
	return &Mapper[IN, OUT]{
		out:        out,
		concurrent: concurrent,
		proc:       f,
		wg:         &sync.WaitGroup{},
	}
}

func (mapper *Mapper[IN, OUT]) Join(c Consumer[Data[OUT]]) {
	c.In(mapper.Out())
}

func (mapper *Mapper[IN, OUT]) In(in <-chan Data[IN]) {
	mapper.in = in
}

func (mapper *Mapper[IN, OUT]) Out() <-chan Data[OUT] {
	return mapper.out
}

func (mapper *Mapper[IN, OUT]) Run(ctx context.Context) {
	if mapper.in == nil {
		return
	}
	for i := 0; i < mapper.concurrent; i++ {
		mapper.wg.Add(1)
		go func() {
		loop:
			for {
				select {
				case <-ctx.Done():
					break loop
				case in, ok := <-mapper.in:
					if !ok {
						break loop
					}
					if in.Err() != nil {
						var out OUT
						mapper.out <- NewData[OUT](out, in.Err())
					} else {
						mapper.out <- NewData[OUT](mapper.proc(ctx, in.Value()))
					}
				}
			}
			mapper.wg.Done()
		}()
	}
	go func() {
		mapper.wg.Wait()
		close(mapper.out)
	}()
}
