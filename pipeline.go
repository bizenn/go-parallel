package parallel

import (
	"context"
)

type Runnable interface {
	Run(context.Context)
}

type Producer[OUT any] interface {
	Out() <-chan OUT
	Join(Consumer[OUT])
	Runnable
}

type Consumer[IN any] interface {
	In(<-chan IN)
	Runnable
}

type Processor[IN any, OUT any] interface {
	Producer[OUT]
	Consumer[IN]
}

type Pipeline[IN any, OUT any] struct {
	Processor[Data[IN], Data[OUT]]
	head  Consumer[Data[IN]]
	tail  Producer[Data[OUT]]
	procs []Runnable
}

func Compose[IN any, INOUT any, OUT any](p1 Processor[Data[IN], Data[INOUT]], p2 Processor[Data[INOUT], Data[OUT]]) *Pipeline[IN, OUT] {
	p1.Join(p2)
	return &Pipeline[IN, OUT]{
		head:  p1,
		tail:  p2,
		procs: []Runnable{p1, p2},
	}
}

func (pipeline *Pipeline[IN, OUT]) Join(c Consumer[Data[OUT]]) {
	c.In(pipeline.Out())
}

func (pipeline *Pipeline[IN, OUT]) In(in <-chan Data[IN]) {
	pipeline.head.In(in)
}

func (pipeline *Pipeline[IN, OUT]) Out() <-chan Data[OUT] {
	return pipeline.tail.Out()
}

func (pipeline *Pipeline[IN, OUT]) Run(ctx context.Context) {
	for _, p := range pipeline.procs {
		p.Run(ctx)
	}
}
