package parallel

import (
	"context"
)

type Runnable interface {
	Run(context.Context)
}

type Producer[OUT any] interface {
	Out() <-chan OUT
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
	Processor[IN, OUT]
	head  Consumer[IN]
	tail  Producer[OUT]
	procs []Runnable
}

func Compose[IN any, INOUT any, OUT any](p1 Processor[IN, INOUT], p2 Processor[INOUT, OUT]) *Pipeline[IN, OUT] {
	p2.In(p1.Out())
	return &Pipeline[IN, OUT]{
		head:  p1,
		tail:  p2,
		procs: []Runnable{p1, p2},
	}
}

func (pipeline Pipeline[IN, OUT]) In(in <-chan IN) {
	pipeline.head.In(in)
}

func (pipeline Pipeline[IN, OUT]) Out() <-chan OUT {
	return pipeline.tail.Out()
}

func (pipeline Pipeline[IN, OUT]) Run(ctx context.Context) {
	for _, p := range pipeline.procs {
		p.Run(ctx)
	}
}
