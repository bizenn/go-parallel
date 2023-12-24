package parallel

import (
	"context"
	"errors"
	"io"
	"sync"
)

var ErrEOD = errors.New("end of data")

type Tap[OUT any] struct {
	Producer[Data[OUT]]
	Runnable
	out  chan Data[OUT]
	proc TapFunc[OUT]
	wg   *sync.WaitGroup
}

type TapFunc[OUT any] func(context.Context) (OUT, error)

func NewTap[OUT any](f TapFunc[OUT]) *Tap[OUT] {
	out := make(chan Data[OUT])
	return &Tap[OUT]{
		out:  out,
		proc: f,
		wg:   &sync.WaitGroup{},
	}
}

func (src *Tap[OUT]) Out() <-chan Data[OUT] {
	return src.out
}

func (src Tap[OUT]) Run(ctx context.Context) {
	src.wg.Add(1)
	go func() {
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			default:
				d, err := src.proc(ctx)
				if err == ErrEOD {
					break loop
				}
				src.out <- NewData(d, err)
			}
		}
		src.wg.Done()
	}()
	go func() {
		src.wg.Wait()
		close(src.out)
	}()
}

func genFromSlice[T any](vs []T) TapFunc[T] {
	var zero T
	var i int
	return func(context.Context) (T, error) {
		if i >= len(vs) {
			return zero, ErrEOD
		}
		v := vs[i]
		i++
		return v, nil
	}
}

func NewTapFromSlice[T any](vs []T) *Tap[T] {
	return NewTap(genFromSlice(vs))
}

type KeyValue[K comparable, V any] struct {
	Key   K
	Value V
}

func genFromMap[K comparable, V any](m map[K]V) TapFunc[KeyValue[K, V]] {
	s := make([]KeyValue[K, V], 0, len(m))
	for k, v := range m {
		s = append(s, KeyValue[K, V]{
			Key:   k,
			Value: v,
		})
	}
	return genFromSlice(s)
}

func NewTapFromMap[K comparable, V any](m map[K]V) *Tap[KeyValue[K, V]] {
	return NewTap(genFromMap(m))
}

type Decoder interface {
	Decode(any) error
}

func NewTapFromDecoder[V any](dec Decoder) *Tap[V] {
	return NewTap[V](func(ctx context.Context) (V, error) {
		var v V
		if err := dec.Decode(&v); err == io.EOF {
			return v, ErrEOD
		} else if err != nil {
			return v, ErrEOD
		}
		return v, nil
	})
}
