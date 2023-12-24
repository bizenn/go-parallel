package parallel

import (
	"context"
	"testing"
)

func genRange(start, end, step int) TapFunc[int] {
	val := start
	return func(context.Context) (int, error) {
		v := val
		if v >= end {
			return v, ErrEOD
		}
		val += step
		return v, nil
	}
}

func TestJoin(t *testing.T) {
	var (
		start = 0
		end   = 10000
		step  = 1
	)
	ctx := context.Background()
	tap1 := NewTap(genRange(start, end, step))
	p1_mapper1 := NewMapper(5, func(ctx context.Context, in int) (int, error) {
		return in * in, nil
	})
	p1_reducer1 := NewReducer(3, func(ctx context.Context, in int, out []int) ([]int, bool, error) {
		var flush bool
		out = append(out, in)
		if len(out) >= 7 {
			flush = true
		}
		return out, flush, nil
	})
	p1_spreader1 := NewSpreader(3, func(ctx context.Context, in []int) ([]int, error) {
		return in, nil
	})
	p1_reducer2 := NewReducer(1, func(ctx context.Context, in int, out int) (int, bool, error) {
		return in + out, false, nil
	})
	tap1.Join(p1_mapper1)
	p1_mapper1.Join(p1_reducer1)
	p1_reducer1.Join(p1_spreader1)
	p1_spreader1.Join(p1_reducer2)
	tap1.Run(ctx)

	result1 := <-p1_reducer2.Out()
	var expected1 int
	for v := start; v < end; v += step {
		expected1 += v * v
	}
	if result1.Value() != expected1 {
		t.Errorf("%v expected but got %v", expected1, result1.Value())
	}
}

func TestCompose(t *testing.T) {
	var (
		start = 0
		end   = 10000
		step  = 1
	)
	ctx := context.Background()
	tap1 := NewTap(genRange(start, end, step))
	p1_mapper1 := NewMapper(5, func(ctx context.Context, in int) (int, error) {
		return in * in, nil
	})
	p1_reducer1 := NewReducer(3, func(ctx context.Context, in int, out []int) ([]int, bool, error) {
		var flush bool
		out = append(out, in)
		if len(out) >= 7 {
			flush = true
		}
		return out, flush, nil
	})
	p1_spreader1 := NewSpreader(3, func(ctx context.Context, in []int) ([]int, error) {
		return in, nil
	})
	p1_reducer2 := NewReducer(1, func(ctx context.Context, in int, out int) (int, bool, error) {
		return in + out, false, nil
	})
	p1 := Compose(Compose(Compose(p1_mapper1, p1_reducer1), p1_spreader1), p1_reducer2)
	tap1.Join(p1)
	tap1.Run(ctx)

	result1 := <-p1_reducer2.Out()
	var expected1 int
	for v := start; v < end; v += step {
		expected1 += v * v
	}
	if result1.Value() != expected1 {
		t.Errorf("%v expected but got %v", expected1, result1.Value())
	}
}
