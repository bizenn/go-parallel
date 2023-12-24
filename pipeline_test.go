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

func TestPipeline(t *testing.T) {
	ctx := context.Background()
	tap1 := NewTap(genRange(0, 100, 1))
	tap1.Run(ctx)

	p1_mapper1 := NewMapper(5, func(ctx context.Context, in int) (int, error) {
		return in * in, nil
	})
	p1_reducer1 := NewReducer(1, func(ctx context.Context, in int, out int) (int, bool, error) {
		return in + out, false, nil
	})
	p1 := Compose(p1_mapper1, p1_reducer1)
	p1.In(tap1.Out())
	p1.Run(ctx)

	result1 := <-p1.Out()
	var expected1 int
	for v := 0; v < 100; v += 1 {
		expected1 += v * v
	}
	if result1.Value() != expected1 {
		t.Errorf("%v expected but got %v", expected1, result1)
	}
}
