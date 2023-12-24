package parallel

import (
	"context"
	"testing"
)

func TestReducer(t *testing.T) {
	ctx := context.Background()
	src1 := []int{0, 1, 2, 3, 4, 5}
	tap1 := NewTapFromSlice(src1)
	tap1.Run(ctx)
	reducer1 := NewReducer(1, func(ctx context.Context, in, out int) (int, bool, error) {
		out += in
		return out, false, nil
	})
	reducer1.In(tap1.Out())
	reducer1.Run(ctx)

	result1 := <-reducer1.Out()
	expected := 15
	if result1.Value() != expected {
		t.Errorf("%v expected but got %v", expected, result1)
	}
}
