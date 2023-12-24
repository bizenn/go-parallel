package parallel

import (
	"context"
	"reflect"
	"testing"
)

func TestSpreader(t *testing.T) {
	ctx := context.Background()
	src1 := [][]int{{0, 1, 2, 3, 4, 5}, {6, 7, 8}}
	tap1 := NewTapFromSlice(src1)
	tap1.Run(ctx)
	spreader1 := NewSpreader[[]int, int](1, func(ctx context.Context, vs []int) ([]int, error) {
		return vs, nil
	})
	spreader1.In(tap1.Out())
	spreader1.Run(ctx)

	result1 := []int{}
	for d := range spreader1.Out() {
		result1 = append(result1, d.Value())
	}
	expected1 := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	if !reflect.DeepEqual(result1, expected1) {
		t.Errorf("%v expected but got %v", expected1, result1)
	}
}
