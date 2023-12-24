package parallel

import (
	"context"
	"reflect"
	"testing"
)

func TestFilter(t *testing.T) {
	ctx := context.Background()
	src1 := []int{0, 1, 2, 3, 4, 5}
	tap1 := NewTapFromSlice(src1)
	tap1.Run(ctx)
	filter1 := NewFilter(1, func(ctx context.Context, in int) (bool, error) {
		return (in%2 == 0), nil
	})
	filter1.In(tap1.Out())
	filter1.Run(ctx)

	result1 := []int{}
	for v := range filter1.Out() {
		result1 = append(result1, v.Value())
	}
	expected := []int{0, 2, 4}
	if !reflect.DeepEqual(result1, expected) {
		t.Errorf("%v expected but got %v", expected, result1)
	}
}
