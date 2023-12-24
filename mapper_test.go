package parallel

import (
	"context"
	"reflect"
	"testing"
)

func TestMapper(t *testing.T) {
	ctx := context.Background()
	src1 := []int{0, 1, 2, 3, 4, 5}
	tap1 := NewTapFromSlice(src1)
	tap1.Run(ctx)
	mapper1 := NewMapper(1, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	mapper1.In(tap1.Out())
	mapper1.Run(ctx)

	result1 := []int{}
	for d := range mapper1.Out() {
		result1 = append(result1, d.Value())
	}
	expected1 := []int{0, 2, 4, 6, 8, 10}
	if !reflect.DeepEqual(result1, expected1) {
		t.Errorf("%v expected but got %v", expected1, result1)
	}
}
