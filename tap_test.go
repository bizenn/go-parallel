package parallel

import (
	"context"
	"reflect"
	"testing"
)

func TestTapFromSlice(t *testing.T) {
	src1 := []int{0, 1, 2, 3, 4, 5}
	tap1 := NewTapFromSlice(src1)
	tap1.Run(context.Background())

	result1 := []int{}
	for v := range tap1.Out() {
		if v.Err() != nil {
			t.Errorf("Err %v expected but got %v", nil, v.Err())
		}
		result1 = append(result1, v.Value())
	}
	if !reflect.DeepEqual(src1, result1) {
		t.Errorf("%v expected but got %v", src1, result1)
	}

	src2 := []string{"0", "1", "2", "3", "4", "5"}
	tap2 := NewTapFromSlice(src2)
	tap2.Run(context.Background())

	result2 := []string{}
	for v := range tap2.Out() {
		if v.Err() != nil {
			t.Errorf("Err %v expected but got %v", nil, v.Err())
		}
		result2 = append(result2, v.Value())
	}
	if !reflect.DeepEqual(result2, src2) {
		t.Errorf("%v expected but got %v", src2, result2)
	}
}

func TestTapFromMap(t *testing.T) {
	src1 := map[string]int{
		"zero":  0,
		"one":   1,
		"two":   2,
		"three": 3,
		"four":  4,
		"five":  5,
	}
	tap1 := NewTapFromMap(src1)
	tap1.Run(context.TODO())

	result1 := map[string]int{}
	for v := range tap1.Out() {
		if v.Err() != nil {
			t.Errorf("Err %v expected but got %v", nil, v.Err())
		}
		result1[v.Value().Key] = v.Value().Value
	}
	if !reflect.DeepEqual(src1, result1) {
		t.Errorf("%v expected but got %v", src1, result1)
	}

	src2 := map[int]string{
		0: "zero",
		1: "one",
		2: "two",
		3: "three",
		4: "four",
		5: "five",
	}
	tap2 := NewTapFromMap(src2)
	tap2.Run(context.Background())

	result2 := map[int]string{}
	for v := range tap2.Out() {
		if v.Err() != nil {
			t.Errorf("Err %v expected but got %v", nil, v.Err())
		}
		result2[v.Value().Key] = v.Value().Value
	}
	if !reflect.DeepEqual(src2, result2) {
		t.Errorf("%v expected but got %v", src2, result2)
	}
}
