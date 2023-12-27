package parallel

import (
	"context"
	"reflect"
	"testing"
)

type StringMap map[string]string

func (sm StringMap) Clone() any {
	if sm == nil {
		return nil
	}
	result := StringMap{}
	for k, v := range sm {
		result[k] = v
	}
	return result
}

func cat[T Clonable](ctx context.Context, v T) (T, error) {
	return v, nil
}

func appendToSlice[T any](ctx context.Context, sm T, result []T) ([]T, bool, error) {
	if result == nil {
		return []T{sm}, false, nil
	}
	return append(result, sm), false, nil
}

func TestBroadcaster1(t *testing.T) {
	gen := NewTapFromSlice([]StringMap{
		{
			"zero": "0",
			"one":  "1",
			"two":  "2",
		},
		{
			"one":   "1",
			"two":   "2",
			"three": "3",
			"four":  "4",
		},
		{
			"two":   "2",
			"three": "3",
			"four":  "4",
			"five":  "5",
			"six":   "6",
		},
	})
	p0 := Compose(NewMapper(1, cat[StringMap]), NewReducer(1, appendToSlice[StringMap]))
	p1 := Compose(NewMapper(1, cat[StringMap]), NewReducer(1, appendToSlice[StringMap]))
	p2 := Compose(NewMapper(1, cat[StringMap]), NewReducer(1, appendToSlice[StringMap]))
	bc := NewBroadcaster(p0, p1, p2)

	gen.Join(bc)
	gen.Run(context.Background())

	expected := []StringMap{
		{
			"zero": "0",
			"one":  "1",
			"two":  "2",
		},
		{
			"one":   "1",
			"two":   "2",
			"three": "3",
			"four":  "4",
		},
		{
			"two":   "2",
			"three": "3",
			"four":  "4",
			"five":  "5",
			"six":   "6",
		},
	}

	for _, p := range []Producer[Data[[]StringMap]]{p0, p1, p2} {
		result := <-p.Out()
		if !reflect.DeepEqual(expected, result.Value()) {
			t.Errorf("%v expected but got %v", expected, result.Value())
		}
	}
}

func squareValue[K comparable, V int | int8 | int16 | int32 | int64](ctx context.Context, kv *KeyValue[K, V]) (*KeyValue[K, V], error) {
	if kv == nil {
		return nil, nil
	}
	return &KeyValue[K, V]{kv.Key, kv.Value * kv.Value}, nil
}

func makeMap[K comparable, V any](ctx context.Context, kv *KeyValue[K, V], accum map[K]V) (map[K]V, bool, error) {
	if kv == nil {
		return accum, false, nil
	}
	if accum == nil {
		return map[K]V{kv.Key: kv.Value}, false, nil
	}
	accum[kv.Key] = kv.Value
	return accum, false, nil
}

func TestBroadcaster2(t *testing.T) {
	ctx := context.Background()
	gen := NewTapFromMap(map[string]int{
		"zero":  0,
		"one":   1,
		"two":   2,
		"three": 3,
		"four":  4,
		"five":  5,
		"six":   6,
		"seven": 7,
		"eight": 8,
		"nine":  9,
	})
	pls := []Processor[Data[*KeyValue[string, int]], Data[map[string]int]]{
		Compose(NewMapper(1, cat[*KeyValue[string, int]]), NewReducer(1, makeMap[string, int])),
		Compose(NewMapper(1, squareValue[string, int]), NewReducer(1, makeMap[string, int])),
	}
	bc := NewBroadcaster(pls[0], pls[1])
	gen.Join(bc)
	gen.Run(ctx)

	expected := []map[string]int{
		{
			"zero":  0,
			"one":   1,
			"two":   2,
			"three": 3,
			"four":  4,
			"five":  5,
			"six":   6,
			"seven": 7,
			"eight": 8,
			"nine":  9,
		},
		{
			"zero":  0,
			"one":   1,
			"two":   4,
			"three": 9,
			"four":  16,
			"five":  25,
			"six":   36,
			"seven": 49,
			"eight": 64,
			"nine":  81,
		},
	}

	for i := range pls {
		result := <-pls[i].Out()
		if !reflect.DeepEqual(expected[i], result.Value()) {
			t.Errorf("%v expected but got %v", expected[i], result.Value())
		}
	}
}
