package parallel

type Data[T any] interface {
	Value() T
	Err() error
}

type SimpleData[T any] struct {
	value T
	err   error
}

func NewData[T any](v T, err error) *SimpleData[T] {
	return &SimpleData[T]{
		value: v,
		err:   err,
	}
}

func (d *SimpleData[T]) Value() T {
	return d.value
}

func (d *SimpleData[T]) Err() error {
	return d.err
}
