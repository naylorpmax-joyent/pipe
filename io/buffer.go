package io

import "sync"

// Buffer is basically a sync.Pool except a) objects can't get evicted and b) there's
// a soft limit on the number of objects that can be allocated at once
type Buffer interface {
	Put(buff []byte)
	Get() []byte
}

func NewBuffer(bufferSize, poolSize int) Buffer {
	return &pooledBuffer{pool: make(chan []byte, poolSize), size: bufferSize}
}

type pooledBuffer struct {
	pool chan []byte
	size int
}

func (b *pooledBuffer) Put(buff []byte) {
	select {
	case b.pool <- buff:
	default:
	}
}

func (b *pooledBuffer) Get() []byte {
	select {
	case buff := <-b.pool:
		return buff
	default:
		return make([]byte, b.size)
	}
}

// sync.Pool-based implementation just for comparison (the memory usage tends to
// be multiple scales of magnititude higher than the channel-based implementation
// though in the bench results, presumably because the pool size is unlimited and
// nothing's stopping the reader from being greedy with its buffers)
type syncBuffer struct {
	pool *sync.Pool
}

//nolint:unused
func NewSyncBuffer(bufferSize int) Buffer {
	return &syncBuffer{pool: &sync.Pool{
		New: func() any {
			x := make([]byte, bufferSize)
			return &x
		},
	}}
}

//nolint:unused
func (b *syncBuffer) Put(x []byte) {
	b.pool.Put(&x)
}

//nolint:unused
func (b *syncBuffer) Get() []byte {
	x := b.pool.Get().(*[]byte)
	return *x
}
