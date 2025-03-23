package io

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/naylorpmax-joyent/pipe"
)

// Pool implements pipe.Sink and writes regions using a pool of writers
func Pool(buff Buffer, writers ...io.WriterAt) *pool {
	p := make(chan io.WriterAt, len(writers))
	for i := range writers {
		p <- writers[i]
	}

	return &pool{
		buff:    buff,
		writers: p,
	}
}

type pool struct {
	writers chan io.WriterAt
	buff    Buffer
}

func (p *pool) Read(ctx context.Context, source <-chan pipe.Region, errs chan<- error) {
	var waiter sync.WaitGroup
	for {
		data, more := <-source
		if !more || ctx.Err() != nil {
			// all out of data to write !
			break
		}

		waiter.Add(1)
		// acquire an idle writer from the pool
		writer := <-p.writers
		go func() {
			written := 0
			for written < len(data.Data) {
				n, err := writer.WriteAt(data.Data[written:], data.Off)
				if err != nil {
					errs <- fmt.Errorf("error writing regions: %w", err)
					return
				}
				written += n
			}

			p.writers <- writer   // release writer
			p.buff.Put(data.Data) // release buffer
			waiter.Done()
		}()
	}

	waiter.Wait()
	errs <- nil
}

func Sink(w io.WriterAt, b Buffer) *sink {
	return &sink{w: w, buff: b}
}

type sink struct {
	w    io.WriterAt
	buff Buffer
}

func (w *sink) Read(ctx context.Context, source <-chan pipe.Region, errs chan<- error) {
	for {
		data, more := <-source
		if !more || ctx.Err() != nil {
			// all out of data to write !
			break
		}

		written := 0
		for written < len(data.Data) {
			n, err := w.w.WriteAt(data.Data[written:], data.Off)
			if err != nil {
				errs <- fmt.Errorf("error writing region: %w", err)
				return
			}
			written += n
		}

		w.buff.Put(data.Data) // release buffer
	}

	errs <- nil
}
