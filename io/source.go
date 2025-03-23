package io

import (
	"bufio"
	"context"
	"errors"
	"io"

	"github.com/naylorpmax-joyent/pipe"
)

// Source implements pipe.Source
func Source(r io.Reader, off int64, buff Buffer) pipe.Source {
	return &source{
		r:    r,
		off:  off,
		buff: buff,
	}
}

type source struct {
	r   io.Reader
	off int64

	buff Buffer
}

func (b *source) Write(ctx context.Context, sink chan pipe.Region, errs chan error) {
	defer close(sink)

	reader := bufio.NewReader(b.r)

	var done bool
	for !done || ctx.Err() != nil {
		data := b.buff.Get()
		n, err := reader.Read(data)
		if err != nil && !errors.Is(err, io.EOF) {
			errs <- err
			return
		} else if errors.Is(err, io.EOF) || n == 0 {
			done = true
		} else if err != nil {
			errs <- err
			return
		}
		if n == 0 {
			break
		}

		r := pipe.Region{Data: data[:n], Off: b.off}
		sink <- r
		b.off += int64(n)
	}
}
