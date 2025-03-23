package pipe

import (
	"context"
	"sync"
)

func Fan(sources ...Source) *fan {
	return &fan{sources: sources}
}

type fan struct {
	sources []Source
}

func (s *fan) Write(ctx context.Context, sink chan Region, errs chan error) {
	// fan out : each source writes to its own separate sink
	sinks := make([]chan Region, len(s.sources))
	for i := range s.sources {
		sinks[i] = make(chan Region)
		go s.sources[i].Write(ctx, sinks[i], errs)
	}

	// fan in : items on the source-specific sinks are written to the final sink
	var waiter sync.WaitGroup
	defer close(sink)
	for i := range sinks {
		waiter.Add(1)
		go func() {
			s.pass(ctx, sinks[i], sink)
			waiter.Done()
		}()
	}

	waiter.Wait()
}

func (b *fan) pass(ctx context.Context, in, out chan Region) {
	for {
		curr, more := <-in
		if !more || ctx.Err() != nil {
			return
		}
		out <- curr
	}
}
