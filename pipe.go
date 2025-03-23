package pipe

import (
	"context"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
)

// Source writes Regions to the sink channel. Sources are responsible for closing
// the sink channel when it is done producing regions (to communicate this to
// the downstream reader).
//
// To interrupt execution, a Source can place an error on the errs channel. If the
// Source detects that execution has been interrupted by another component via the
// context, the Source should exit gracefully.
type Source interface {
	Write(ctx context.Context, sink chan Region, errs chan error)
}

// Sink reads Regions from the source channel. Sinks are responsible for placing
// the result of the execution on the errs channel once the source channel has
// been closed by the upstream writer and the regions have been drained.
//
// To interrupt execution, a Sink can place an error on the errs channel. If the
// Sink detects that execution has been interrupted by another component via the
// context, the Sink should exit gracefully.
type Sink interface {
	Read(ctx context.Context, source <-chan Region, errs chan<- error)
}

// Valve reads Regions from its source channel and writes Regions to its sink
// channel. Valves are responsible for closing their sink channel to communicate
// to the downstream reader that there are no more Regions to read.
//
// To interrupt execution, a Valve can place an error on the errs channel. If the
// Valve detects that execution has been interrupted by another component via the
// context, the Valve should exit gracefully.
type Valve interface {
	// Open is a non-blocking method that returns the channel off of which the Valve
	// will read regions from.
	Open(ctx context.Context, sink chan Region, errs chan error) (source chan Region)
}

// Region is a piece of contiguous data with a reference to its offset in the overall
// data stream.
type Region struct {
	Data []byte
	Off  int64
}

// New constructs a new pipe that streams a sequence of Regions from a Source to a Sink,
// and optionally through a sequence of Valves. The components of the pipe are connected
// to each other through channels, each of which serves as the sink for the upstream
// component and the source for the downstream component.
//
// For example, a pipe with two Valves will have three intermediary channels (ignoring
// any fan-out or fan-in channels):
//
// [Source] --> chan a --> [Valve] --> chan b --> [Valve] --> chan c --> [Sink]
func New(source Source, sink Sink, valves ...Valve) *Pipe {
	return &Pipe{
		source: source,
		sink:   sink,
		valves: valves,
	}
}

type Pipe struct {
	source Source
	sink   Sink
	valves []Valve
}

// Pipe executes the pipe, first connecting each of its components together and then
// turning on each component concurrently to handle the stream of regions.
//
// Once all components are running, Pipe will wait until either one of the following
// occurs:
//   - execution is done: a nil error has been placed on the `done` channel by the Sink
//   - execution has been interrupted: an error has been placed on the `done` channel
//     by one of the components; this error is returned to the caller
//   - execution timed out: the context is done
//
// Finally, Pipe will close the connector channels (sink to source / in "reverse" order)
// to ensure no goroutines are left running.
func (p *Pipe) Pipe(ctx context.Context) error {
	// go p.logGoroutines()

	// communicate to all components via the context if the execution is interrupted
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan error, 1)

	// hook up the valves by passing the sink channel of each valve to the previous valve;
	// data flows through valves sequentially, in the order they are provided
	connectors := p.open(ctx, done)

	// pipe data from each reader onto an idle writer
	go func() {
		// source pushes region onto the first sink channel
		first := connectors[len(connectors)-1]
		go p.source.Write(ctx, first, done)

		// write takes region off of the last sink channel
		last := connectors[0]
		p.sink.Read(ctx, last, done)
	}()

	// wait for `something` to happen . . .
	select {
	case err := <-done:
		cancel()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pipe) open(ctx context.Context, done chan error) []chan Region {
	connectors := make([]chan Region, len(p.valves)+1)
	connectors[0] = make(chan Region)

	i := 1
	out := connectors[0]
	for back := len(p.valves) - 1; back >= 0; back-- {
		in := p.valves[back].Open(ctx, out, done)
		out = in

		connectors[i] = in
		i++
	}

	return connectors
}
