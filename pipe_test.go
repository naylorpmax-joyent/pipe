package pipe_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/naylorpmax-joyent/pipe"
)

var (
	regions = []pipe.Region{
		{
			Off:  0,
			Data: []byte("AAAAAAAAAA"),
		},
		{
			Off:  10,
			Data: []byte("BBBBBBBBBB"),
		},
		{
			Off:  90,
			Data: []byte("JJJJJJJJJJ"),
		},
	}

	sortRegions = func(a, b pipe.Region) int {
		if a.Off < b.Off {
			return -1
		}
		if a.Off == b.Off {
			return 0
		}
		return 1
	}
)

func TestPipe(t *testing.T) {
	type args struct {
		ctx    context.Context
		cancel func()

		source pipe.Source
		sink   pipe.Sink
		valves []pipe.Valve
	}

	type expected struct {
		err    error
		assert func()
	}

	tests := []struct {
		name  string
		setup func() (args, expected)
	}{
		{
			name: "success",
			setup: func() (args, expected) {
				read := make([]pipe.Region, 0)
				sinkFunc := func(r pipe.Region) error {
					read = append(read, r)
					return nil
				}
				args := args{
					source: &source{regions: regions},
					sink:   &sink{f: sinkFunc},
				}
				expected := expected{
					assert: func() { assert.DeepEqual(t, read, regions) },
				}

				return args, expected
			},
		},
		{
			name: "success/one-valve",
			setup: func() (args, expected) {
				read := make([]pipe.Region, 0)
				sinkFunc := func(r pipe.Region) error {
					read = append(read, r)
					return nil
				}

				var firsts string
				valveFunc := func(r pipe.Region) error {
					firsts += string(r.Data[0])
					return nil
				}

				args := args{
					source: &source{regions: regions},
					sink:   &sink{f: sinkFunc},
					valves: []pipe.Valve{
						&noopValve{f: valveFunc},
					},
				}
				expected := expected{
					assert: func() {
						assert.DeepEqual(t, read, regions)
						assert.DeepEqual(t, firsts, "ABJ")
					},
				}

				return args, expected
			},
		},
		{
			name: "success/two-valves",
			setup: func() (args, expected) {
				read := make([]pipe.Region, 0)
				sinkFunc := func(r pipe.Region) error {
					read = append(read, r)
					return nil
				}

				var firsts, seconds string
				valve1Func := func(r pipe.Region) error {
					firsts += string(r.Data[0])
					return nil
				}
				valve2Func := func(r pipe.Region) error {
					seconds += string(r.Data[1])
					return nil
				}

				args := args{
					source: &source{regions: regions},
					sink:   &sink{f: sinkFunc},
					valves: []pipe.Valve{
						&noopValve{f: valve1Func},
						&noopValve{f: valve2Func},
					},
				}
				expected := expected{
					assert: func() {
						assert.DeepEqual(t, read, regions)
						assert.DeepEqual(t, firsts, "ABJ")
						assert.DeepEqual(t, seconds, "ABJ")
					},
				}

				return args, expected
			},
		},
		{
			name: "success/empty-source",
			setup: func() (args, expected) {
				read := make([]pipe.Region, 0)
				sinkFunc := func(r pipe.Region) error {
					read = append(read, r)
					return nil
				}
				args := args{
					source: &source{regions: nil},
					sink:   &sink{f: sinkFunc},
				}
				expected := expected{
					assert: func() { assert.Equal(t, len(read), 0) },
				}

				return args, expected
			},
		},
		{
			name: "success/two-sources",
			setup: func() (args, expected) {
				read := make([]pipe.Region, 0)
				sinkFunc := func(r pipe.Region) error {
					read = append(read, r)
					return nil
				}

				fromA := regions
				fromB := []pipe.Region{
					{Off: 20, Data: []byte("CCCCCCCCCC")},
					{Off: 70, Data: []byte("HHHHHHHHHH")},
				}
				all := append(fromA, fromB...)

				a := &source{regions: fromA}
				b := &source{regions: fromB}

				args := args{
					source: pipe.Fan(a, b),
					sink:   &sink{f: sinkFunc},
				}

				expected := expected{
					assert: func() {
						slices.SortFunc(all, sortRegions)
						slices.SortFunc(read, sortRegions)
						assert.DeepEqual(t, read, all)
					},
				}

				return args, expected
			},
		},
		{
			name: "error/source",
			setup: func() (args, expected) {
				read := make([]pipe.Region, 0)
				sinkFunc := func(r pipe.Region) error {
					read = append(read, r)
					return nil
				}
				args := args{
					source: &source{
						regions: regions[:1],
						err:     errors.New("aw beans"),
					},
					sink: &sink{f: sinkFunc},
				}
				expected := expected{err: errors.New("aw beans")}

				return args, expected
			},
		},
		{
			name: "error/sink",
			setup: func() (args, expected) {
				read := make([]pipe.Region, 0)
				sinkFunc := func(r pipe.Region) error {
					if r.Off == 90 {
						return errors.New("oops! dropped one")
					}

					read = append(read, r)
					return nil
				}

				args := args{
					source: &source{regions: regions},
					sink:   &sink{f: sinkFunc},
				}
				expected := expected{
					err:    errors.New("oops! dropped one"),
					assert: func() { assert.DeepEqual(t, read, regions[:2]) },
				}

				return args, expected
			},
		},
		{
			name: "error/valve",
			setup: func() (args, expected) {
				read := make([]pipe.Region, 0)
				sinkFunc := func(r pipe.Region) error {
					read = append(read, r)
					return nil
				}

				var v int
				valveFunc := func(r pipe.Region) error {
					if v > 0 {
						return errors.New("welp")
					}

					v++
					return nil
				}

				args := args{
					source: &source{regions: regions},
					sink:   &sink{f: sinkFunc},
					valves: []pipe.Valve{
						&noopValve{f: valveFunc},
					},
				}
				expected := expected{
					err:    errors.New("welp"),
					assert: func() { assert.Equal(t, v, 1) },
				}

				return args, expected
			},
		},
		{
			name: "error/ctx",
			setup: func() (args, expected) {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				lazyFunc := func(_ pipe.Region) error {
					time.Sleep(1 * time.Second)
					return nil
				}
				args := args{
					source: &source{regions: regions},
					sink:   &sink{f: lazyFunc},
					ctx:    ctx,
					cancel: cancel,
				}
				expected := expected{
					err: context.DeadlineExceeded,
				}

				return args, expected
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// given
			args, expected := test.setup()

			ctx, cancel := args.ctx, args.cancel
			if ctx == nil {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()

			// when
			p := pipe.New(args.source, args.sink, args.valves...)
			actual := p.Pipe(ctx)

			// then
			if expected.err != nil {
				assert.ErrorContains(t, actual, expected.err.Error())
			} else {
				assert.NilError(t, actual)
			}

			if expected.assert != nil {
				expected.assert()
			}
		})
	}
}

// test implementations

type source struct {
	regions []pipe.Region
	err     error
}

func (s *source) Write(ctx context.Context, sink chan pipe.Region, errs chan error) {
	defer close(sink)

	for _, r := range s.regions {
		sink <- r
	}

	if s.err != nil {
		errs <- s.err
	}
}

type sink struct {
	f func(pipe.Region) error
}

func (s *sink) Read(ctx context.Context, source <-chan pipe.Region, errs chan<- error) {
	for {
		r, more := <-source
		if !more || ctx.Err() != nil {
			break
		}

		if err := s.f(r); err != nil {
			errs <- err
			break
		}
	}

	errs <- nil
}

type noopValve struct {
	f func(pipe.Region) error
}

func (v *noopValve) Open(ctx context.Context, sink chan pipe.Region, errs chan error) chan pipe.Region {
	source := make(chan pipe.Region)
	go func() {
		defer close(sink)

		for {
			r, more := <-source
			if !more || ctx.Err() != nil {
				break
			}

			if err := v.f(r); err != nil {
				errs <- err
				break
			}

			sink <- r
		}
	}()

	return source
}
