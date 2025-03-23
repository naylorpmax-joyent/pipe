package pipe_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/naylorpmax-joyent/pipe"
	pipeio "github.com/naylorpmax-joyent/pipe/io"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
)

func TestPipe_FileToFile(t *testing.T) {
	// given
	setup, err := setupFileToFile(t.Name(), MiB, 1, 1, 4*KiB, 10)
	if setup.close != nil {
		t.Cleanup(setup.close)
	}
	assert.NilError(t, err)

	// when
	assert.NilError(t, setup.pipe.Pipe(context.Background()))

	// then
	assert.NilError(t, diffFiles(setup.dst, setup.src))
}

type setup struct {
	pipe  *pipe.Pipe
	close func()
	src   string
	dst   string
}

func setupFileToFile(name string, fileSize int64, numReaders, numWriters, bufferSize, maxBuffers int, valves ...pipe.Valve) (*setup, error) {
	var setup setup
	cleanup := make([]func(), 0)
	defer func() {
		setup.close = func() {
			for _, c := range cleanup {
				if c != nil {
					c()
				}
			}
		}
	}()

	// create shared/reusable testdata file filled with random data
	// (if doesn't already exist)
	base := fmt.Sprintf("testdata/%dMB.bin", fileSize/MiB)
	if err := fillFile(base, fileSize); err != nil {
		return &setup, err
	}

	// create test-specific file with the same contents as the testdata
	src := fmt.Sprintf("testdata/%s-src.bin", name)
	if err := copyFile(src, base); err != nil {
		return &setup, err
	}
	if err := diffFiles(base, src); err != nil {
		return &setup, err
	}

	// create pipe source: file reader
	var source pipe.Source
	buff := pipeio.NewBuffer(bufferSize, maxBuffers)

	if numReaders == 1 {
		f, err := os.Open(src)
		if err != nil {
			return &setup, err
		}
		cleanup = append(cleanup, func() { _ = f.Close() })

		source = pipeio.Source(f, 0, buff)
	} else {
		sources, close, err := shard(src, numReaders, buff)
		if close != nil {
			cleanup = append(cleanup, close)
		}
		if err != nil {
			return &setup, err
		}

		source = pipe.Fan(sources...)
	}

	// create pipe sink: file writer
	dst := fmt.Sprintf("testdata/%s-dst.bin", name)
	f, err := os.Create(dst)
	if err != nil {
		return &setup, err
	}

	cleanup = append(cleanup, func() {
		_ = f.Close()
		_ = os.Remove(src)
		_ = os.Remove(dst)
	})

	var sink pipe.Sink
	if numWriters == 1 {
		sink = pipeio.Sink(f, buff)
	} else {
		pool, close, err := pool(dst, numWriters, buff)
		if close != nil {
			cleanup = append(cleanup, close)
		}
		if err != nil {
			return &setup, err
		}

		sink = pool
	}

	setup.pipe = pipe.New(source, sink, valves...)
	setup.src = src
	setup.dst = dst

	return &setup, nil
}

func pool(path string, n int, buff pipeio.Buffer) (pipe.Sink, func(), error) {
	writers := make([]io.WriterAt, n)
	closers := make([]func() error, n)

	var close func()
	defer func() {
		close = func() {
			for _, c := range closers {
				if c != nil {
					_ = c()
				}
			}
		}
	}()

	for i := 0; i < n; i++ {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return nil, close, err
		}

		writers[i] = f
		closers[i] = f.Close
	}

	return pipeio.Pool(buff, writers...), close, nil
}

func shard(path string, shards int, bufferPool pipeio.Buffer) ([]pipe.Source, func(), error) {
	// determine total size of the file
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	totalSize := stat.Size()

	// determine max size of each reader
	maxShardSize := int64(math.Ceil(float64(totalSize) / float64(shards)))

	sources := make([]pipe.Source, shards)
	closers := make([]func() error, shards)

	var close func()
	defer func() {
		close = func() {
			for _, c := range closers {
				if c != nil {
					_ = c()
				}
			}
		}
	}()

	for i := 0; i < shards; i++ {
		f, err := os.Open(path)
		if err != nil {
			return nil, close, err
		}

		closers[i] = f.Close

		rOff := maxShardSize * int64(i)
		_, err = f.Seek(rOff, 0)
		if err != nil {
			return nil, close, err
		}

		limited := &io.LimitedReader{R: f, N: int64(maxShardSize)}
		sources[i] = pipeio.Source(limited, rOff, bufferPool)
	}

	return sources, close, nil
}

func copyFile(dst, src string) error {
	dstFile, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

// Copy is yoinked from the io package to serve as a performance baseline.
//
// `But why yoink it when you could just call it directly?` you may ask !
// Turns out io.Copy includes a nifty optimization that avoids allocating a
// buffer for io types that satisfy the right interfaces - such as *os.File.
//
// We want a reasonable baseline (and IRL we'll be working with reader and writer
// types that don't satisfy those interfaces anyway), so we don't WANT to hit
// the optimized path in this context.
func Copy(dst io.Writer, src io.Reader) (written int64, err error) {
	return copyBuffer(dst, src, nil)
}

func copyBuffer(dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	// the above-mentioned optimization:

	// // If the reader has a WriteTo method, use it to do the copy.
	// // Avoids an allocation and a copy.
	// if wt, ok := src.(WriterTo); ok {
	// 	return wt.WriteTo(dst)
	// }
	// // Similarly, if the writer has a ReadFrom method, use it to do the copy.
	// if rf, ok := dst.(ReaderFrom); ok {
	// 	return rf.ReadFrom(src)
	// }

	if buf == nil {
		size := 32 * 1024
		if l, ok := src.(*io.LimitedReader); ok && int64(size) > l.N {
			if l.N < 1 {
				size = 1
			} else {
				size = int(l.N)
			}
		}
		buf = make([]byte, size)
	}
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errInvalidWrite
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

var errInvalidWrite = errors.New("invalid write result")

func fillFile(path string, fill int64) error {
	f, err := os.Open(path)
	if err == nil {
		// file is present, nothing to do
		f.Close()
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("error opening %s: %w", path, err)
	}

	// okay no file yet; let's set that up
	f.Close()

	f, err = os.Create(path)
	if err != nil {
		return fmt.Errorf("error creating %s: %w", path, err)
	}
	defer f.Close()

	if fill == 0 {
		return nil
	}

	filler := &io.LimitedReader{
		R: rand.Reader,
		N: fill,
	}

	_, err = io.Copy(f, filler)
	if err != nil {
		return fmt.Errorf("error filling %s with %d bytes: %w", path, fill, err)
	}

	return nil
}

func diffFiles(a, b string) error {
	fileA, err := os.Open(a)
	if err != nil {
		return err
	}
	defer fileA.Close()

	fileB, err := os.Open(b)
	if err != nil {
		return err
	}
	defer fileB.Close()

	statA, err := fileA.Stat()
	if err != nil {
		return err
	}
	statB, err := fileB.Stat()
	if err != nil {
		return err
	}

	if statA.Size() != statB.Size() {
		return fmt.Errorf("file %s (size %d) is not same size as file %s (size %d)",
			a, statA.Size(), b, statB.Size(),
		)
	}

	buffA := make([]byte, 4*KiB)
	buffB := make([]byte, 4*KiB)

	var done bool
	var off int64
	for !done {
		n, err := fileA.Read(buffA)
		if errors.Is(err, io.EOF) || n == 0 {
			done = true
		} else if err != nil {
			return err
		}
		if n == 0 {
			break
		}

		m, err := fileB.Read(buffB)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		if n != m {
			return fmt.Errorf("well that's annoying... n=%d, m=%d)", n, m)
		}

		off += int64(n)
		if !bytes.Equal(buffA, buffB) {
			return fmt.Errorf("contents of file=%s do not equal file=%s at offset=%d", a, b, off)
		}
	}

	return nil
}

type delayValve struct {
	delay time.Duration
}

func (d delayValve) Open(ctx context.Context, out chan pipe.Region, errs chan error) chan pipe.Region {
	in := make(chan pipe.Region)
	go func() {
		defer close(out)
		for {
			time.Sleep(d.delay)
			r, more := <-in
			if !more || ctx.Err() != nil {
				break
			}

			out <- r
		}
	}()

	return in
}
