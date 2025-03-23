package pipe_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"gotest.tools/v3/assert"
)

// General note on running these benchmarks: using any b.N > 1 may be a bit misleading,
// depending on what information you're interested in - it's decent for relative comparison
// for experimenting with different parameters, but don't interpret the metrics as an accurate
// measure of how fast or how memory-intensive real-life uses will be.
//
// I think the compiler maybe is doing some optimization that I'm not sure how to dodge, or
// perhaps it's partly because the files are warmed-up. Might be worth exploring one day but
// in the meantime just take objective numbers for b.N > 1 with a scoop of salt.

func benchFileToFile(b *testing.B, fileSize int64, numReaders, numWriters, bufferSize, maxBuffers int) {
	// given
	setup, err := setupFileToFile(b.Name(), fileSize, numReaders, numWriters, bufferSize, maxBuffers)
	if setup.close != nil {
		b.Cleanup(setup.close)
	}
	assert.NilError(b, err)

	for i := 0; i < b.N; i++ {
		// when
		assert.NilError(b, setup.pipe.Pipe(context.Background()))

		// then
		assert.NilError(b, diffFiles(setup.dst, setup.src))
	}
}

// baseline for comparison (doesn't use the pipe package at all, just io)

func BenchmarkPipe_IOCopy(b *testing.B) {
	// given
	base := "testdata/1GiB.bin"
	assert.NilError(b, fillFile(base, GiB))

	src := fmt.Sprintf("testdata/1G-%s-src.bin", b.Name())
	assert.NilError(b, copyFile(src, base))
	assert.NilError(b, diffFiles(src, base))

	dst := fmt.Sprintf("testdata/1G-%s-dst.bin", b.Name())

	b.Cleanup(func() { _ = os.Remove(dst) })
	b.Cleanup(func() { _ = os.Remove(src) })

	for i := 0; i < b.N; i++ {
		// when
		assert.NilError(b, copyFile(dst, src))
		// then
		assert.NilError(b, diffFiles(dst, src))
	}
}

// benchmark different buffer sizes and (soft) max buffer count;
// no concurrency (1 reader, 1 writer)

// max 20 buffers

func BenchmarkPipe_4KBuff(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 4*KiB, 20)
}

func BenchmarkPipe_8KBuff(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 8*KiB, 20)
}

func BenchmarkPipe_16KBuff(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 16*KiB, 20)
}

func BenchmarkPipe_32KBuff(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 32*KiB, 20)
}

func BenchmarkPipe_64KBuff(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 64*KiB, 20)
}

func BenchmarkPipe_128KBuff(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 128*KiB, 20)
}

func BenchmarkPipe_256KBuff(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 256*KiB, 20)
}

// max 15 buffers

func BenchmarkPipe_4KBuff_15Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 4*KiB, 15)
}

func BenchmarkPipe_8KBuff_15Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 8*KiB, 15)
}

func BenchmarkPipe_16KBuff_15Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 16*KiB, 15)
}

func BenchmarkPipe_32KBuff_15Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 32*KiB, 15)
}

func BenchmarkPipe_64KBuff_15Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 64*KiB, 15)
}

func BenchmarkPipe_128KBuff_15Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 128*KiB, 15)
}

func BenchmarkPipe_256KBuff_15Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 256*KiB, 15)
}

// max 10 buffers

func BenchmarkPipe_4KBuff_10Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 4*KiB, 10)
}

func BenchmarkPipe_8KBuff_10Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 8*KiB, 10)
}

func BenchmarkPipe_16KBuff_10Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 16*KiB, 10)
}

func BenchmarkPipe_32KBuff_10Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 32*KiB, 10)
}

func BenchmarkPipe_64KBuff_10Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 64*KiB, 10)
}

func BenchmarkPipe_128KBuff_10Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 128*KiB, 10)
}

func BenchmarkPipe_256KBuff_10Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 256*KiB, 10)
}

// max 5 buffers

func BenchmarkPipe_4KBuff_5Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 4*KiB, 5)
}

func BenchmarkPipe_8KBuff_5Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 8*KiB, 5)
}

func BenchmarkPipe_16KBuff_5Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 16*KiB, 5)
}

func BenchmarkPipe_32KBuff_5Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 32*KiB, 5)
}

func BenchmarkPipe_64KBuff_5Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 64*KiB, 5)
}

func BenchmarkPipe_128KBuff_5Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 128*KiB, 5)
}

func BenchmarkPipe_256KBuff_5Buffs(b *testing.B) {
	benchFileToFile(b, GiB, 1, 1, 256*KiB, 5)
}

// benchmark different ratios of concurrent readers/writers;
// use the single ("best"?) buffer size and buffer count
//
// note that these tests just prove there aren't really performance gains to adding
// concurrency when piping data from one file to another. reads and writes are
// roughly equivalent amounts of work and I believe we're IO-bound here, so having
// more readers than writers or vice versa just adds overhead without any real boost

func BenchmarkPipe_2Read_1Write_32KBuff(b *testing.B) {
	benchFileToFile(b, GiB, 2, 1, 32*KiB, 20)
}

func BenchmarkPipe_1Read_2Write_32KBuff(b *testing.B) {
	benchFileToFile(b, GiB, 1, 2, 32*KiB, 20)
}
