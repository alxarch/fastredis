package resp

import (
	"math"
	"strconv"
	"sync"
)

type Buffer struct {
	B       []byte
	scratch []byte
}

func (b *Buffer) Reset() {
	b.B = b.B[:0]
}

func (b *Buffer) WriteSimpleString(s string) {
	b.B = AppendSimpleString(b.B, s)
}

func (b *Buffer) WriteBulkString(s string) {
	b.B = AppendBulkString(b.B, s)
}
func (b *Buffer) WriteBulkStringRaw(raw []byte) {
	b.B = AppendBulkStringRaw(b.B, raw)
}
func (b *Buffer) WriteError(err string) {
	b.B = AppendError(b.B, err)
}
func (b *Buffer) WriteInt(n int64) {
	b.B = AppendInt(b.B, n)
}
func (b *Buffer) WriteArray(size int) {
	b.B = AppendArray(b.B, size)
}
func (b *Buffer) WriteNullArray() {
	b.B = AppendNullArray(b.B)
}
func (b *Buffer) WriteNullString() {
	b.B = AppendNullBulkString(b.B)
}
func (b *Buffer) WriteBulkStrings(values ...string) {
	b.B = AppendBulkStringArray(b.B, values...)
}
func (b *Buffer) WriteInts(values ...int64) {
	b.B = AppendIntArray(b.B, values...)
}

func (b *Buffer) WriteArgs(args ...Arg) {
	for i := range args {
		a := &args[i]
		b.writeArg(a)
	}
}
func (b *Buffer) WriteArgsArray(args ...Arg) {
	b.WriteArray(len(args))
	b.WriteArgs(args...)
}

func (b *Buffer) WriteArg(a Arg) {
	b.writeArg(&a)
}

func (b *Buffer) writeArg(a *Arg) {
	switch a.typ {
	case typString, typKey:
		b.B = AppendBulkString(b.B, a.str)
	case typBuffer:
		b.B = AppendBulkStringRaw(b.B, a.buf)
	case typInt:
		b.scratch = strconv.AppendInt(b.scratch[:0], int64(a.num), 10)
		b.B = AppendBulkStringRaw(b.B, b.scratch)
	case typFloat:
		b.scratch = strconv.AppendFloat(b.scratch, math.Float64frombits(a.num), 'f', -1, 64)
		b.B = AppendBulkStringRaw(b.B, b.scratch)
	case typUint:
		b.scratch = strconv.AppendUint(b.scratch, a.num, 10)
		b.B = AppendBulkStringRaw(b.B, b.scratch)
	case typTrue:
		b.B = AppendBulkString(b.B, "true")
	case typFalse:
		b.B = AppendBulkString(b.B, "false")
	default:
		b.B = AppendNullBulkString(b.B)
	}
}

var bufferPool sync.Pool

func BlankBuffer() *Buffer {
	x := bufferPool.Get()
	if x == nil {
		return new(Buffer)
	}
	return x.(*Buffer)
}

// Close resets and returns a Buffer to the pool.
func (b *Buffer) Close() {
	b.Reset()
	bufferPool.Put(b)
}
