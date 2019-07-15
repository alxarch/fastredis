package resp

import (
	"math"
	"strconv"
)

type Buffer struct {
	B       []byte
	scratch []byte
}

func (b *Buffer) Reset() {
	b.B = b.B[:0]
}

func (b *Buffer) SimpleString(s string) {
	b.B = appendSimpleString(b.B, s)
}

func (b *Buffer) BulkString(s string) {
	b.B = appendBulkString(b.B, s)
}
func (b *Buffer) BulkStringBytes(data []byte) {
	b.B = appendBulkStringRaw(b.B, data)
}
func (b *Buffer) Error(err string) {
	b.B = appendError(b.B, err)
}
func (b *Buffer) Int(n int64) {
	b.B = appendInt(b.B, n)
}
func (b *Buffer) Array(size int) {
	b.B = appendArray(b.B, size)
}
func (b *Buffer) NullArray() {
	b.B = appendNullArray(b.B)
}
func (b *Buffer) NullString() {
	b.B = appendNullBulkString(b.B)
}
func (b *Buffer) BulkStringArray(values ...string) {
	b.B = appendBulkStringArray(b.B, values...)
}
func (b *Buffer) IntArray(values ...int64) {
	b.B = appendIntArray(b.B, values...)
}

func (b *Buffer) Arg(args ...Arg) {
	for i := range args {
		a := &args[i]
		b.write(a)
	}
}
func (b *Buffer) ArgArray(args ...Arg) {
	b.Array(len(args))
	for i := range args {
		a := &args[i]
		b.write(a)
	}
}

// write appends an arg to the buffer
// We can't use AppendRESP because numeric types need scratch buffer to append as bulk string
func (b *Buffer) write(a *Arg) {
	switch a.typ {
	case typString, typKey:
		b.B = appendBulkString(b.B, a.str)
	case typBuffer:
		b.B = appendBulkStringRaw(b.B, a.buf)
	case typInt:
		b.scratch = strconv.AppendInt(b.scratch[:0], int64(a.num), 10)
		b.B = appendBulkStringRaw(b.B, b.scratch)
	case typFloat:
		b.scratch = strconv.AppendFloat(b.scratch, math.Float64frombits(a.num), 'f', -1, 64)
		b.B = appendBulkStringRaw(b.B, b.scratch)
	case typUint:
		b.scratch = strconv.AppendUint(b.scratch, a.num, 10)
		b.B = appendBulkStringRaw(b.B, b.scratch)
	case typTrue:
		b.B = appendBulkString(b.B, "true")
	case typFalse:
		b.B = appendBulkString(b.B, "false")
	default:
		b.B = appendNullBulkString(b.B)
	}

}
