package resp

import (
	"math"
	"strconv"
)

// Buffer is a utility buffer to write RESP values
type Buffer struct {
	B       []byte
	scratch []byte
}

// Reset resets the buffer
func (b *Buffer) Reset() {
	b.B = b.B[:0]
}

// SimpleString writes a RESP simple string to the buffer
func (b *Buffer) SimpleString(s string) {
	b.B = appendSimpleString(b.B, s)
}

// BulkString writes a RESP bulk string to the buffer
func (b *Buffer) BulkString(s string) {
	b.B = appendBulkString(b.B, s)
}

// BulkStringBytes writes a raw RESP bulk string to the buffer
func (b *Buffer) BulkStringBytes(data []byte) {
	b.B = appendBulkStringRaw(b.B, data)
}

// Error writes a RESP error to the buffer
func (b *Buffer) Error(err string) {
	b.B = appendError(b.B, err)
}

// Int writes a RESP integer to the buffer
func (b *Buffer) Int(n int64) {
	b.B = appendInt(b.B, n)
}

// Array writes a RESP array header to the buffer
func (b *Buffer) Array(size int) {
	b.B = appendArray(b.B, size)
}

// NullArray writes a null RESP array to the buffer
func (b *Buffer) NullArray() {
	b.B = appendNullArray(b.B)
}

// NullString writes a null RESP bulk string to the buffer
func (b *Buffer) NullString() {
	b.B = appendNullBulkString(b.B)
}

// BulkStringArray writes an array of RESP bulk strings to the buffer
func (b *Buffer) BulkStringArray(values ...string) {
	b.B = appendBulkStringArray(b.B, values...)
}

// IntArray writes an array of RESP integers to the buffer
func (b *Buffer) IntArray(values ...int64) {
	b.B = appendIntArray(b.B, values...)
}

// Arg writes RESP command arguments to the buffer
func (b *Buffer) Arg(args ...Arg) {
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
