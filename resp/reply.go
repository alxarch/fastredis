package resp

import (
	"bufio"
	"bytes"
	"errors"
)

// Reply is a reply for a redis command.
type Reply struct {
	values []value
	buffer []byte
	n      int // number of read values
}

// ParseValue parses a RESP value from a buffer
func ParseValue(b []byte) (Value, error) {
	rep := new(Reply)
	r := bytes.NewReader(b)
	return rep.ReadFrom(bufio.NewReader(r))

}

// Value is a value in a redis reply.
type Value struct {
	id    int
	reply *Reply
}

type value struct {
	start int
	end   int
	num   int64
	typ   byte
	arr   []int
}

// Reset resets a reply invalidating any Value pointing to it.
func (reply *Reply) Reset() {
	reply.n = 0
	reply.buffer = reply.buffer[:0]
}

// Null returns a Null value
func Null() Value {
	return Value{-1, nil}
}

// Value returns the root Value of a reply or a NullValue
func (reply *Reply) Value() Value {
	if reply.n == 0 {
		return Null()
	}
	return Value{id: 0, reply: reply}
}

func (reply *Reply) value() (v *value) {
	if 0 <= reply.n && reply.n < len(reply.values) {
		v = &reply.values[reply.n]
		reply.n++
		return
	}
	tmp := make([]value, 2*len(reply.values)+1)
	copy(tmp, reply.values)
	reply.values = tmp
	if 0 <= reply.n && reply.n < len(reply.values) {
		v = &reply.values[reply.n]
		reply.n++
	}
	return
}

// Reply returns the parent Reply for the Value.
func (v Value) Reply() *Reply {
	return v.reply
}

func (v Value) get() *value {
	if v.reply != nil && 0 <= v.id && v.id < len(v.reply.values) {
		return &v.reply.values[v.id]
	}
	return nil
}

// Get returns the i-th element of an array reply.
func (v Value) Get(i int) Value {
	if vv := v.get(); vv != nil && vv.typ == Array && 0 <= i && i < len(vv.arr) {
		return Value{id: vv.arr[i], reply: v.reply}
	}
	return Null()
}

// Bytes returns the slice of bytes for a value.
func (v Value) Bytes() []byte {
	if vv := v.get(); vv != nil && (vv.typ == SimpleString || vv.typ == BulkString) {
		return vv.slice(v.reply.buffer)
	}
	return nil
}

// Err returns an error if the value is an error value.
func (v Value) Err() error {
	if vv := v.get(); vv != nil && vv.typ == Error {
		return errors.New(string(vv.slice(v.reply.buffer)))
	}
	return nil
}

func (v *value) slice(buf []byte) []byte {
	if 0 <= v.start && v.start <= v.end && v.end <= len(buf) {
		return buf[v.start:v.end]
	}
	return nil
}

// Type returns the type of the value.
func (v Value) Type() byte {
	if vv := v.get(); vv != nil {
		return vv.typ
	}
	return 0
}

// Int retuns the reply as int.
func (v Value) Int() (int64, bool) {
	if vv := v.get(); vv != nil {
		switch vv.typ {
		case Integer:
			return vv.num, true
		case SimpleString, BulkString:
			return btoi(vv.slice(v.reply.buffer))
		}
	}
	return 0, false
}

// IsNull checks if a value is the NullValue.
func (v Value) IsNull() bool {
	if vv := v.get(); vv != nil {
		return vv.num == -1 && (vv.typ == BulkString || vv.typ == Array)
	}
	return v.id == -1
}

// Len returns the number of an array value's elements.
func (v Value) Len() int {
	if vv := v.get(); vv != nil {
		return len(vv.arr)
	}
	return 0
}

func btoi(buf []byte) (int64, bool) {
	var (
		signed bool
		n      int64
	)
	if len(buf) > 0 && buf[0] == '-' {
		signed = true
		buf = buf[1:]
	}
	for _, c := range buf {
		c -= '0'
		if 0 <= c && c <= 9 {
			n = n*10 + int64(c)
		} else {
			return 0, false
		}
	}
	if signed {
		return -n, true
	}
	return n, true
}

// ReadFromN reads n replies from a redis stream.
func (reply *Reply) ReadFromN(r *bufio.Reader, n int64) (Value, error) {
	id := reply.n
	reply.values = reply.values[:cap(reply.values)]
	err := reply.readArray(r, n)
	reply.values = reply.values[:reply.n]
	return Value{id: id, reply: reply}, err
}

func (reply *Reply) readArray(r *bufio.Reader, n int64) error {
	if n < -1 {
		return ProtocolError(`Invalid array size`)
	}
	id := reply.n
	v := reply.value()
	v.typ = Array
	v.num = n
	v.start = -1
	v.end = -1
	v.arr = v.arr[:0]
	if n == -1 {
		return nil
	}
	for i := int64(0); i < n; i++ {
		v.arr = append(v.arr, reply.n)
		if err := reply.read(r); err != nil {
			return err
		}
	}
	reply.values[id] = *v
	return nil
}

// ReadFrom reads a single reply from a redis stream.
func (reply *Reply) ReadFrom(r *bufio.Reader) (Value, error) {
	id := reply.n
	reply.values = reply.values[:cap(reply.values)]
	err := reply.read(r)
	reply.values = reply.values[:reply.n]
	return Value{id: id, reply: reply}, err
}

func (reply *Reply) read(r *bufio.Reader) error {
	typ, err := r.ReadByte()
	if err != nil {
		return err
	}
	switch typ {
	case Error, SimpleString:
		start := len(reply.buffer)
		reply.buffer, err = readLine(reply.buffer, r)
		if err != nil {
			return err
		}
		v := reply.value()
		v.typ = typ
		v.num = 0
		v.arr = v.arr[:0]
		v.start = start
		v.end = len(reply.buffer)
		return nil
	case Integer:
		var n int64
		n, err = readInt(r)
		if err != nil {
			return err
		}
		v := reply.value()
		v.typ = typ
		v.arr = v.arr[:0]
		v.start = -1
		v.num = n
		return nil
	case BulkString:
		var n int64
		n, err = readInt(r)
		if err != nil {
			return err
		}
		start := len(reply.buffer)
		reply.buffer, err = ReadBulkString(reply.buffer, n, r)
		if err != nil {
			return err
		}
		v := reply.value()
		v.start = start
		v.num = n
		v.typ = typ
		v.arr = v.arr[:0]
		v.end = len(reply.buffer)
		return nil
	case Array:
		var n int64
		n, err = readInt(r)
		if err != nil {
			return err
		}
		return reply.readArray(r, n)
	default:
		return ProtocolError(`Invalid RESP value type`)
	}
}

func (reply *Reply) get(id int) *value {
	if reply != nil && 0 <= id && id < len(reply.values) {
		return &reply.values[id]
	}
	return nil
}

// ForEach iterates each value in a BulkStringArray reply
func (v Value) ForEach(fn func(v Value)) {
	if fn == nil {
		return
	}
	if vv := v.reply.get(v.id); vv != nil || vv.typ == Array {
		for _, id := range vv.arr {
			fn(Value{id: id, reply: v.reply})
		}
	}
}

// ForEachKV iterates each key value pair in a BulkStringArray reply
func (v Value) ForEachKV(fn func(k []byte, v Value)) {
	if fn == nil {
		return
	}
	if vv := v.reply.get(v.id); vv != nil && vv.typ == Array {
		var k *value
		for i, id := range vv.arr {
			if i%2 == 0 {
				k = v.reply.get(id)
			} else if k != nil {
				fn(k.slice(v.reply.buffer), Value{id: id, reply: v.reply})
				k = nil
			}
		}
		if k != nil {
			fn(k.slice(v.reply.buffer), Null())
		}
	}

}
