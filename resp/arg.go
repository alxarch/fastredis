package resp

import (
	"math"
)

type argType uint

const (
	_ argType = iota
	typKey
	typString
	typBuffer
	typInt
	typUint
	typFloat
	typTrue
	typFalse
)

// Arg is a RESP command argument
type Arg struct {
	typ argType
	str string
	buf []byte
	num uint64
}

// Key creates a string argument to be used as a key.
func Key(s string) Arg {
	return Arg{typ: typKey, str: s}
}

// String createa a string argument.
func String(s string) Arg {
	return Arg{typ: typString, str: s}
}

// Raw creates a byte slice argument.
func Raw(b []byte) Arg {
	return Arg{typ: typBuffer, buf: b}
}

// Uint creates an unsigned int argument.
func Uint(n uint64) Arg {
	return Arg{typ: typUint, num: n}
}

// Int creates an int argument.
func Int(n int64) Arg {
	return Arg{typ: typInt, num: uint64(n)}
}

// Float creates a float argument.
func Float(f float64) Arg {
	return Arg{typ: typFloat, num: math.Float64bits(f)}
}

// Bool creates a boolean argument.
func Bool(b bool) Arg {
	if b {
		return Arg{typ: typTrue}
	}
	return Arg{typ: typFalse}
}

// KV is a pair of key value
type KV struct {
	Key string
	Arg
}

// Pair creates a key value pair argument.
func Pair(key string, arg Arg) KV {
	return KV{Key: key, Arg: arg}
}

// IsKey checks if an arg is key
func (a Arg) IsKey() bool {
	return a.typ == typKey
}

// Prefix prepends a prefix to a key argument
func (a Arg) Prefix(prefix string) Arg {
	if a.typ == typKey {
		return Arg{typ: typKey, str: prefix + a.str}
	}
	return a
}
