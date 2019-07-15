// Package resp provides RESP protocol serialization and deserialization
package resp

import (
	"bufio"
	"strconv"
)

// Value types
const (
	SimpleString byte = '+'
	Error        byte = '-'
	Integer      byte = ':'
	BulkString   byte = '$'
	Array        byte = '*'
)

// ProtocolError is a RESP protocol error
type ProtocolError string

func (e ProtocolError) Error() string {
	return string(e)
}
func (e ProtocolError) String() string {
	return string(e)
}

func ReadBulkString(buf []byte, size int64, r *bufio.Reader) ([]byte, error) {
	switch {
	case 0 < size && size <= int64(r.Size()):
		n := int(size)
		peek, err := r.Peek(n)
		buf = append(buf, peek...)
		if err == nil {
			_, err = r.Discard(n + 2)
		}
		return buf, err
	case size > 0:
		var (
			err error
			nn  int
			n   = int64(len(buf))
		)
		size = n + size
		if size > int64(cap(buf)) {
			tmp := make([]byte, size)
			copy(tmp, buf)
			buf = tmp
		}
		for err == nil && n < size {
			nn, err = r.Read(buf[n:])
			n += int64(nn)
		}
		if err == nil {
			_, err = r.Discard(2)
		}
		return buf[:n], err
	case size == 0:
		_, err := r.Discard(2)
		return buf, err
	case size == -1:
		return buf, nil
	default:
		return buf, ProtocolError(`Invalid bulk string size`)
	}
}

func readLine(buf []byte, r *bufio.Reader) ([]byte, error) {
	line, isPrefix, err := r.ReadLine()
	buf = append(buf, line...)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		buf = append(buf, line...)
	}
	return buf, err
}

func readInt(r *bufio.Reader) (int64, error) {
	line, isPrefix, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	signed := false
	if len(line) > 0 && line[0] == '-' {
		line = line[1:]
		signed = true
	}
	n := int64(0)
	var c byte
btoi:
	for _, c = range line {
		c -= '0'
		if 0 <= c && c <= 9 {
			n = n*10 + int64(c)
		} else {
			return 0, ProtocolError("Invalid integer value")
		}
	}
	if isPrefix {
		line, isPrefix, err = r.ReadLine()
		if err != nil {
			return 0, err
		}
		goto btoi
	}
	if signed {
		return -n, nil
	}
	return n, nil
}

func Discard(r *bufio.Reader) error {
	c, err := r.ReadByte()
	if err != nil {
		return err
	}
	switch c {
	case SimpleString, Error, Integer:
		for {
			_, isPrefix, err := r.ReadLine()
			if err != nil {
				return err
			}
			if !isPrefix {
				return nil
			}
		}
	case BulkString:
		var n int64
		n, err = readInt(r)
		if err == nil {
			_, err = r.Discard(int(n) + 2)
		}
		return err
	case Array:
		var n int64
		n, err = readInt(r)
		for err == nil && n > 0 {
			err = Discard(r)
			n--
		}
		return err
	default:
		r.UnreadByte()
		return ProtocolError(`Invalid RESP type`)
	}
}

func appendCRLF(buf []byte) []byte {
	return append(buf, '\r', '\n')
}

func appendSimpleString(buf []byte, s string) []byte {
	buf = append(buf, SimpleString)
	buf = append(buf, s...)
	return appendCRLF(buf)
}

func appendBulkStringRaw(buf []byte, raw []byte) []byte {
	buf = append(buf, BulkString)
	buf = strconv.AppendInt(buf, int64(len(raw)), 10)
	buf = appendCRLF(buf)
	buf = append(buf, raw...)
	return appendCRLF(buf)
}
func appendBulkString(buf []byte, s string) []byte {
	buf = append(buf, BulkString)
	buf = strconv.AppendInt(buf, int64(len(s)), 10)
	buf = appendCRLF(buf)
	buf = append(buf, s...)
	return appendCRLF(buf)
}

func appendError(buf []byte, err string) []byte {
	buf = append(buf, Error)
	buf = append(buf, err...)
	return appendCRLF(buf)
}

func appendInt(buf []byte, n int64) []byte {
	buf = append(buf, Integer)
	buf = strconv.AppendInt(buf, n, 10)
	return appendCRLF(buf)
}

func appendArray(buf []byte, n int) []byte {
	buf = append(buf, Array)
	buf = strconv.AppendInt(buf, int64(n), 10)
	return appendCRLF(buf)
}
func appendNullArray(buf []byte) []byte {
	return append(buf, Array, '-', '1', '\r', '\n')
}
func appendNullBulkString(buf []byte) []byte {
	return append(buf, BulkString, '-', '1', '\r', '\n')
}
func appendBulkStringArray(buf []byte, values ...string) []byte {
	buf = appendArray(buf, len(values))
	for _, s := range values {
		buf = appendBulkString(buf, s)
	}
	return buf
}

func appendIntArray(buf []byte, values ...int64) []byte {
	buf = appendArray(buf, len(values))
	for _, n := range values {
		buf = appendInt(buf, n)
	}
	return buf
}

type Appender interface {
	AppendRESP(buf []byte) []byte
}
