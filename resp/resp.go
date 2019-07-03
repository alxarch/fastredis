package resp

import (
	"bufio"
	"errors"
	"strconv"
)

const (
	SimpleString byte = '+'
	Error        byte = '-'
	Integer      byte = ':'
	BulkString   byte = '$'
	Array        byte = '*'
)

var (
	ProtocolError = errors.New("Protocol error")
)

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
		return buf, ProtocolError
	}
}
func ReadLine(buf []byte, r *bufio.Reader) ([]byte, error) {
	line, isPrefix, err := r.ReadLine()
	buf = append(buf, line...)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		buf = append(buf, line...)
	}
	return buf, err
}

func ReadInt(r *bufio.Reader) (int64, error) {
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
			return 0, ProtocolError
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
		n, err = ReadInt(r)
		if err == nil {
			_, err = r.Discard(int(n) + 2)
		}
		return err
	case Array:
		var n int64
		n, err = ReadInt(r)
		for err == nil && n > 0 {
			err = Discard(r)
			n--
		}
		return err
	default:
		r.UnreadByte()
		return ProtocolError
	}
}

func appendCRLF(buf []byte) []byte {
	return append(buf, '\r', '\n')
}

func AppendSimpleString(buf []byte, s string) []byte {
	buf = append(buf, SimpleString)
	buf = append(buf, s...)
	return appendCRLF(buf)
}

func AppendBulkStringRaw(buf []byte, raw []byte) []byte {
	buf = append(buf, BulkString)
	buf = strconv.AppendInt(buf, int64(len(raw)), 10)
	buf = appendCRLF(buf)
	buf = append(buf, raw...)
	return appendCRLF(buf)
}
func AppendBulkString(buf []byte, s string) []byte {
	buf = append(buf, BulkString)
	buf = strconv.AppendInt(buf, int64(len(s)), 10)
	buf = appendCRLF(buf)
	buf = append(buf, s...)
	return appendCRLF(buf)
}

func AppendError(buf []byte, err string) []byte {
	buf = append(buf, Error)
	buf = append(buf, err...)
	return appendCRLF(buf)
}

func AppendInt(buf []byte, n int64) []byte {
	buf = append(buf, Integer)
	buf = strconv.AppendInt(buf, n, 10)
	return appendCRLF(buf)
}

func AppendArray(buf []byte, n int) []byte {
	buf = append(buf, Array)
	buf = strconv.AppendInt(buf, int64(n), 10)
	return appendCRLF(buf)
}
func AppendNullArray(buf []byte) []byte {
	return append(buf, Array, '-', '1', '\r', '\n')
}
func AppendNullBulkString(buf []byte) []byte {
	return append(buf, BulkString, '-', '1', '\r', '\n')
}
func AppendBulkStringArray(buf []byte, values ...string) []byte {
	buf = AppendArray(buf, len(values))
	for _, s := range values {
		buf = AppendBulkString(buf, s)
	}
	return buf
}

func AppendIntArray(buf []byte, values ...int64) []byte {
	buf = AppendArray(buf, len(values))
	for _, n := range values {
		buf = AppendInt(buf, n)
	}
	return buf
}
