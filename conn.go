package redis

import (
	"bufio"
	"net"
	"time"

	"github.com/alxarch/fastredis/resp"
)

type Conn struct {
	conn       net.Conn
	r          *bufio.Reader
	err        error
	lastUsedAt time.Time
	createdAt  time.Time
	options    *ConnOptions
}

type ConnOptions struct {
	Address        net.Addr
	ReadBufferSize int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	WriteOnly      bool
	// MaxRetries     int
	// RetryBackoff   time.Duration
}

const DefaultPort = 6379

func (options *ConnOptions) Addr() net.Addr {
	if options == nil || options.Address == nil {
		return &net.TCPAddr{
			IP:   net.ParseIP("0.0.0.0"),
			Port: DefaultPort,
		}

	}
	return options.Address
}

func Dial(options *ConnOptions) (*Conn, error) {
	if options == nil {
		options = new(ConnOptions)
	}
	addr := options.Addr()
	conn, err := net.Dial(addr.Network(), addr.String())
	if err != nil {
		return nil, err
	}
	if options.WriteOnly {
		if conn, ok := conn.(closeReader); ok {
			err = conn.CloseRead()
		} else {
			err = errConnWriteOnly
		}
	}
	if err != nil {
		return nil, err
	}

	return NewConn(conn, options), nil
}

const minBufferSize = 4096

type closeReader interface {
	CloseRead() error
}

func NewConn(conn net.Conn, options *ConnOptions) *Conn {
	if options == nil {
		options = new(ConnOptions)
	}
	now := time.Now()
	c := Conn{
		conn:       conn,
		lastUsedAt: now,
		createdAt:  now,
		options:    options,
	}
	size := options.ReadBufferSize
	if size < minBufferSize {
		size = minBufferSize
	}
	c.r = bufio.NewReaderSize(&c, size)
	if options.ReadTimeout <= 0 {
		conn.SetReadDeadline(time.Time{})
	}
	if options.WriteTimeout <= 0 {
		conn.SetWriteDeadline(time.Time{})
	}
	return &c
}

func (c *Conn) Do(p *Pipeline, r *Reply) (err error) {
	if c.err != nil {
		return c.err
	}
	n := p.n
	if n <= 0 {
		return nil
	}
	_, err = c.Write(p.buf)
	if err == nil {
		if r == nil {
			if !c.options.WriteOnly {
				err = resp.Discard(c.r)
			}
		} else if c.options.WriteOnly {
			return errConnWriteOnly
		} else {
			_, err = r.ReadFromN(c.r, n)
		}
	}
	if err != nil {
		err = c.closeWithError(err)
	}
	return
}

func (c *Conn) closeWithError(err error) error {
	if c.err == nil {
		c.err = err
		c.conn.Close()
		c.conn = nil
	}
	return c.err
}

func (c *Conn) Close() error {
	return c.closeWithError(errConnClosed)
}

func (c *Conn) Write(p []byte) (n int, err error) {
	if c.conn == nil {
		return 0, c.closeWithError(errConnClosed)
	}
	if c.options.WriteTimeout > 0 {
		now := time.Now()
		deadline := now.Add(c.options.WriteTimeout)
		c.lastUsedAt = now
		err = c.conn.SetWriteDeadline(deadline)
	}
	if err == nil {
		n, err = c.conn.Write(p)
	}
	if err != nil {
		err = c.closeWithError(err)
	}
	return
}

func (c *Conn) Read(p []byte) (n int, err error) {
	if c.conn == nil {
		return 0, c.closeWithError(errConnClosed)
	}
	if c.err != nil {
		return 0, c.err
	}
	if c.options.WriteOnly {
		return 0, errConnWriteOnly
	}
	if c.options.ReadTimeout > 0 {
		now := time.Now()
		deadline := now.Add(c.options.ReadTimeout)
		c.lastUsedAt = now
		err = c.conn.SetReadDeadline(deadline)
	}
	if err == nil {
		n, err = c.conn.Read(p)
	}
	if err != nil {
		err = c.closeWithError(err)
	}
	return
}
