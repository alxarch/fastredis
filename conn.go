package redis

import (
	"bufio"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/alxarch/fastredis/resp"
)

// Conn is a connectio to a Redis server
type Conn struct {
	conn       net.Conn
	db         int64
	r          *bufio.Reader
	err        error
	lastUsedAt time.Time
	createdAt  time.Time
	options    *ConnOptions
}

// ConnOptions holds connection options
type ConnOptions struct {
	ReadBufferSize int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	WriteOnly      bool
	// MaxRetries     int
	// RetryBackoff   time.Duration
}

// Dial opens a connection to a redis server
func Dial(addr string, options ConnOptions) (*Conn, error) {
	// addr := options.Addr()
	conn, err := net.Dial("tcp", addr)
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

	return newConn(conn, options), nil
}

const minBufferSize = 4096

type closeReader interface {
	CloseRead() error
}

func newConn(conn net.Conn, options ConnOptions) *Conn {
	now := time.Now()
	c := Conn{
		conn:       conn,
		lastUsedAt: now,
		createdAt:  now,
		options:    &options,
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

// Do executes pipeline reading responses into reply
func (c *Conn) Do(pipeline *Pipeline, reply *resp.Reply) (err error) {
	if c.err != nil {
		return c.err
	}
	n := int64(pipeline.Len())
	if n <= 0 {
		return nil
	}
	_, err = c.Write(pipeline.B)
	if err == nil {
		discard := int64(pipeline.offset)
		if reply == nil {
			if c.options.WriteOnly {
				// WriteOnly connection no error
				return nil
			}
			discard += n
			n = 0
		} else if c.options.WriteOnly {
			// WriteOnly connection cannot read reply
			return errConnWriteOnly
		}
		if discard > 0 {
			err = resp.DiscardN(c.r, discard)
			if err == nil && n > 0 {
				_, err = reply.ReadFromN(c.r, n)
			}
		} else if n > 0 {
			_, err = reply.ReadFromN(c.r, n)
		}
	}
	if err != nil {
		err = c.closeWithError(err)
	}
	return
}

// Ping performs a PING to the server
func (c *Conn) Ping() error {
	p := BlankPipeline(0)
	defer ReleasePipeline(p)
	rep := BlankReply()
	defer ReleaseReply(rep)
	if err := c.Do(p, rep); err != nil {
		return err
	}
	pong := rep.Value().Get(0).Bytes()
	if string(pong) != "PONG" {
		return Err("Invalid PING reply")
	}
	return nil
}

// PopPush executes the blocking BRPOPLPUSH command
func (c *Conn) PopPush(src, dst string, timeout time.Duration) (string, error) {
	p := BlankPipeline(c.db)
	defer ReleasePipeline(p)
	p.BRPopLPush(src, dst, timeout)
	rep := BlankReply()
	defer ReleaseReply(rep)
	err := c.Do(p, rep)
	if err != nil {
		return "", err
	}
	value := rep.Value()
	if value.IsNull() {
		return "", new(TimeoutError)
	}
	return string(value.Get(0).Bytes()), nil
}

func (c *Conn) bpop(cmd string, timeout time.Duration, key string, keys []string) (k, v string, score float64, err error) {
	p := BlankPipeline(c.db)
	defer ReleasePipeline(p)
	p.Command(cmd, len(keys)+2)
	p.Arg(resp.Key(key))
	for _, key := range keys {
		p.Arg(resp.Key(key))
	}
	p.Arg(resp.Int(int64(timeout / time.Second)))
	rep := BlankReply()
	defer ReleaseReply(rep)
	err = c.Do(p, rep)
	if err != nil {
		return
	}
	value := rep.Value()
	if value.IsNull() {
		err = new(TimeoutError)
		return
	}
	switch cmd {
	case "BZPOPMAX", "BZPOPMIN":
		k = string(value.Get(0).Bytes())
		v = string(value.Get(2).Bytes())
		switch s := string(value.Get(1).Bytes()); s {
		case "+inf":
			score = math.Inf(+1)
		case "-inf":
			score = math.Inf(-1)
		default:
			score, _ = strconv.ParseFloat(s, 64)
		}
	default:
		k = string(value.Get(0).Bytes())
		v = string(value.Get(1).Bytes())
		score = math.NaN()
	}
	return
}

// PopLeft executes the blocking BLPOP command
func (c *Conn) PopLeft(timeout time.Duration, key string, keys ...string) (k, v string, err error) {
	k, v, _, err = c.bpop("BLPOP", timeout, key, keys)
	return
}

// PopRight executes the blocking BRPOP command
func (c *Conn) PopRight(timeout time.Duration, key string, keys ...string) (k, v string, err error) {
	k, v, _, err = c.bpop("BRPOP", timeout, key, keys)
	return
}

// PopMin executes the blocking BZPOPMIN command
func (c *Conn) PopMin(timeout time.Duration, key string, keys ...string) (k, v string, score float64, err error) {
	return c.bpop("BZPOPMIN", timeout, key, keys)
}

// PopMax executes the blocking BZPOPMAX command
func (c *Conn) PopMax(timeout time.Duration, key string, keys ...string) (k, v string, score float64, err error) {
	return c.bpop("BZPOPMIN", timeout, key, keys)
}

func (c *Conn) closeWithError(err error) error {
	if c.err == nil {
		c.err = err
		c.conn.Close()
		c.conn = nil
	}
	return c.err
}

// Close closes a connection
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

// Auth authenticates a connection to the server
func (c *Conn) Auth(password string) error {
	p := BlankPipeline(-1)
	defer ReleasePipeline(p)
	p.Auth(password)
	r := BlankReply()
	defer ReleaseReply(r)
	if err := c.Do(p, r); err != nil {
		return err
	}
	v := r.Value().Get(0)
	if err := v.Err(); err != nil {
		return err
	}
	return nil
}

// Select will select db on next Do
func (c *Conn) Select(db int64) {
	c.db = db
}

// Quit closes the connection issuing a QUIT command
func (c *Conn) Quit() error {
	defer c.Close()
	p := BlankPipeline(-1)
	defer ReleasePipeline(p)
	p.Quit()
	return c.Do(p, nil)
}

// LoadScript loads a Lua script
func (c *Conn) LoadScript(src string) (sha1 string, err error) {
	p := BlankPipeline(-1)
	defer ReleasePipeline(p)
	r := BlankReply()
	defer ReleaseReply(r)
	p.ScriptLoad(src)
	err = c.Do(p, r)
	if err != nil {
		return
	}
	v := r.Value().Get(0)
	err = v.Err()
	if err != nil {
		return
	}
	sha1 = string(v.Bytes())
	return
}
