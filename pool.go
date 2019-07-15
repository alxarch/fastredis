package redis

import (
	"bufio"
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alxarch/fastredis/resp"
)

// Pool is a pool of redis connections
type Pool struct {
	noCopy

	ReadBufferSize    int
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	Address           string
	MaxConnections    int
	MaxIdleTime       time.Duration
	MaxConnectionAge  time.Duration
	CheckIdleInterval time.Duration
	Dial              func(address string) (net.Conn, error)

	numOpen int32
	numIdle int32

	mu        sync.Mutex
	cond      sync.Cond
	active    int
	closed    bool
	idle      []*Conn
	closeChan chan struct{}
	ts        int64

	hits, misses, timeouts int64
}

// Do executes a RESP pipeline
func (pool *Pool) Do(p *Pipeline, r *resp.Reply) error {
	conn, err := pool.Get(time.Time{})
	if err != nil {
		return err
	}
	err = conn.Do(p, r)
	pool.Put(conn)
	return err
}

// Close closes a pool
func (pool *Pool) Close() error {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return errPoolClosed
	}
	pool.closed = true
	atomic.StoreInt32(&pool.numIdle, math.MinInt32)
	atomic.StoreInt32(&pool.numOpen, math.MinInt32)
	for i, c := range pool.idle {
		pool.idle[i] = nil
		c.closeWithError(errPoolClosed)
	}
	pool.idle = pool.idle[:0]
	if ch := pool.closeChan; ch != nil {
		pool.closeChan = nil
		close(ch)
	}
	pool.cond.Broadcast()
	pool.mu.Unlock()
	return nil
}

// Get gets  a connection from the pool
func (pool *Pool) Get(deadline time.Time) (*Conn, error) {
	for {
		if n := atomic.LoadInt32(&pool.numIdle); n > 0 {
			if atomic.CompareAndSwapInt32(&pool.numIdle, n, n-1) {
				return pool.get(deadline.UnixNano())
			}
		} else if n < 0 {
			return nil, errPoolClosed
		} else {
			break
		}
	}
	max := pool.maxConnections()
	for {
		if n := atomic.LoadInt32(&pool.numOpen); 0 <= n && n < max {
			if atomic.CompareAndSwapInt32(&pool.numOpen, n, n+1) {
				go pool.dial(pool.Address)
				break
			}
		} else if n < 0 {
			return nil, errPoolClosed
		} else {
			break
		}
	}
	return pool.get(deadline.UnixNano())
}

// Put releases a connection to the pool
func (pool *Pool) Put(c *Conn) {
	if c == nil {
		return
	}

	if c.err != nil {
		pool.closeConn(c)
		return
	}
	now := time.Now()
	if pool.MaxConnectionAge > 0 && now.Sub(c.createdAt) > pool.MaxConnectionAge {
		pool.closeConn(c)
		return
	}
	c.lastUsedAt = now
	pool.put(c)
}

func defaultDial(addr string) (net.Conn, error) {
	return net.Dial("tcp", addr)
}

type noCopy struct{}

func (noCopy) Lock()   {}
func (noCopy) Unlock() {}

func (pool *Pool) clean(scratch *[]*Conn, now time.Time) (int, error) {
	maxIdleTime := pool.MaxIdleTime
	if maxIdleTime <= 0 {
		maxIdleTime = time.Hour
	}
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return 0, errPoolClosed
	}
	i := 0
	idle := pool.idle
	for 0 <= i && i < len(idle) && now.Sub(idle[i].lastUsedAt) > maxIdleTime {
		i++
	}
	if i == 0 {
		pool.mu.Unlock()
		return 0, nil
	}
	*scratch = append((*scratch)[:0], idle[:i]...)
	// Decrement active count while under lock
	pool.active -= i
	j := copy(pool.idle, idle[i:])
	if len(pool.idle) > j {
		for i := range pool.idle[j:] {
			pool.idle[i] = nil
		}
		pool.idle = pool.idle[:j]
	}
	if len(pool.idle) == 0 {
		pool.cond.L = nil
	}

	pool.mu.Unlock()
	atomic.StoreInt32(&pool.numIdle, int32(j))
	tmp := *scratch
	for i, c := range tmp {
		c.Close()
		tmp[i] = nil
	}
	n := len(tmp)
	*scratch = tmp[:0]
	return n, nil
}

// const poolClockInterval = 100 * time.Millisecond

func (pool *Pool) newConn(conn net.Conn) (c *Conn) {
	now := time.Now()
	x := connPool.Get()
	if x == nil {
		c = new(Conn)
		size := pool.ReadBufferSize
		if size < minBufferSize {
			size = minBufferSize
		}
		c.r = bufio.NewReaderSize(c, size)
	} else {
		c = x.(*Conn)
		c.r.Reset(c)
	}
	c.options = &ConnOptions{
		ReadBufferSize: pool.ReadBufferSize,
		ReadTimeout:    pool.ReadTimeout,
		WriteTimeout:   pool.WriteTimeout,
	}
	c.conn = conn
	c.createdAt = now
	c.lastUsedAt = now
	return
}

func (pool *Pool) dial(addr string) {
	dialer := pool.Dial
	if dialer == nil {
		dialer = defaultDial
	}
	conn, err := dialer(addr)
	if err != nil {
		atomic.AddInt32(&pool.numOpen, -1)
		return
	}
	pool.put(pool.newConn(conn))
}

func (pool *Pool) runCleaner() {
	interval := pool.CheckIdleInterval
	if interval < time.Second {
		interval = time.Second
	}
	var scratch []*Conn
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-pool.closeChan:
			tick.Stop()
			return
		case t := <-tick.C:
			pool.clean(&scratch, t)
		}
	}
}

// pool of *Conn objects
var connPool sync.Pool

func (pool *Pool) closeConn(c *Conn) {
	conn := c.conn
	c.conn = nil
	conn.Close()
	c.r.Reset(nil)
	c.err = nil
	connPool.Put(c)
	for {
		n := atomic.LoadInt32(&pool.numOpen)
		if n <= 0 || atomic.CompareAndSwapInt32(&pool.numOpen, n, n-1) {
			return
		}
	}
}

func (pool *Pool) put(c *Conn) {
	run := false
	ts := c.lastUsedAt.UnixNano()
	isNew := c.lastUsedAt.Equal(c.createdAt)
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		pool.closeConn(c)
		return
	}
	pool.ts = ts
	if isNew {
		pool.active++
	}

	pool.idle = append(pool.idle, c)
	if pool.closeChan == nil {
		pool.closeChan = make(chan struct{})
		run = true
	}
	if pool.cond.L == nil {
		pool.cond.L = &pool.mu
	}
	pool.cond.Signal()
	pool.mu.Unlock()
	atomic.AddInt32(&pool.numIdle, 1)
	if run {
		go pool.runCleaner()
	}
}

func (pool *Pool) get(deadline int64) (conn *Conn, err error) {
	miss := false
	pool.mu.Lock()
	for len(pool.idle) == 0 {
		miss = true
		if pool.closed {
			pool.mu.Unlock()
			err = errPoolClosed
			return
		}
		if 0 < deadline && deadline < pool.ts {
			pool.mu.Unlock()
			atomic.AddInt64(&pool.timeouts, 1)
			err = errDeadlineExceeded
			return
		}
		if pool.cond.L == nil {
			pool.cond.L = &pool.mu
		}
		pool.cond.Wait()
	}

	if n := len(pool.idle) - 1; 0 <= n && n < len(pool.idle) {
		conn = pool.idle[n]
		pool.idle[n] = nil
		pool.idle = pool.idle[:n]
	}
	pool.mu.Unlock()
	if miss {
		atomic.AddInt64(&pool.misses, 1)
	} else {
		atomic.AddInt64(&pool.hits, 1)
	}
	return
}

var (
	errConnWriteOnly    = Err("Write only connection")
	errConnClosed       = Err("Connection closed")
	errPoolClosed       = Err("Pool closed")
	errDeadlineExceeded = Err("Deadline exceeded")
)

func (pool *Pool) maxConnections() int32 {
	max := int32(pool.MaxConnections)
	if max <= 0 {
		max = math.MaxInt32
	}
	return max
}

// ParseURL parses a URL to PoolOptions
func (pool *Pool) ParseURL(rawurl string) (err error) {
	if rawurl == "" {
		return
	}
	u, err := url.Parse(rawurl)
	if err != nil {
		return
	}
	if u.Scheme != "redis" {
		err = fmt.Errorf(`Invalid URL scheme %q`, u.Scheme)
		return
	}
	switch n := strings.Trim(u.Path, "/"); n {
	case "", "0":
	default:
		err = fmt.Errorf(`Invalid URL path %q`, u.Path)
		return
	}
	q := u.Query()
	host, port := u.Hostname(), u.Port()
	if port == "" {
		port = "6379"
	}
	pool.Address = host + ":" + port

	if v, ok := q["read-timeout"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			pool.ReadTimeout = d
		}
	}
	if v, ok := q["write-timeout"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			pool.WriteTimeout = d
		}
	}
	if v, ok := q["read-buffer-size"]; ok && len(v) > 0 {
		if size, _ := strconv.Atoi(v[0]); size > 0 {
			pool.ReadBufferSize = size
		}
	}

	if v, ok := q["max-conn-age"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			pool.MaxConnectionAge = d
		}
	}
	if v, ok := q["max-idle-time"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			pool.MaxIdleTime = d
		}
	}
	if v, ok := q["check-idle-interval"]; ok && len(v) > 0 {
		if d, _ := time.ParseDuration(v[0]); d > 0 {
			pool.CheckIdleInterval = d
		}
	}

	return
}

func (pool *Pool) Idle() int {
	return int(atomic.LoadInt32(&pool.numIdle))
}
func (pool *Pool) Open() int {
	return int(atomic.LoadInt32(&pool.numOpen))
}

type PoolStats struct {
	Hits, Misses, Timeouts int64
}

func (pool *Pool) Stats() PoolStats {
	return PoolStats{
		Hits:     atomic.LoadInt64(&pool.hits),
		Misses:   atomic.LoadInt64(&pool.misses),
		Timeouts: atomic.LoadInt64(&pool.timeouts),
	}
}
