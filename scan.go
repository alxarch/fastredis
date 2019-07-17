package redis

import "github.com/alxarch/fastredis/resp"

// ScanIterator is an iterator for Redis scan commands
type ScanIterator struct {
	cmd   string
	match string
	key   string
	cur   int64
	val   resp.Value
	n     int
	i     int
	err   error
	count int64
}

// Scan starts a key scan iterator
func Scan(match string, count int64) *ScanIterator {
	s := ScanIterator{
		cmd:   "SCAN",
		match: match,
		count: count,
	}
	return &s
}

// HScan starts a hash object scan iterator
func HScan(key, match string, count int64) *ScanIterator {
	s := ScanIterator{
		cmd:   "HSCAN",
		key:   key,
		match: match,
		count: count,
	}
	return &s
}

// ZScan starts a sorted set scan iterator
func ZScan(key, match string, count int64) *ScanIterator {
	s := ScanIterator{
		cmd:   "ZSCAN",
		key:   key,
		match: match,
		count: count,
	}
	return &s
}

// SScan starts a set scan iterator
func SScan(key, match string, count int64) *ScanIterator {
	s := ScanIterator{
		cmd:   "SSCAN",
		key:   key,
		match: match,
		count: count,
	}
	return &s
}

// Each executes a callback for each result in the iterator
func (s *ScanIterator) Each(conn *Conn, scan func(k []byte, v resp.Value) error) error {
	switch s.cmd {
	case "HSCAN", "ZSCAN", "SSCAN":
		var k []byte
		for i, v := 0, s.Next(conn); !v.IsNull(); v, i = s.Next(conn), i+1 {
			if i%2 == 0 {
				k = append(k[:0], v.Bytes()...)
			} else if err := scan(k, v); err != nil {
				return err
			}
		}
	default:
		null := resp.Null()
		for v := s.Next(conn); !v.IsNull(); v = s.Next(conn) {
			if err := scan(v.Bytes(), null); err != nil {
				return err
			}
		}
	}
	return s.Err()
}

// Err returns the scan error if any
func (s *ScanIterator) Err() error {
	return s.err
}

// ErrIteratorClosed occurs when an iterator is used after Close()
const ErrIteratorClosed = Err("Iterator closed")

// Close closes an iterator
func (s *ScanIterator) Close() error {
	if s.err == nil {
		var v resp.Value
		v, s.val, s.err = s.val, resp.Null(), ErrIteratorClosed
		ReleaseReply(v.Reply())
		return nil
	}
	return s.err
}

// Next gets the next value in an iterator
func (s *ScanIterator) Next(conn *Conn) resp.Value {
	var reply *resp.Reply
	for s.err == nil {
		if s.i < s.n {
			v := s.val.Get(s.i)
			s.i++
			return v
		}
		if s.val.IsNull() {
			// Iterator closed
			return s.val
		}
		reply = s.val.Reply()
		if reply == nil {
			reply = BlankReply()
		} else if s.cur == 0 {
			// Full cycle
			goto end
		} else {
			reply.Reset()
		}

		p := BlankPipeline(-1)
		switch s.cmd {
		case "HSCAN":
			p.HScan(s.key, s.cur, s.match, s.count)
		case "ZSCAN":
			p.ZScan(s.key, s.cur, s.match, s.count)
		case "SSCAN":
			p.SScan(s.key, s.cur, s.match, s.count)
		default:
			p.Scan(s.cur, s.match, s.count)
		}
		s.err = conn.Do(p, reply)
		ReleasePipeline(p)
		if s.err != nil {
			s.val = resp.Null()
			ReleaseReply(reply)
			return s.val
		}
		v := reply.Value().Get(0)
		s.err = v.Err()
		if s.err != nil {
			goto end
		}
		cur, ok := v.Get(0).Int()
		if !ok {
			s.err = Err(`Protocol error`)
			goto end
		}
		s.cur = cur
		s.val = v.Get(1)
		s.i, s.n = 0, s.val.Len()
		if s.n == 0 {
			s.val = v
		}
	}
	return resp.Null()
end:
	s.val = resp.Null()
	ReleaseReply(reply)
	return s.val
}
