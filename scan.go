package redis

import "github.com/alxarch/fastredis/resp"

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

func Scan(match string, count int64) *ScanIterator {
	s := ScanIterator{
		cmd:   "SCAN",
		match: match,
		count: count,
	}
	return &s
}
func HScan(key, match string, count int64) *ScanIterator {
	s := ScanIterator{
		cmd:   "HSCAN",
		key:   key,
		match: match,
		count: count,
	}
	return &s
}
func ZScan(key, match string, count int64) *ScanIterator {
	s := ScanIterator{
		cmd:   "ZSCAN",
		key:   key,
		match: match,
		count: count,
	}
	return &s
}
func SScan(key, match string, count int64) *ScanIterator {
	s := ScanIterator{
		cmd:   "SSCAN",
		key:   key,
		match: match,
		count: count,
	}
	return &s
}

func (s *ScanIterator) Each(conn *Conn, scan func(v resp.Value, k []byte) error) error {
	switch s.cmd {
	case "HSCAN", "ZSCAN", "SSCAN":
		var k []byte
		for i, v := 0, s.Next(conn); !v.IsNull(); v, i = s.Next(conn), i+1 {
			if i%2 == 0 {
				k = append(k[:0], v.Bytes()...)
			} else if err := scan(v, k); err != nil {
				return err
			}
		}
	default:
		for v := s.Next(conn); !v.IsNull(); v = s.Next(conn) {
			if err := scan(v, nil); err != nil {
				return err
			}
		}
	}
	return s.Err()
}

func (s *ScanIterator) Err() error {
	return s.err
}

const ErrIteraratorClosed = Err("Iterator closed")

func (s *ScanIterator) Close() error {
	if s.err == nil {
		var v resp.Value
		v, s.val, s.err = s.val, resp.NullValue(), ErrIteraratorClosed
		ReleaseReply(v.Reply())
		return nil
	}
	return s.err
}

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

		p := BlankPipeline()
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
		p.Close()
		if s.err != nil {
			s.val = resp.NullValue()
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
	return resp.NullValue()
end:
	s.val = resp.NullValue()
	ReleaseReply(reply)
	return s.val
}
