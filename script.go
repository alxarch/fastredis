package redis

type Script struct {
	sha1 [40]byte
}

func (s Script) String() string {
	return string(s.sha1[:])
}

func (c *Conn) LoadScript(src string) (*Script, error) {
	p := BlankPipeline()
	defer p.Close()
	r := BlankReply()
	defer r.Close()
	p.appendArr(3)
	p.appendArg(String("SCRIPT"))
	p.appendArg(String("LOAD"))
	p.appendArg(String(src))
	p.n++
	if err := c.Do(p, r); err != nil {
		return nil, err
	}
	v := r.Value().Get(0)
	if err := v.Err(); err != nil {
		return nil, err
	}
	s := Script{}
	copy(s.sha1[:], v.Bytes())
	return &s, nil
}
