package redis

import "github.com/alxarch/fastredis/resp"

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
	defer ReleaseReply(r)
	p.Command("SCRIPT", 2)
	p.WriteArg(resp.String("LOAD"))
	p.WriteArg(resp.String(src))
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
