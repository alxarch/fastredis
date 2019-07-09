package redis

import (
	"testing"

	"github.com/alxarch/fastredis/resp"
)

func TestScript(t *testing.T) {
	conn, err := Dial(":6379", ConnOptions{})
	if err != nil {
		t.Fatal(err)
	}
	src := "return {KEYS[1],ARGV[1],KEYS[2],ARGV[2]}"
	s, err := conn.LoadScript(src)
	if err != nil {
		t.Fatal(err)
	}
	if s.String() != `da95252e2c27e41cd53b9114f28b4ba84e7d64d4` {
		t.Errorf("Invalid SHA1: %s", s)
	}
	p := BlankPipeline()
	defer p.Close()
	r := BlankReply()
	defer ReleaseReply(r)
	p.EvalSHA(s, resp.Key("foo"), resp.Key("bar"), resp.String("bar"), resp.String("baz"))

	if err := conn.Do(p, r); err != nil {
		t.Fatal(err)
	}
	v := r.Value().Get(0)
	if err := v.Err(); err != nil {
		t.Fatal(err)
	}
	if n := v.Len(); n != 4 {
		t.Errorf("Invalid value length: %d", n)
	}
	v.ForEachKV(func(k []byte, v resp.Value) {
		switch string(k) {
		case "foo":
			if string(v.Bytes()) == "bar" {
				return
			}
		case "bar":
			if string(v.Bytes()) == "baz" {
				return
			}
		}
		t.Errorf("Invalid value: %s %s", k, v.Bytes())
		t.Errorf("%s, %s", k, v.Bytes())
	})

}
