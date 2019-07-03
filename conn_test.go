package redis

import (
	"testing"

	"github.com/alxarch/fastredis/resp"
)

func TestConn(t *testing.T) {
	conn, err := Dial(nil)
	if err != nil {
		t.Fatalf(`Dial nil failed: %s`, err)
	}
	p := BlankPipeline()
	defer p.Close()
	r := BlankReply()
	defer ReleaseReply(r)
	p.Select(10)
	p.Set("foo", resp.String("bar"), 0)
	p.Keys("*")
	p.FlushDB()
	if err := conn.Do(p, r); err != nil {
		t.Fatalf(`Do failed: %s`, err)
	}
	v := r.Value()
	if v.Len() != 4 {
		t.Errorf("Invalid reply length: %d", v.Len())
	}
	if v.Type() != resp.Array {
		t.Errorf("Invalid reply type: %d", v.Type())
	}
	if ok := v.Get(0); string(ok.Bytes()) != "OK" {
		t.Errorf("Invalid select reply: %s", ok.Bytes())
	}

	if ok := v.Get(1); string(ok.Bytes()) != "OK" {
		t.Errorf("Invalid set reply: %s", ok.Bytes())
	}
	if keys := v.Get(2); keys.Len() != 1 {
		t.Errorf("Invalid keys reply size: %d", keys.Len())
	}
	if ok := v.Get(3); string(ok.Bytes()) != "OK" {
		t.Errorf("Invalid flushdb reply: %s", ok.Bytes())
	}
}

func BenchmarkPipeline(b *testing.B) {
	b.ReportAllocs()
	p := BlankPipeline()
	defer p.Close()
	for i := 0; i < b.N; i++ {
		p.Reset()
		p.HIncrBy("foo", "bar", 1)
	}
}
