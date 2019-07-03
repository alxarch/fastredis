package redis

import (
	"testing"
	"time"

	"github.com/alxarch/fastredis/resp"
)

func Test_Pool(t *testing.T) {
	pool := NewPool(&PoolOptions{})
	conn, err := pool.Get(time.Time{})
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	defer pool.Put(conn)
	p := BlankPipeline()
	defer p.Close()
	p.HSet("foo", "bar", resp.String("baz"))
	conn.Do(p, nil)

}
func Test_ParseURL(t *testing.T) {
	opts, err := ParseURL("")
	if err != nil {
		t.Fatalf("Unexpected error %s", err)
	}
	_ = opts

}