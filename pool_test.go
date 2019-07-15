package redis

import (
	"testing"
	"time"

	"github.com/alxarch/fastredis/resp"
)

func Test_Pool(t *testing.T) {
	pool := new(Pool)
	if err := pool.ParseURL("redis://:6379"); err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	conn, err := pool.Get(time.Time{})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	defer pool.Put(conn)
	p := BlankPipeline()
	defer ReleasePipeline(p)
	p.HSet("foo", "bar", resp.String("baz"))
	conn.Do(p, nil)

}
