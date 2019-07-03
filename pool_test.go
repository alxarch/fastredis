package redis

import (
	"reflect"
	"testing"
	"time"
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
	p.HSet("foo", "bar", String("baz"))
	conn.Do(p, nil)

}
func Test_ParseURL(t *testing.T) {
	opts, err := ParseURL("")
	if err != nil {
		t.Fatalf("Unexpected error %s", err)
	}
	if def := DefaultPoolOptions(); !reflect.DeepEqual(&opts, &def) {
		t.Errorf("Invalid blank options %v != %v", opts, def)

	}

}
