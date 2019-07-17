package redis

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/alxarch/fastredis/resp"
)

func Test_Pool(t *testing.T) {
	pool := new(Pool)
	defer pool.Close()
	// if err := pool.ParseURL("redis://:6379"); err != nil {
	if err := pool.ParseURL("redis://"); err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	pool.WaitTimeout = time.Second
	pool.MaxConnections = 5
	pool.MaxIdleTime = time.Second
	now := time.Now()
	key := fmt.Sprintf("mdbredis:%d", now.UnixNano())
	wg := new(sync.WaitGroup)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := pool.Pipeline()
			defer ReleasePipeline(p)
			field := fmt.Sprintf("foo-%d", i)
			p.HSet(key, field, resp.String("baz"))
			err := pool.Do(p, nil)
			if err != nil {
				t.Errorf("Unexpected error: %s", err)
				return
			}
		}(i)
	}
	wg.Wait()
	p := pool.Pipeline()
	defer ReleasePipeline(p)
	p.HGetAll(key)
	r := BlankReply()
	if err := pool.Do(p, r); err != nil {
		t.Errorf("Unexpected error: %s", err)
		return
	}
	v := r.Value().Get(0)
	var fields []string
	v.ForEachKV(func(k []byte, v resp.Value) {
		fields = append(fields, string(k))
		if string(v.Bytes()) != "baz" {
			t.Errorf("Unexpected value: %s %s", k, v.Bytes())
		}
	})
	sort.Strings(fields)
	if !reflect.DeepEqual(fields, []string{
		"foo-0",
		"foo-1",
		"foo-2",
		"foo-3",
		"foo-4",
		"foo-5",
		"foo-6",
		"foo-7",
		"foo-8",
		"foo-9",
	}) {

		t.Errorf("Unexpected fields: %s", fields)
	}

}
