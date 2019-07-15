package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/alxarch/fastredis/resp"
)

func Test_Scan(t *testing.T) {
	now := time.Now()
	p := new(Pipeline)
	key := fmt.Sprintf("scantest:%d", now.UnixNano())
	p.HSet(key, "foo", resp.String("bar"))
	p.HSet(key, "bar", resp.String("baz"))
	conn, err := Dial(":6379", ConnOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Do(p, nil); err != nil {
		t.Fatal(err)
	}
	iter := HScan(key, "", 0)
	values := make(map[string]string)
	scan := func(k []byte, v resp.Value) error {
		values[string(k)] = string(v.Bytes())
		return nil
	}
	if err := iter.Each(conn, scan); err != nil {
		t.Fatalf("Scan error %s", err)
	}
	if len(values) != 2 {
		t.Errorf("Invalid scan %v", values)
	}
	if values["foo"] != "bar" {
		t.Errorf("Invalid scan %v", values)
	}
	if values["bar"] != "baz" {
		t.Errorf("Invalid scan %v", values)
	}

}
