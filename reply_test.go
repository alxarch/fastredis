package redis

import (
	"testing"
)

func TestScanIterator(t *testing.T) {
	conn, err := Dial(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	s := Scan("", 0)
	v := s.Next(conn)
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	if v.IsNull() {
		t.Errorf("Nil reply")
	}
}
