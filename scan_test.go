package redis

import "testing"

func Test_Scan(t *testing.T) {
	it := Scan("foo*", 5)

	conn, err := Dial(nil)
	if err != nil {
		t.Fatal(err)
	}
	v := it.Next(conn)
	if v.IsNull() {
		t.Errorf("Null reply")
	}

}
