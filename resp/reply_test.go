package resp

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestReplyReadFrom(t *testing.T) {
	rep := new(Reply)
	b := bytes.NewBuffer(nil)
	b.WriteString("+OK\r\n")
	r := bufio.NewReader(b)
	v, err := rep.ReadFrom(r)
	if err != nil {
		t.Errorf("Read failed %s", err)
	}
	if v.Type() != SimpleString {
		t.Errorf("Invalid type %v", v)
	}
}
func TestReplyReadFromN(t *testing.T) {
	rep := new(Reply)
	b := new(Buffer)
	b.BulkString("foo")
	b.BulkString("bar")
	b.BulkString("baz")
	r := bufio.NewReader(bytes.NewReader(b.B))
	v, err := rep.ReadFromN(r, 3)
	if err != nil {
		t.Errorf("Read failed %s", err)
	}
	if v.Type() != Array {
		t.Errorf("Invalid type %v", v)
	}

	if v.Len() != 3 {
		t.Fatalf("Invalid size %v", v)
	}
	if vv := v.Get(0); vv.Type() != BulkString || string(vv.Bytes()) != "foo" {
		t.Fatalf("Invalid foo %v", vv)
	}
	if vv := v.Get(1); vv.Type() != BulkString || string(vv.Bytes()) != "bar" {
		t.Fatalf("Invalid bar %v", vv)
	}
	if vv := v.Get(2); vv.Type() != BulkString || string(vv.Bytes()) != "baz" {
		t.Fatalf("Invalid baz %v", vv)
	}
}

func TestParseValue(t *testing.T) {
	b := new(Buffer)
	b.BulkStringArray("foo", "bar", "answer", "42")
	v, err := ParseValue(b.B)
	if err != nil {
		t.Errorf("Parse failed %s", err)
	}
	if v.Type() != Array {
		t.Errorf("Invalid type %v", v)
	}

	if v.Len() != 4 {
		t.Fatalf("Invalid size %v", v)
	}
	if vv := v.Get(0); vv.Type() != BulkString || string(vv.Bytes()) != "foo" {
		t.Fatalf("Invalid foo %v", vv)
	}
	if vv := v.Get(1); vv.Type() != BulkString || string(vv.Bytes()) != "bar" {
		t.Fatalf("Invalid bar %v", vv)
	}
	if vv := v.Get(2); vv.Type() != BulkString || string(vv.Bytes()) != "answer" {
		t.Fatalf("Invalid answer %v", vv)
	}
	if vv := v.Get(3); vv.Type() != BulkString || string(vv.Bytes()) != "42" {
		t.Fatalf("Invalid 42 %v", vv)
	}
	{
		values := []string{}
		v.ForEach(func(v Value) {
			if v.Type() != BulkString {
				t.Errorf("Invalid type")
			}
			values = append(values, string(v.Bytes()))
		})
		if !reflect.DeepEqual(values, []string{"foo", "bar", "answer", "42"}) {
			t.Errorf("Invalid each values %v", values)
		}

	}
	{
		values := map[string]string{}
		v.ForEachKV(func(k []byte, v Value) {
			if v.Type() != BulkString {
				t.Errorf("Invalid type")
			}
			values[string(k)] = string(v.Bytes())
		})
		if len(values) != 2 {
			t.Errorf("Invalid each values %v", values)
		}
		if values["foo"] != "bar" {
			t.Errorf("Invalid each values %v", values)

		}
		if values["answer"] != "42" {
			t.Errorf("Invalid each values %v", values)
		}
	}
}
