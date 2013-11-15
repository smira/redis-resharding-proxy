package main

import (
	"bufio"
	"bytes"
	"testing"
)

func TestFilterRDB(t *testing.T) {
	RDBFile := "REDIS0006\xfe\x00\x00\x03b_1\x04kuku\x00\x03a_1\x04lala\x00\x03b_3\xc3\t@\xb3\x01aa\xe0\xa6\x00\x01aa\xfc\xdb\x82\xb0\\B\x01\x00\x00\x00\x03b_2\r2343545345345\x00\x03a_2\xc0!\xffT\x81\xe9\x86\xcc\x9f\x1f\xc4"

	ch := make(chan []byte)

	go func() {
		err := FilterRDB(bufio.NewReader(bytes.NewBufferString(RDBFile)), ch, func(string) bool { return true })
		if err != nil {
			t.Error("Filtering failed: ", err)
		}
	}()

	received := ""

	for {
		data, ok := <-ch
		if !ok {
			break
		}
		received += string(data)
	}

	if RDBFile != received {
		t.Errorf("output not equal to expected: %#v != %#v", RDBFile, received)
	}
}
