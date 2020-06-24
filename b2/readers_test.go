package b2

import (
	"bytes"
	"crypto/sha1"
	"io/ioutil"
	"testing"
)

func TestPostfixingSha1_Content(t *testing.T) {
	buf := Closer(bytes.NewBufferString("hello world"))
	r := &HashedPostfixedReader{R: buf, H: sha1.New()}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	expected := "hello world2aae6c35c94fcfb415dbe95f408b9ce91ee846ed"
	if string(b) != expected {
		t.Fatalf("Expected %#v != %#v", string(b), expected)
	}
}

func TestPostfixingSha1_Empty(t *testing.T) {
	buf := Closer(bytes.NewBuffer(nil))
	r := &HashedPostfixedReader{R: buf, H: sha1.New()}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	expected := "da39a3ee5e6b4b0d3255bfef95601890afd80709"
	if string(b) != expected {
		t.Fatalf("Expected %#v != %#v", string(b), expected)
	}
}
