package avro_test

import (
	"testing"

	"github.com/wanabe/avroturf-go/avro"
)

func TestDecodeInt(t *testing.T) {
	decoder := avro.Decoder{Buffer: []byte{}}
	_, err := decoder.DecodeInt()
	if err == nil || err.Error() != "can't read int" {
		t.Errorf("unexpected error: %v", err)
	}

	data := map[int][]byte{
		0:           []byte{0},
		-1:          []byte{1},
		1:           []byte{2},
		-2:          []byte{3},
		2:           []byte{4},
		63:          []byte{0x7e},
		-64:         []byte{0x7f},
		64:          []byte{0x80, 0x01},
		-65:         []byte{0x81, 0x01},
		65:          []byte{0x82, 0x01},
		127:         []byte{0xfe, 0x01},
		-128:        []byte{0xff, 0x01},
		128:         []byte{0x80, 0x02},
		192:         []byte{0x80, 0x03},
		8191:        []byte{0xfe, 0x7f},
		-8192:       []byte{0xff, 0x7f},
		8192:        []byte{0x80, 0x80, 0x01},
		2147483647:  []byte{0xfe, 0xff, 0xff, 0xff, 0x0f},
		-2147483648: []byte{0xff, 0xff, 0xff, 0xff, 0x0f},
		/* 64bit only
		9223372036854775807:  []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01},
		-9223372036854775808: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01},
		*/
	}
	for n, buf := range data {
		decoder.Buffer = buf
		decoder.Offset = 0
		i, err := decoder.DecodeInt()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if i != n {
			t.Errorf("expected %d but got %d", n, i)
		}
		if decoder.Offset != len(buf) {
			t.Errorf("expected %d but got %d", len(buf), decoder.Offset)
		}
	}
}

func TestDecodeString(t *testing.T) {
	decoder := avro.Decoder{Buffer: []byte{}}
	_, err := decoder.DecodeString()
	if err == nil || err.Error() != "can't read int" {
		t.Errorf("unexpected error: %v", err)
	}
	decoder.Buffer = []byte{2}
	decoder.Offset = 0
	_, err = decoder.DecodeString()
	if err == nil || err.Error() != "unexpected buffer length: 1 < 2" {
		t.Errorf("unexpected error: %v", err)
	}

	data := map[string][]byte{
		" ":  []byte{2, 0x20},
		"  ": []byte{4, 0x20, 0x20},
	}
	for n, buf := range data {
		decoder.Buffer = buf
		decoder.Offset = 0
		s, err := decoder.DecodeString()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if s != n {
			t.Errorf("expected %s but got %s", n, s)
		}
		if decoder.Offset != len(buf) {
			t.Errorf("expected %d but got %d", len(buf), decoder.Offset)
		}
	}

	longString := "abcd"
	for i := 0; i < 4; i++ {
		longString = longString + longString
	}
	decoder.Buffer = []byte{0x80, 0x01}
	decoder.Buffer = append(decoder.Buffer, longString...)
	decoder.Offset = 0
	s, err := decoder.DecodeString()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if s != longString {
		t.Errorf("expected %s but got %s", longString, s)
	}
	if decoder.Offset != len(decoder.Buffer) {
		t.Errorf("expected %d but got %d", len(decoder.Buffer), decoder.Offset)
	}
}
