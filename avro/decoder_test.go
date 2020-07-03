package avro_test

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/wanabe/avroturf-go/avro"
)

type decodeTestStruct struct {
	Str  string        `avro:"str"`
	Num1 int32         `avro:"num1"`
	Num2 int64         `avro:"num2"`
	Num3 sql.NullInt32 `avro:"num3"`
	Num4 sql.NullInt64 `avro:"num4"`
}

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

func TestUnmarshal(t *testing.T) {
	schema := &avro.Schema{
		Type: avro.Type{Primitive: avro.Record},
		Fields: []avro.Schema{
			{
				Type: avro.Type{Primitive: avro.String},
				Name: "str",
			},
		},
	}
	obj := decodeTestStruct{}
	buf := []byte{6, 'a', 'b', 'c', 'd'}
	err := avro.Unmarshal(buf, &obj, schema)
	if err != nil {
		t.Error(err)
	}
	if obj.Str != "abc" {
		t.Errorf(`expected "abc" but got "%s"`, obj.Str)
	}

	schema = &avro.Schema{
		Type: avro.Type{Primitive: avro.Record},
		Fields: []avro.Schema{
			{
				Type: avro.Type{Primitive: avro.Int},
				Name: "num1",
			},
			{
				Type: avro.Type{Primitive: avro.Long},
				Name: "num2",
			},
			{
				Type: avro.Type{
					Primitive: avro.Union,
					UnionedTypes: []avro.Type{
						{Primitive: avro.Null},
						{Primitive: avro.Int},
					},
				},
				Name: "num3",
			},
			{
				Type: avro.Type{
					Primitive: avro.Union,
					UnionedTypes: []avro.Type{
						{Primitive: avro.Null},
						{Primitive: avro.Long},
					},
				},
				Name: "num4",
			},
		},
	}

	buf = []byte{
		2, // num1
		8, // num2
		0, // schema of num3,
		2, // schema of num4,
		4, // num4
	}
	obj = decodeTestStruct{}
	err = avro.Unmarshal(buf, &obj, schema)
	if err != nil {
		t.Error(err)
	}
	expected := decodeTestStruct{
		Num1: 1,
		Num2: 4,
		Num3: sql.NullInt32{},
		Num4: sql.NullInt64{Int64: 2, Valid: true},
	}
	if !reflect.DeepEqual(expected, obj) {
		t.Errorf(`expected %+v but got %+v`, expected, obj)
	}

	buf = []byte{
		0x81, 0x80, 0x1, // num1
		11, // num2
		2,  // schema of num3,
		5,  // num3
		0,  // schema of num4,
	}
	obj = decodeTestStruct{}
	err = avro.Unmarshal(buf, &obj, schema)
	if err != nil {
		t.Error(err)
	}
	expected = decodeTestStruct{
		Num1: -8193,
		Num2: -6,
		Num3: sql.NullInt32{Int32: -3, Valid: true},
		Num4: sql.NullInt64{},
	}
	if !reflect.DeepEqual(expected, obj) {
		t.Errorf(`expected %+v but got %+v`, expected, obj)
	}
}
