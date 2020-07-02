package avro_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/wanabe/avroturf-go/avro"
)

func TestPrimitiveUnmarshalJSON(t *testing.T) {
	typ := avro.Type{}

	data := map[interface{}][]byte{
		avro.Null:    []byte(`"null"`),
		avro.Record:  []byte(`"record"`),
		avro.Boolean: []byte(`"boolean"`),
		avro.Int:     []byte(`"int"`),
		avro.Long:    []byte(`"long"`),
		avro.Float:   []byte(`"float"`),
		avro.Double:  []byte(`"double"`),
		avro.Bytes:   []byte(`"bytes"`),
		avro.String:  []byte(`"string"`),
	}
	for prim, b := range data {
		if err := json.Unmarshal(b, &typ); err != nil {
			t.Error(err)
		}
		if typ.Primitive != prim {
			t.Errorf("expected %s but %+v", prim, typ.Primitive)
		}
	}
}

func TestLogicalTypeUnmarshalJSON(t *testing.T) {
	typ := avro.Type{}
	b := []byte(`{"type": "bytes", "logicalType": "decimal"}`)
	if err := json.Unmarshal(b, &typ); err != nil {
		t.Error(err)
	}
	expected := avro.Type{
		Primitive: avro.Bytes,
		Logical:   "decimal",
	}
	if !reflect.DeepEqual(expected, typ) {
		t.Errorf("expected %+v but %+v", expected, typ)
	}
}

func TestUnionUnmarshalJSON(t *testing.T) {
	typ := avro.Type{}
	b := []byte(`["null", "int"]`)
	if err := json.Unmarshal(b, &typ); err != nil {
		t.Error(err)
	}
	expected := avro.Type{
		Primitive: avro.Union,
		UnionedTypes: []avro.Type{
			{Primitive: avro.Null},
			{Primitive: avro.Int},
		},
	}
	if !reflect.DeepEqual(expected, typ) {
		t.Errorf("expected %+v but %+v", expected, typ)
	}
}
