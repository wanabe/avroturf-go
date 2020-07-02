package avroturf_test

import (
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/wanabe/avroturf-go/avro"
	"github.com/wanabe/avroturf-go/avroturf"
	"github.com/wanabe/avroturf-go/avroturf/mock_avroturf"
)

type record struct {
	Str string `avro:"str"`
}

func TestNewMessaging(t *testing.T) {
	messaging := avroturf.NewMessaging(
		"com.example",
		"./",
		"http://example.com",
	)

	if messaging.NameSpace != "com.example" {
		t.Errorf("unexpected namespace: %s", messaging.NameSpace)
	}
	registry := messaging.Registry.(*avroturf.CachedConfluentSchemaRegistry)
	if registry.Upstream.RegistryURL != "http://example.com" {
		t.Errorf("unexpected registry url: %s", registry.Upstream.RegistryURL)
	}
	store := messaging.SchemaStore
	if store.Path != "./" {
		t.Errorf("unexpected schema store path: %s", store.Path)
	}
}

func TestDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	schema := &avro.Schema{
		Type: avro.Type{Primitive: avro.Record},
		Fields: []avro.Schema{
			{
				Type: avro.Type{Primitive: avro.String},
				Name: "str",
			},
		},
	}

	registry := mock_avroturf.NewMockSchemaRegistryInterface(ctrl)
	registry.EXPECT().FetchSchema(uint32(123)).Return(schema, nil)

	messaging := &avroturf.Messaging{Registry: registry, NameSpace: "test-namespace", SchemasByID: make(map[uint32]*avro.Schema)}
	obj := record{}
	b := []byte{0, 0, 0, 0, 123, 8}
	b = append(b, "hoge"...)

	err := messaging.Decode(b, &obj, "test-name")
	if err != nil {
		t.Errorf("unexpected err: %v", err)
		return
	}
	if obj.Str != "hoge" {
		t.Errorf("expected \"%s\" but got \"%s\"", "hoge", obj.Str)
	}
}

func TestFailDecode(t *testing.T) {
	messaging := &avroturf.Messaging{}
	obj := record{}

	b := []byte{0, 0, 0, 0}
	err := messaging.Decode(b, &obj, "test-name")
	if err == nil || err.Error() != "data too short: 4 byte(s)" {
		t.Errorf("unexpected error: %+v", err)
	}

	b = []byte{11, 0, 0, 0, 0}
	err = messaging.Decode(b, &obj, "test-name")
	if err == nil || err.Error() != "Expected data to begin with a magic byte, got `11`" {
		t.Errorf("unexpected error: %+v", err)
	}
}
