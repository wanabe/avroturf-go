package avroturf_test

import (
	"bytes"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/hamba/avro"
	"github.com/wanabe/avroturf-go"
	"github.com/wanabe/avroturf-go/mock_avroturf"
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

	schema, err := avro.Parse(`
		{
			"type": "record",
			"name": "TestSchemaRoot",
			"fields": [
				{
					"type": "string",
					"name": "str"
				}
			]
		}
	`)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}

	registry := mock_avroturf.NewMockSchemaRegistry(ctrl)
	registry.EXPECT().FetchSchema(uint32(123)).Return(schema, nil)

	messaging := &avroturf.Messaging{Registry: registry, NameSpace: "test-namespace", SchemasByID: make(map[uint32]avro.Schema)}
	obj := record{}
	b := []byte{0, 0, 0, 0, 123, 8}
	b = append(b, "hoge"...)

	err = messaging.Decode(b, &obj)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
		return
	}
	if obj.Str != "hoge" {
		t.Errorf("expected \"%s\" but got \"%s\"", "hoge", obj.Str)
	}
}

func TestDecodeByLocalSchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	messaging := &avroturf.Messaging{
		NameSpace:   "test-namespace",
		SchemaStore: avroturf.NewSchemaStore(path.Join(dir, "testdata")),
	}
	obj := record{}
	b := []byte{0, 0, 0, 0, 123, 8}
	b = append(b, "hoge"...)

	err = messaging.DecodeByLocalSchema(b, &obj, "test-name", "test-namespace")
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
	err := messaging.Decode(b, &obj)
	if err == nil || err.Error() != "data too short: 4 byte(s)" {
		t.Errorf("unexpected error: %+v", err)
	}

	b = []byte{11, 0, 0, 0, 0}
	err = messaging.Decode(b, &obj)
	if err == nil || err.Error() != "Expected data to begin with a magic byte, got `11`" {
		t.Errorf("unexpected error: %+v", err)
	}
}

func TestGetSchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	schema, err := avro.Parse(`
		{
			"type": "record",
			"name": "TestSchemaRoot",
			"fields": [
				{
					"type": "string",
					"name": "str"
				}
			]
		}
	`)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}

	registry := mock_avroturf.NewMockSchemaRegistry(ctrl)
	registry.EXPECT().FetchSchema(uint32(123)).Return(schema, nil)

	messaging := &avroturf.Messaging{Registry: registry, NameSpace: "test-namespace", SchemasByID: make(map[uint32]avro.Schema)}
	b := []byte{0, 0, 0, 0, 123, 8}
	b = append(b, "hoge"...)

	s, err := messaging.GetSchema(b)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
		return
	}
	if s != schema {
		t.Errorf("expected \"%v\" but got \"%v\"", schema, s)
	}
}

func TestGetSchemaIOnParallel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	schema, err := avro.Parse(`
		{
			"type": "record",
			"name": "TestSchemaRoot",
			"fields": [
				{
					"type": "string",
					"name": "str"
				}
			]
		}
	`)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}

	registry := mock_avroturf.NewMockSchemaRegistry(ctrl)
	registry.EXPECT().FetchSchema(uint32(123)).Return(schema, nil)

	messaging := &avroturf.Messaging{Registry: registry, NameSpace: "test-namespace", SchemasByID: make(map[uint32]avro.Schema)}
	b := []byte{0, 0, 0, 0, 123, 8}
	b = append(b, "hoge"...)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := messaging.GetSchema(b)
			if err != nil {
				t.Errorf("unexpected err: %v", err)
				return
			}
			if s != schema {
				t.Errorf("expected \"%v\" but got \"%v\"", schema, s)
			}
		}(i)
	}
	wg.Wait()
}

func TestGetRecordSchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	schema, err := avro.Parse(`
		{
			"type": "record",
			"name": "TestSchemaRoot",
			"fields": [
				{
					"type": "string",
					"name": "str"
				}
			]
		}
	`)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}

	registry := mock_avroturf.NewMockSchemaRegistry(ctrl)
	registry.EXPECT().FetchSchema(uint32(123)).Return(schema, nil)

	messaging := &avroturf.Messaging{Registry: registry, NameSpace: "test-namespace", SchemasByID: make(map[uint32]avro.Schema)}
	b := []byte{0, 0, 0, 0, 123, 8}
	b = append(b, "hoge"...)

	s, err := messaging.GetRecordSchema(b)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
		return
	}
	if s != schema {
		t.Errorf("expected %v but got %v", schema, s)
	}
	if s.Name() != "TestSchemaRoot" {
		t.Errorf("expected \"TestSchemaRoot\" but got \"%s\"", s.Name())
	}
}

func TestEncode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	schema, err := avro.Parse(`
		{
			"type": "record",
			"name": "TestSchemaRoot",
			"fields": [
				{
					"type": "string",
					"name": "str"
				}
			]
		}
	`)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}

	registry := mock_avroturf.NewMockSchemaRegistry(ctrl)
	registry.EXPECT().Register("TestSchemaRoot-input", schema).Return(uint32(123), nil)

	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	messaging := &avroturf.Messaging{
		Registry:    registry,
		NameSpace:   "test-namespace",
		SchemasByID: make(map[uint32]avro.Schema),
		SchemaStore: avroturf.NewSchemaStore(path.Join(dir, "testdata")),
	}
	obj := record{Str: "hoge"}

	b, err := messaging.Encode(&obj, "TestSchemaRoot-input", "test-name", "test-namespace")
	if err != nil {
		t.Errorf("unexpected err: %v", err)
		return
	}
	expected := []byte{0, 0, 0, 0, 123, 8}
	expected = append(expected, "hoge"...)
	if bytes.Compare(expected, b) != 0 {
		t.Errorf("expected %+v but got %+v", expected, b)
	}
}

func TestEncodeByLocalSchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	messaging := &avroturf.Messaging{
		NameSpace:   "test-namespace",
		SchemasByID: make(map[uint32]avro.Schema),
		SchemaStore: avroturf.NewSchemaStore(path.Join(dir, "testdata")),
	}
	obj := record{Str: "hoge"}

	b, err := messaging.EncodeByLocalSchema(&obj, "test-name", "test-namespace", 123)
	if err != nil {
		t.Errorf("unexpected err: %v", err)
		return
	}
	expected := []byte{0, 0, 0, 0, 123, 8}
	expected = append(expected, "hoge"...)
	if bytes.Compare(expected, b) != 0 {
		t.Errorf("expected %+v but got %+v", expected, b)
	}
}
