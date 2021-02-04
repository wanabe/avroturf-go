// +build integration

package avroturf_test

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/wanabe/avroturf-go"
)

type TestObj struct {
	Str string `avro:"str"`
}

func TestIntegration_EncodeAndDecode(t *testing.T) {
	messaging := avroturf.NewMessaging("test-namespace", "testdata/", "http://schema-registry:8081")

	avroturf.HTTPClient = &http.Client{}
	srcObj := TestObj{Str: "test"}
	b, err := messaging.Encode(srcObj, "TestSchema-value", "test-name", "test-namespace")
	if err != nil {
		t.Error(err)
	}
	dstObj := TestObj{}
	err = messaging.Decode(b, &dstObj)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(srcObj, dstObj) {
		t.Errorf("expected %v but got %v", srcObj, dstObj)
	}

	// cached data
	avroturf.HTTPClient = nil
	srcObj = TestObj{Str: "cached"}
	b, err = messaging.Encode(srcObj, "TestSchema-value", "test-name", "test-namespace")
	if err != nil {
		t.Error(err)
	}
	dstObj = TestObj{}
	err = messaging.Decode(b, &dstObj)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(srcObj, dstObj) {
		t.Errorf("expected %v but got %v", srcObj, dstObj)
	}
}
