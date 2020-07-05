package avroturf_test

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/hamba/avro"
	"github.com/wanabe/avroturf-go/avroturf"
)

type stubReadCloser struct {
	body []byte
}

type httpClientStub struct {
	DoFunc func(req *http.Request) (*http.Response, error)
}

func (r *stubReadCloser) Read(p []byte) (n int, err error) {
	l := copy(p, r.body)
	r.body = r.body[l:]
	return l, nil
}

func (*stubReadCloser) Close() error {
	return nil
}

func (c *httpClientStub) Do(req *http.Request) (*http.Response, error) {
	return c.DoFunc(req)
}

func TestFetchSchema(t *testing.T) {
	avroturf.HTTPClient = &httpClientStub{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			if expected := "http://schema-registry:8081/schemas/ids/135"; req.URL.String() != expected {
				t.Errorf("expected '%s' but got '%s'", expected, req.URL)
			}
			body := `{"schema":"{\"name\":\"TestRecord\",\"type\":\"record\", \"fields\":[{\"name\":\"Str1\",\"type\":\"string\"}]}"}`
			return &http.Response{
				Body: &stubReadCloser{
					body: []byte(body),
				},
			}, nil
		},
	}
	r := &avroturf.ConfluentSchemaRegistry{
		RegistryURL: "http://schema-registry:8081",
	}
	s, err := r.FetchSchema(uint32(135))
	if err != nil {
		t.Error(err)
	}
	expectedSchema, err := avro.Parse(`
		{
			"type": "record",
			"name": "TestRecord",
			"fields": [
				{
					"type": "string",
					"name": "Str1"
				}
			]
		}
	`)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(s, expectedSchema) {
		t.Errorf("expected %+v but got %+v", expectedSchema, s)
	}
}
