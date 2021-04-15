package avroturf_test

import (
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"

	"github.com/wanabe/avroturf-go"
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
	expectedSchema, err := avroturf.Parse(`
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

func TestRegister(t *testing.T) {
	avroturf.HTTPClient = &httpClientStub{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			if expected := "http://schema-registry:8081/subjects/TestRecord/versions"; req.URL.String() != expected {
				t.Errorf("expected '%s' but got '%s'", expected, req.URL)
			}
			reqBytes, err := ioutil.ReadAll(req.Body)
			if err != nil {
				t.Error(err)
			}
			if expected := `{"schema":"{\"fields\":[{\"name\":\"Str1\",\"type\":\"string\"}],\"name\":\"TestRecord\",\"type\":\"record\"}"}`; string(reqBytes) != expected {
				t.Errorf("expected:\n  %#v but got:\n  %#v", expected, string(reqBytes))
			}

			body := `{"id":135}`
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
	schema, err := avroturf.Parse(`
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
	id, err := r.Register("TestRecord", schema)
	if err != nil {
		t.Error(err)
	}
	if id != 135 {
		t.Errorf("expected %d but got %d", 135, id)
	}
}
