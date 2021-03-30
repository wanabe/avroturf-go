package avroturf_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/hamba/avro"
	"github.com/rakyll/statik/fs"

	"github.com/wanabe/avroturf-go"
	_ "github.com/wanabe/avroturf-go/statik"
)

func TestFind(t *testing.T) {
	s, err := avro.Parse(`
		{
			"type": "record",
			"name": "TestSchema",
			"fields": [
				{
					"type": "string",
					"name": "str"
				}
			]
		}
	`)
	if err != nil {
		t.Error(err)
	}

	store := avroturf.NewSchemaStore("/")
	schema, err := store.Find("test-name", "test-namespace")
	if err == nil || !strings.Contains(err.Error(), "no such file or directory") {
		t.Errorf(`expected "no such file or directory" error but "%v"`, err)
	}
	if schema != nil {
		t.Errorf("expected nil but %v", schema)
	}

	fs, err := fs.New()
	if err != nil {
		t.Fatal(err)
	}
	store.FS = fs
	schema, err = store.Find("test-name", "test-namespace")
	if err != nil {
		t.Error(err)
	}
	if reflect.DeepEqual(s, schema) {
		t.Errorf("expected %v by %v", s, schema)
	}
}
