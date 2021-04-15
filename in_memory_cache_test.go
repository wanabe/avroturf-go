package avroturf_test

import (
	"reflect"
	"testing"

	"github.com/wanabe/avroturf-go"
)

func TestLookupSchemaByID(t *testing.T) {
	c := &avroturf.InMemoryCache{}
	schema := c.LookupSchemaByID(135)
	if schema != nil {
		t.Errorf("expected nil but got %v", schema)
	}

	s, err := avroturf.Parse(`
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
	c = &avroturf.InMemoryCache{
		SchemasByID: map[uint32]*avroturf.Schema{135: s},
	}
	schema = c.LookupSchemaByID(135)
	if !reflect.DeepEqual(s, schema) {
		t.Errorf("expected %v but got %v", s, schema)
	}
	schema = c.LookupSchemaByID(123)
	if schema != nil {
		t.Errorf("expected nil but got %v", schema)
	}
}

func TestStoreSchemaByID(t *testing.T) {
	c := &avroturf.InMemoryCache{SchemasByID: map[uint32]*avroturf.Schema{}}
	s, err := avroturf.Parse(`
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
	schema := c.StoreSchemaByID(135, s)
	if schema != s {
		t.Errorf("expected %v but got %v", s, schema)
	}

	expectedSchemasByID := map[uint32]*avroturf.Schema{135: s}
	if !reflect.DeepEqual(expectedSchemasByID, c.SchemasByID) {
		t.Errorf("expected %v but got %v", expectedSchemasByID, c.SchemasByID)

	}
}

func TestLookupIdBySchema(t *testing.T) {
	schemaStr := `
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
	`
	s1, err := avroturf.Parse(schemaStr)
	if err != nil {
		t.Error(err)
	}
	s2, err := avroturf.Parse(schemaStr)
	if err != nil {
		t.Error(err)
	}

	c := &avroturf.InMemoryCache{
		IdsBySchema: map[string]uint32{
			("subject1" + s1.String()): 135,
		},
	}
	id := c.LookupIdBySchema("subject1", s1)
	if id != 135 {
		t.Errorf("expected 135 but got %d", id)
	}
	id = c.LookupIdBySchema("subject1", s2)
	if id != 135 {
		t.Errorf("expected 135 but got %d", id)
	}
	id = c.LookupIdBySchema("subject2", s1)
	if id != 0 {
		t.Errorf("expected 0 but got %d", id)
	}
}
