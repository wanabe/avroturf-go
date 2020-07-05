package avroturf

import (
	"github.com/hamba/avro"
)

type SchemaStore struct {
	Path string
}

func (*SchemaStore) Find(schemaName string, namespace string) avro.Schema {
	// TODO: implement
	return nil
}
