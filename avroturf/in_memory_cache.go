package avroturf

import "github.com/wanabe/avroturf-go/avro"

type InMemoryCache struct {
}

func (*InMemoryCache) LookupSchemaByID(schemaID uint32) *avro.Schema {
	// TODO: implement
	return nil
}

func (*InMemoryCache) StoreSchemaByID(schemaID uint32, schema *avro.Schema) *avro.Schema {
	// TODO: implement
	return schema
}
