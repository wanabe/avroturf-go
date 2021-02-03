package avroturf

import "github.com/hamba/avro"

type InMemoryCache struct {
}

func (*InMemoryCache) LookupSchemaByID(schemaID uint32) avro.Schema {
	// TODO: implement
	return nil
}

func (*InMemoryCache) StoreSchemaByID(schemaID uint32, schema avro.Schema) avro.Schema {
	// TODO: implement
	return schema
}

func (*InMemoryCache) LookupBySchema(subject string, schema avro.Schema) uint32 {
	// TODO: implement
	return 0
}

func (*InMemoryCache) StoreBySchema(subject string, schema avro.Schema, schemaID uint32) uint32 {
	// TODO: implement
	return schemaID
}
