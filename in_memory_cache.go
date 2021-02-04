package avroturf

import "github.com/hamba/avro"

type InMemoryCache struct {
	SchemasByID             map[uint32]avro.Schema
	IdsBySchema             map[string]uint32
	SchemasBySubjectVersion map[string]avro.Schema
}

func NewInMemoryCache() *InMemoryCache {
	return &InMemoryCache{
		SchemasByID:             map[uint32]avro.Schema{},
		IdsBySchema:             map[string]uint32{},
		SchemasBySubjectVersion: map[string]avro.Schema{},
	}
}

func (c *InMemoryCache) LookupSchemaByID(schemaID uint32) avro.Schema {
	return c.SchemasByID[schemaID]
}

func (c *InMemoryCache) StoreSchemaByID(schemaID uint32, schema avro.Schema) avro.Schema {
	c.SchemasByID[schemaID] = schema
	return schema
}

func (c *InMemoryCache) LookupIdBySchema(subject string, schema avro.Schema) uint32 {
	key := subject + schema.String()
	return c.IdsBySchema[key]
}

func (c *InMemoryCache) StoreIdBySchema(subject string, schema avro.Schema, schemaID uint32) uint32 {
	key := subject + schema.String()
	c.IdsBySchema[key] = schemaID
	return schemaID
}
