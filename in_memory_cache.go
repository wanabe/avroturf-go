package avroturf

import (
	"sync"

	"github.com/hamba/avro"
)

type InMemoryCache struct {
	SchemasByID             map[uint32]avro.Schema
	IdsBySchema             map[string]uint32
	SchemasBySubjectVersion map[string]avro.Schema
	sync.Mutex
}

func NewInMemoryCache() *InMemoryCache {
	return &InMemoryCache{
		SchemasByID:             map[uint32]avro.Schema{},
		IdsBySchema:             map[string]uint32{},
		SchemasBySubjectVersion: map[string]avro.Schema{},
	}
}

func (c *InMemoryCache) LookupSchemaByID(schemaID uint32) avro.Schema {
	c.Lock()
	defer c.Unlock()
	return c.SchemasByID[schemaID]
}

func (c *InMemoryCache) StoreSchemaByID(schemaID uint32, schema avro.Schema) avro.Schema {
	c.Lock()
	defer c.Unlock()
	c.SchemasByID[schemaID] = schema
	return schema
}

func (c *InMemoryCache) LookupIdBySchema(subject string, schema avro.Schema) uint32 {
	key := subject + schema.String()
	c.Lock()
	defer c.Unlock()
	return c.IdsBySchema[key]
}

func (c *InMemoryCache) StoreIdBySchema(subject string, schema avro.Schema, schemaID uint32) uint32 {
	key := subject + schema.String()
	c.Lock()
	defer c.Unlock()
	c.IdsBySchema[key] = schemaID
	return schemaID
}
