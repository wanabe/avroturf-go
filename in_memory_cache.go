package avroturf

import (
	"sync"
)

type InMemoryCache struct {
	SchemasByID             map[uint32]*Schema
	IdsBySchema             map[string]uint32
	SchemasBySubjectVersion map[string]*Schema
	sync.Mutex
}

func NewInMemoryCache() *InMemoryCache {
	return &InMemoryCache{
		SchemasByID:             map[uint32]*Schema{},
		IdsBySchema:             map[string]uint32{},
		SchemasBySubjectVersion: map[string]*Schema{},
	}
}

func (c *InMemoryCache) LookupSchemaByID(schemaID uint32) *Schema {
	c.Lock()
	defer c.Unlock()
	return c.SchemasByID[schemaID]
}

func (c *InMemoryCache) StoreSchemaByID(schemaID uint32, schema *Schema) *Schema {
	c.Lock()
	defer c.Unlock()
	c.SchemasByID[schemaID] = schema
	return schema
}

func (c *InMemoryCache) LookupIdBySchema(subject string, schema *Schema) uint32 {
	key := subject + schema.String()
	c.Lock()
	defer c.Unlock()
	return c.IdsBySchema[key]
}

func (c *InMemoryCache) StoreIdBySchema(subject string, schema *Schema, schemaID uint32) uint32 {
	key := subject + schema.String()
	c.Lock()
	defer c.Unlock()
	c.IdsBySchema[key] = schemaID
	return schemaID
}
