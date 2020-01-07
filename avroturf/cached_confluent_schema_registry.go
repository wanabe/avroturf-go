package avroturf

import "github.com/wanabe/avroturf-go/avro"

type CachedConfluentSchemaRegistry struct {
	Upstream *ConfluentSchemaRegistry
}

//go:generate mockgen -destination=mock_avroturf/mock_schema_registry.go github.com/wanabe/avroturf-go/avroturf SchemaRegistryInterface
type SchemaRegistryInterface interface {
	FetchSchema(schemaId uint32) *avro.Schema
}

func (c *CachedConfluentSchemaRegistry) FetchSchema(schemaId uint32) *avro.Schema {
	// TODO: implement
	return nil
}
