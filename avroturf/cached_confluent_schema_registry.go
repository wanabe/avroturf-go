package avroturf

import "github.com/wanabe/avroturf-go/avro"

type CachedConfluentSchemaRegistry struct {
	Upstream *ConfluentSchemaRegistry
	Cache    *InMemoryCache
}

//go:generate mockgen -destination=mock_avroturf/mock_schema_registry.go github.com/wanabe/avroturf-go/avroturf SchemaRegistryInterface
type SchemaRegistryInterface interface {
	FetchSchema(schemaID uint32) (*avro.Schema, error)
}

func (r *CachedConfluentSchemaRegistry) FetchSchema(schemaID uint32) (*avro.Schema, error) {
	schema := r.Cache.LookupSchemaByID(schemaID)
	if schema != nil {
		return schema, nil
	}

	schema, err := r.Upstream.FetchSchema(schemaID)
	if err != nil {
		return nil, err
	}
	return r.Cache.StoreSchemaByID(schemaID, schema), nil
}
