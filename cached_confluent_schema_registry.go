package avroturf

import "github.com/hamba/avro"

type CachedConfluentSchemaRegistry struct {
	Upstream *ConfluentSchemaRegistry
	Cache    *InMemoryCache
}

func (r *CachedConfluentSchemaRegistry) FetchSchema(schemaID uint32) (avro.Schema, error) {
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
