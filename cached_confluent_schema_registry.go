package avroturf

type CachedConfluentSchemaRegistry struct {
	Upstream *ConfluentSchemaRegistry
	Cache    *InMemoryCache
}

func (r *CachedConfluentSchemaRegistry) FetchSchema(schemaID uint32) (*Schema, error) {
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

func (r *CachedConfluentSchemaRegistry) Register(subject string, schema *Schema) (uint32, error) {
	schemaId := r.Cache.LookupIdBySchema(subject, schema)
	if schemaId != 0 {
		return schemaId, nil
	}
	schemaId, err := r.Upstream.Register(subject, schema)
	if err != nil {
		return 0, err
	}
	return r.Cache.StoreIdBySchema(subject, schema, schemaId), nil
}
