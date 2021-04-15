package avroturf

//go:generate mockgen -destination=mock_avroturf/mock_schema_registry.go -package mock_avroturf github.com/wanabe/avroturf-go SchemaRegistry
type SchemaRegistry interface {
	FetchSchema(schemaID uint32) (*Schema, error)
	Register(subject string, schema *Schema) (uint32, error)
}
