package avroturf

import "github.com/hamba/avro"

//go:generate mockgen -destination=mock_avroturf/mock_schema_registry.go -package mock_avroturf github.com/wanabe/avroturf-go SchemaRegistry
type SchemaRegistry interface {
	FetchSchema(schemaID uint32) (avro.Schema, error)
	Register(subject string, schema avro.Schema) (uint32, error)
}
