package avroturf

import (
	"encoding/binary"
	"fmt"

	"github.com/wanabe/avroturf-go/avro"
)

type Messaging struct {
	NameSpace   string
	SchemaStore *SchemaStore
	Registry    SchemaRegistryInterface
	SchemasByID map[uint32]*avro.Schema
}

func NewMessaging(n string, p string, u string) *Messaging {
	return &Messaging{
		NameSpace: n,
		SchemaStore: &SchemaStore{
			Path: p,
		},
		Registry: &CachedConfluentSchemaRegistry{
			Upstream: &ConfluentSchemaRegistry{
				RegistryURL: u,
			},
			Cache: &InMemoryCache{},
		},
		SchemasByID: make(map[uint32]*avro.Schema),
	}
}

func (m *Messaging) Decode(data []byte, obj interface{}, schemaName string) error {
	// TODO: get and use readerSchema
	if len(data) < 5 {
		return fmt.Errorf("data too short: %d byte(s)", len(data))
	}
	magicByte := data[0]
	if magicByte != byte(0) {
		return fmt.Errorf("Expected data to begin with a magic byte, got `%d`", magicByte)
	}

	schemaID := binary.BigEndian.Uint32(data[1:5])
	writersSchema, hit := m.SchemasByID[schemaID]
	if !hit {
		schema, err := m.Registry.FetchSchema(schemaID)
		if err != nil {
			return err
		}
		writersSchema = schema
		m.SchemasByID[schemaID] = writersSchema
	}
	return avro.Unmarshal(data[5:], obj, writersSchema)
}
