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
		},
		SchemasByID: make(map[uint32]*avro.Schema),
	}
}

func (m *Messaging) Decode(data []byte, obj interface{}, schemaName string) error {
	// TODO: get and use readerSchema
	magicByte := data[0]
	if magicByte != byte(0) {
		return fmt.Errorf("Expected data to begin with a magic byte, got `%d`", magicByte)
	}

	schemaID := binary.BigEndian.Uint32(data[1:5])
	writersSchema, hit := m.SchemasByID[schemaID]
	if !hit {
		writersSchema = m.Registry.FetchSchema(schemaID)
		m.SchemasByID[schemaID] = writersSchema
	}
	return avro.Unmarshal(data[5:], obj, writersSchema)
}
