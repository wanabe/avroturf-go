package avroturf

import (
	"encoding/binary"
	"fmt"

	"github.com/hamba/avro"
)

type Messaging struct {
	NameSpace   string
	SchemaStore *SchemaStore
	Registry    SchemaRegistryInterface
	SchemasByID map[uint32]avro.Schema
}

func NewMessaging(namespace string, path string, registryURL string) *Messaging {
	return &Messaging{
		NameSpace:   namespace,
		SchemaStore: NewSchemaStore(path),
		Registry: &CachedConfluentSchemaRegistry{
			Upstream: &ConfluentSchemaRegistry{
				RegistryURL: registryURL,
			},
			Cache: &InMemoryCache{},
		},
		SchemasByID: make(map[uint32]avro.Schema),
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
	return avro.Unmarshal(writersSchema, data[5:], obj)
}

func (m *Messaging) DecodeByLocalSchema(data []byte, obj interface{}, schemaName string, namespace string) error {
	localSchema, err := m.SchemaStore.Find(schemaName, namespace)
	if err != nil {
		return err
	}
	return avro.Unmarshal(localSchema, data[5:], obj)
}
