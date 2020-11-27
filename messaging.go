package avroturf

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/hamba/avro"
)

type Messaging struct {
	sync.Mutex
	NameSpace   string
	SchemaStore *SchemaStore
	Registry    SchemaRegistry
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

func (m *Messaging) GetSchema(data []byte) (avro.Schema, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("data too short: %d byte(s)", len(data))
	}
	magicByte := data[0]
	if magicByte != byte(0) {
		return nil, fmt.Errorf("Expected data to begin with a magic byte, got `%d`", magicByte)
	}

	schemaID := binary.BigEndian.Uint32(data[1:5])
	m.Lock()
	defer m.Unlock()
	schema, hit := m.SchemasByID[schemaID]
	if !hit {
		s, err := m.Registry.FetchSchema(schemaID)
		if err != nil {
			return nil, err
		}
		schema = s
		m.SchemasByID[schemaID] = s
	}
	return schema, nil
}

func (m *Messaging) Decode(data []byte, obj interface{}) error {
	writersSchema, err := m.GetSchema(data)
	if err != nil {
		return err
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

func (m *Messaging) GetRecordSchema(data []byte) (*avro.RecordSchema, error) {
	schema, err := m.GetSchema(data)
	if err != nil {
		return nil, err
	}
	recordSchema, ok := schema.(*avro.RecordSchema)
	if !ok {
		return nil, fmt.Errorf("invalid schema: %+v", schema)
	}
	return recordSchema, nil
}
