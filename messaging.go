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
	SchemasByID map[uint32]*Schema
}

func NewMessaging(namespace string, path string, registryURL string) *Messaging {
	return &Messaging{
		NameSpace:   namespace,
		SchemaStore: NewSchemaStore(path),
		Registry: &CachedConfluentSchemaRegistry{
			Upstream: &ConfluentSchemaRegistry{
				RegistryURL: registryURL,
			},
			Cache: NewInMemoryCache(),
		},
		SchemasByID: make(map[uint32]*Schema),
	}
}

func (m *Messaging) GetSchema(data []byte) (*Schema, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("data too short: %d byte(s)", len(data))
	}
	magicByte := data[0]
	if magicByte != byte(0) {
		return nil, fmt.Errorf("Expected data to begin with a magic byte, got `%d`", magicByte)
	}

	schemaID := binary.BigEndian.Uint32(data[1:5])
	schema, hit := m.SchemasByID[schemaID]
	if !hit {
		m.Lock()
		defer m.Unlock()
		schema, hit = m.SchemasByID[schemaID]
		if !hit {
			s, err := m.Registry.FetchSchema(schemaID)
			if err != nil {
				return nil, err
			}
			schema = s
			m.SchemasByID[schemaID] = s
		}
	}
	return schema, nil
}

func (m *Messaging) Decode(data []byte, obj interface{}) error {
	writersSchema, err := m.GetSchema(data)
	if err != nil {
		return err
	}
	return avro.Unmarshal(writersSchema.Schema, data[5:], obj)
}

func (m *Messaging) DecodeByLocalSchema(data []byte, obj interface{}, schemaName string, namespace string) error {
	localSchema, err := m.SchemaStore.Find(schemaName, namespace)
	if err != nil {
		return err
	}
	return avro.Unmarshal(localSchema.Schema, data[5:], obj)
}

func (m *Messaging) GetRecordSchema(data []byte) (*avro.RecordSchema, error) {
	schema, err := m.GetSchema(data)
	if err != nil {
		return nil, err
	}
	recordSchema, ok := schema.Schema.(*avro.RecordSchema)
	if !ok {
		return nil, fmt.Errorf("invalid schema: %+v", schema)
	}
	return recordSchema, nil
}

func (m *Messaging) Encode(obj interface{}, subject string, schemaName string, namespace string) ([]byte, error) {
	schemaID, schema, err := m.RegisterSchema(subject, schemaName, namespace)
	if err != nil {
		return nil, err
	}
	return EncodeBySchemaAndId(obj, schemaID, schema)
}

func (m *Messaging) EncodeByLocalSchema(obj interface{}, schemaName string, namespace string, schemaID uint32) ([]byte, error) {
	schema, err := m.SchemaStore.Find(schemaName, namespace)
	if err != nil {
		return nil, err
	}
	return EncodeBySchemaAndId(obj, schemaID, schema)
}

func EncodeBySchemaAndId(obj interface{}, schemaID uint32, schema *Schema) ([]byte, error) {
	data, err := avro.Marshal(schema.Schema, obj)
	if err != nil || len(data) == 0 {
		return nil, err
	}
	data = append([]byte{0, 0, 0, 0, 0}, data...)
	binary.BigEndian.PutUint32(data[1:5], schemaID)
	return data, nil
}

func (m *Messaging) RegisterSchema(subject string, schemaName string, namespace string) (uint32, *Schema, error) {
	schema, err := m.SchemaStore.Find(schemaName, namespace)
	if err != nil {
		return 0, nil, err
	}
	if subject == "" {
		s, ok := schema.Schema.(avro.NamedSchema)
		if ok {
			subject = s.FullName()
		}
	}
	schemaID, err := m.Registry.Register(subject, schema)
	if err != nil {
		return 0, nil, err
	}
	return schemaID, schema, nil
}
