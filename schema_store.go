package avroturf

import (
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
	"sync"

	"github.com/hamba/avro"
)

type SchemaStore struct {
	sync.Mutex
	Path    string
	FS      http.FileSystem
	schemas map[string]avro.Schema
}

func NewSchemaStore(path string) *SchemaStore {
	return &SchemaStore{
		Path:    path,
		schemas: map[string]avro.Schema{},
	}
}

func (store *SchemaStore) Find(schemaName string, namespace string) (avro.Schema, error) {
	fullName := schemaName
	if namespace != "" {
		fullName = namespace + "." + schemaName
	}
	schema, hit := store.schemas[fullName]
	if hit {
		return schema, nil
	}
	store.Lock()
	defer store.Unlock()
	schema, hit = store.schemas[fullName]
	if hit {
		return schema, nil
	}
	return store.loadSchema(fullName)
}

func (store *SchemaStore) loadSchema(fullName string) (avro.Schema, error) {
	slicedPath := append([]string{store.Path}, strings.Split(fullName, ".")...)
	slicedPath[len(slicedPath)-1] = slicedPath[len(slicedPath)-1] + ".avsc"
	avsc, err := store.readFile(filepath.Join(slicedPath...))
	if err != nil {
		return nil, err
	}

	schema, err := avro.Parse(string(avsc))
	if err != nil {
		return nil, err
	}

	store.schemas[fullName] = schema
	return schema, nil
}

func (store *SchemaStore) readFile(filename string) ([]byte, error) {
	if store.FS != nil {
		r, err := store.FS.Open(filename)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return ioutil.ReadAll(r)
	}
	return ioutil.ReadFile(filename)
}
