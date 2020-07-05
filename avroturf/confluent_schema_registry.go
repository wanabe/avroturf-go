package avroturf

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path"

	"github.com/hamba/avro"
)

type ConfluentSchemaRegistry struct {
	RegistryURL string
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

var Logger *log.Logger
var HTTPClient httpClient

func init() {
	HTTPClient = &http.Client{}
}

func (r *ConfluentSchemaRegistry) FetchSchema(schemaID uint32) (avro.Schema, error) {
	if Logger != nil {
		Logger.Printf("Fetching schema with id %d\n", schemaID)
	}
	data, err := r.request("GET", fmt.Sprintf("/schemas/ids/%d", schemaID))
	if err != nil {
		return nil, err
	}
	json, ok := data["schema"].(string)
	if !ok {
		return nil, errors.New("unexpected schema-registry response")
	}
	return avro.Parse(json)
}

func (r *ConfluentSchemaRegistry) request(method string, p string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	u, err := url.Parse(r.RegistryURL)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, p)
	req := &http.Request{Method: method, URL: u}
	res, err := HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}

	err = json.NewDecoder(res.Body).Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}
