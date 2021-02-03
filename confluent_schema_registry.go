package avroturf

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"strings"

	"github.com/hamba/avro"
)

type ConfluentSchemaRegistry struct {
	RegistryURL string
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

const maxUint32 = int(^uint32(0))

var Logger *log.Logger
var HTTPClient httpClient

func init() {
	HTTPClient = &http.Client{}
}

func (r *ConfluentSchemaRegistry) FetchSchema(schemaID uint32) (avro.Schema, error) {
	if Logger != nil {
		Logger.Printf("Fetching schema with id %d\n", schemaID)
	}
	data, err := r.request("GET", fmt.Sprintf("/schemas/ids/%d", schemaID), nil)
	if err != nil {
		return nil, err
	}
	json, ok := data["schema"].(string)
	if !ok {
		return nil, errors.New("unexpected schema-registry response")
	}
	return avro.Parse(json)
}

func (r *ConfluentSchemaRegistry) Register(subject string, schema avro.Schema) (uint32, error) {
	builder := &strings.Builder{}
	err := json.NewEncoder(builder).Encode(map[string]string{"schema": schema.String()})
	if err != nil {
		return 0, err
	}
	body := ioutil.NopCloser(strings.NewReader(strings.TrimRight(builder.String(), "\n")))
	data, err := r.request("POST", fmt.Sprintf("/subjects/%s/versions", subject), body)
	if err != nil {
		return 0, err
	}

	id, hit := data["id"]
	if !hit {
		return 0, fmt.Errorf("invalid schema registry result: %v", data)
	}
	fid, ok := id.(float64)
	if !ok || fid < 0 || fid > float64(maxUint32) {
		return 0, fmt.Errorf("invalid schema registry id: %+v (%v)", id, reflect.TypeOf(id))
	}
	schemaID := uint32(fid)

	if Logger != nil {
		Logger.Printf("Registered schema for subject `%s`; id = %d\n", subject, schemaID)
	}
	return schemaID, nil
}

func (r *ConfluentSchemaRegistry) request(method string, p string, body io.ReadCloser) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	u, err := url.Parse(r.RegistryURL)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, p)
	h := map[string][]string{"Content-type": {"application/json"}}
	req := &http.Request{Method: method, URL: u, Header: h, Body: body}
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
