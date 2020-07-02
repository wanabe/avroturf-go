package avro

import (
	"encoding/json"
)

type Schema struct {
	Fullname string
	Type     Type     `json:"type"`
	Name     string   `json:"name"`
	Fields   []Schema `json:"fields"`
}

func ParseSchema(str string) (*Schema, error) {
	s := Schema{}
	err := json.Unmarshal([]byte(str), &s)
	if err != nil {
		return nil, err
	}
	return &s, nil
}
