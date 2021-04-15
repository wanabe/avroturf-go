package avroturf

import (
	"encoding/json"

	"github.com/hamba/avro"
)

type Schema struct {
	str    string
	Schema avro.Schema
}

func Parse(str string) (*Schema, error) {
	var j interface{}
	err := json.Unmarshal([]byte(str), &j)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(j)
	if err != nil {
		return nil, err
	}

	s := Schema{str: string(b)}
	s.Schema, err = avro.Parse(s.str)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (s *Schema) String() string {
	return s.str
}
