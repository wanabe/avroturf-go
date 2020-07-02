package avro

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type Type uint

const (
	Null Type = iota + 1
	Record
	Boolean
	Int
	Long
	Float
	Double
	Bytes
	String
)

func (t Type) String() string {
	switch t {
	case Null:
		return "null"
	case Record:
		return "record"
	case Boolean:
		return "boolean"
	case Int:
		return "int"
	case Long:
		return "long"
	case Float:
		return "float"
	case Double:
		return "double"
	case Bytes:
		return "bytes"
	case String:
		return "string"
	}
	return "unkown"
}

func (t *Type) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("unexpected data")
	}
	switch s {
	case "null":
		*t = Null
	case "record":
		*t = Record
	case "boolean":
		*t = Boolean
	case "int":
		*t = Int
	case "long":
		*t = Long
	case "float":
		*t = Float
	case "double":
		*t = Double
	case "bytes":
		*t = Bytes
	case "string":
		*t = String
	}
	return nil
}

var typeNameIndexMap map[reflect.Type]map[string]int

func init() {
	typeNameIndexMap = make(map[reflect.Type]map[string]int)
}

func getField(obj interface{}, name string) (reflect.Value, error) {
	e := reflect.ValueOf(obj).Elem()
	t := e.Type()
	m := nameIndexMap(t)

	i := m[name] - 1
	if i >= 0 {
		return e.Field(i), nil
	}

	return reflect.Value{}, fmt.Errorf("undefined field `%s`", name)
}

func nameIndexMap(t reflect.Type) map[string]int {
	m := typeNameIndexMap[t]
	if m == nil {
		m = make(map[string]int)
		n := t.NumField()
		for i := 0; i < n; i++ {
			if name := t.Field(i).Tag.Get("avro"); name != "" {
				m[name] = i + 1
			}
		}
		typeNameIndexMap[t] = m
	}

	return m
}
