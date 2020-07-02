package avro

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type primitiveType uint
type Type struct {
	Primitive    primitiveType
	Logical      string
	UnionedTypes []Type
}

const (
	Unknown primitiveType = iota
	Null
	Record
	Boolean
	Int
	Long
	Float
	Double
	Bytes
	String
	Union
)

func (t Type) String() string {
	switch t.Primitive {
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
	var obj interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	return t.unmarshalObject(obj)
}

func (t *Type) unmarshalObject(obj interface{}) error {
	if s, ok := obj.(string); ok {
		t.Primitive = typeNameToPrimitive(s)
		return nil
	} else if m, ok := obj.(map[string]interface{}); ok {
		// TODO: support logical type attributes
		s, ok := m["type"].(string)
		s2, ok2 := m["logicalType"].(string)
		if ok && ok2 {
			t.Primitive = typeNameToPrimitive(s)
			t.Logical = s2
			return nil
		}
	} else if a, ok := obj.([]interface{}); ok {
		u := make([]Type, len(a))
		for i, o := range a {
			if err := u[i].unmarshalObject(o); err != nil {
				return err
			}
		}
		t.Primitive = Union
		t.UnionedTypes = u
		return nil
	}
	return fmt.Errorf("unexpected type: %s", obj)
}

func typeNameToPrimitive(s string) primitiveType {
	switch s {
	case "null":
		return Null
	case "record":
		return Record
	case "boolean":
		return Boolean
	case "int":
		return Int
	case "long":
		return Long
	case "float":
		return Float
	case "double":
		return Double
	case "bytes":
		return Bytes
	case "string":
		return String
	}
	return Unknown
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
