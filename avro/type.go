package avro

import (
	"fmt"
	"reflect"
)

type Type uint

const (
	Null Type = iota + 1
	Boolean
	Int
	Long
	Float
	Double
	Bytes
	String
)

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
