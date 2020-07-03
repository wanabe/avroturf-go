package avro

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

type Decoder struct {
	Buffer []byte
	Offset int
}

var (
	nullInt32Type = reflect.TypeOf(sql.NullInt32{})
	nullInt64Type = reflect.TypeOf(sql.NullInt64{})
)

func Unmarshal(buf []byte, msg interface{}, w *Schema) error {
	d := &Decoder{Buffer: buf}
	return d.Unmarshal(msg, w)
}

func (d *Decoder) Unmarshal(msg interface{}, w *Schema) error {
	if w.Type.Primitive != Record {
		return fmt.Errorf("unexpected schema type: %s", w.Type.String())
	}
	for _, s := range w.Fields {
		if err := d.unmarshalField(msg, &s); err != nil {
			return err
		}
	}
	return nil
}

func (d *Decoder) unmarshalField(msg interface{}, w *Schema) error {
	return d.unmarshalFieldWithType(msg, w, w.Type)
}

func (d *Decoder) unmarshalFieldWithType(msg interface{}, w *Schema, t Type) error {
	switch t.Primitive {
	// TODO: support other types
	case Null:
		return putNull(msg, w.Name)
	case String:
		if s, err := d.DecodeString(); err != nil {
			return err
		} else if err := putString(msg, w.Name, s); err != nil {
			return err
		}
		return nil
	case Int:
		if n, err := d.DecodeInt(); err != nil {
			return err
		} else if err := putInt(msg, w.Name, n); err != nil {
			return err
		}
		return nil
	case Long:
		if n, err := d.DecodeInt64(); err != nil {
			return err
		} else if err := putInt64(msg, w.Name, n); err != nil {
			return err
		}
		return nil
	case Union:
		i, err := d.DecodeInt()
		if err != nil {
			return err
		}
		if i < 0 || i >= len(w.Type.UnionedTypes) {
			return fmt.Errorf("invalid schema index %d", i)
		}
		return d.unmarshalFieldWithType(msg, w, w.Type.UnionedTypes[i])
	}
	return fmt.Errorf("unkown type: %s", t.String())
}

func (d *Decoder) DecodeString() (string, error) {
	size, err := d.DecodeInt()
	right := d.Offset + size
	if err != nil {
		return "", err
	}
	if size <= 0 {
		return "", fmt.Errorf("unexpected string length: %d", size)
	}
	if len(d.Buffer) < right {
		return "", fmt.Errorf("unexpected buffer length: %d < %d", len(d.Buffer), right)
	}

	str := string(d.Buffer[d.Offset:right])
	d.Offset = right
	return str, nil
}

func (d *Decoder) DecodeInt() (int, error) {
	num, err := d.DecodeInt64()
	return int(num), err
}

func (d *Decoder) DecodeInt64() (int64, error) {
	n := int64(0)
	offset := -1
	flagMask := int64(0)

	for d.Offset < len(d.Buffer) {
		c := d.Buffer[d.Offset]
		d.Offset++
		b := int64(c)

		if offset < 0 {
			if b&1 == 1 {
				flagMask = -1
			}
			n = n | (b&0x7f)>>1
		} else {
			n = n | (b&0x7f)<<offset
		}
		offset += 7

		if b&0x80 == 0 {
			return n ^ flagMask, nil
		}
	}

	return 0, errors.New("can't read int")
}

func putString(msg interface{}, name string, str string) error {
	f, err := getField(msg, name)
	if err != nil {
		return err
	}
	f.SetString(str)
	return nil
}

func putInt(msg interface{}, name string, num int) error {
	return putInt64(msg, name, int64(num))
}

func putInt64(msg interface{}, name string, num int64) error {
	f, err := getField(msg, name)
	if err != nil {
		return err
	}
	switch f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		f.SetInt(int64(num))
		return nil
	case reflect.Struct:
		switch f.Type() {
		case nullInt32Type:
			f.Set(reflect.ValueOf(sql.NullInt32{Int32: int32(num), Valid: true}))
			return nil
		case nullInt64Type:
			f.Set(reflect.ValueOf(sql.NullInt64{Int64: num, Valid: true}))
			return nil
		}
	}
	return fmt.Errorf("invalid field: %v", f)
}

func putNull(msg interface{}, name string) error {
	f, err := getField(msg, name)
	if err != nil {
		return err
	}
	if f.Kind() == reflect.Struct {
		switch f.Type() {
		// TODO: support other nullable types
		case nullInt32Type:
			f.Set(reflect.ValueOf(sql.NullInt32{}))
		case nullInt64Type:
			f.Set(reflect.ValueOf(sql.NullInt64{}))
		}
	}
	return nil
}
