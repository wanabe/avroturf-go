package avro

import (
	"errors"
	"fmt"
)

type Decoder struct {
	Buffer []byte
	Offset int
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
	n := 0
	offset := -1
	flagMask := 0

	for d.Offset < len(d.Buffer) {
		c := d.Buffer[d.Offset]
		d.Offset++
		b := int(c)

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
