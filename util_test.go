package MQTTg

import (
	"reflect"
	"testing"
)

func TestUTF8_encode(t *testing.T) {
	data := "hello world"
	a_wire := make([]byte, 2+len(data))
	a_len := UTF8_encode(a_wire, data)
	e_wire := make([]byte, 2+len(data))
	e_wire[0], e_wire[1] = 0x00, 0x0b
	copy(e_wire[2:], []byte(data))
	e_len := len(e_wire)
	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\nwant %v", a_wire, e_wire)
	}
	if a_len != e_len {
		t.Errorf("got %v\nwant %v", a_len, e_len)
	}

}

func TestUTF8_decode(t *testing.T) {
	e_data := "hello world"
	wire := make([]byte, 2+len(e_data))
	e_len := UTF8_encode(wire, e_data)
	a_len, a_data := UTF8_decode(wire)
	if a_data != e_data {
		t.Errorf("got %v\nwant %v", a_data, e_data)
	}
	if a_len != e_len {
		t.Errorf("got %v\nwant %v", a_len, e_len)
	}
}

func TestRemainEncodeDecode(t *testing.T) {
	exData := []uint32{0, 127, 128, 16383, 16384, 2097151, 2097152, 268435455}
	e_wires := [][]byte{[]byte{0x00}, []byte{0x7f},
		[]byte{0x80, 0x01}, []byte{0xff, 0x7f},
		[]byte{0x80, 0x80, 0x01}, []byte{0xff, 0xff, 0x7f},
		[]byte{0x80, 0x80, 0x80, 0x01}, []byte{0xff, 0xff, 0xff, 0x7f}}
	for i, dat := range exData {
		a_wire := make([]byte, len(e_wires[i]))
		RemainEncode(a_wire, dat)
		if !reflect.DeepEqual(a_wire, e_wires[i]) {
			t.Errorf("got %v\nwant %v", a_wire, e_wires[i])
		}
	}

	for i, data := range e_wires {
		a_data, a_len, _ := RemainDecode(data)
		if a_data != exData[i] {
			t.Errorf("got %v\nwant %v", a_data, exData[i])
		}
		if len(data) != a_len {
			t.Errorf("got %v\nwant %v", a_len, len(data))
		}
	}

}
