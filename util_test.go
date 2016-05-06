package MQTTg

import (
	"bytes"
	"reflect"
	"testing"
	"fmt"
)

func TestUTF8_encode(t *testing.T) {
	data := "hello world"
	var a_wire, e_wire bytes.Buffer
	UTF8_encode(&a_wire, data)
	e_wire.Write([]byte{0x00, 0x0b})
	e_wire.Write([]byte(data))
	e_b := e_wire.Bytes()
	a_b := e_wire.Bytes()
	if !reflect.DeepEqual(a_b, e_b) {
		t.Errorf("got %v\nwant %v", a_wire, e_wire)
	}
	if len(a_b) != len(e_b) {
		t.Errorf("got %v\nwant %v", len(a_b), len(e_b))
	}

}

func TestUTF8_decode(t *testing.T) {
	e_data := "hello world"
	var wire bytes.Buffer

	UTF8_encode(&wire, e_data)
	e_b := wire.Bytes()
	r := bytes.NewReader(e_b)
	var a_data string
	a_len := UTF8_decode(r, &a_data)
	if a_data != e_data {
		t.Errorf("got %v\nwant %v", a_data, e_data)
	}
	if a_len != uint16(len(e_b)) {
		t.Errorf("got %v\nwant %v", a_len, len(e_b))
	}
}

func TestRemainEncodeDecode(t *testing.T) {
	exData := []uint32{0, 127, 128, 16383, 16384, 2097151, 2097152, 268435455}
	e_wires := [][]byte{[]byte{0x00}, []byte{0x7f},
		[]byte{0x80, 0x01}, []byte{0xff, 0x7f},
		[]byte{0x80, 0x80, 0x01}, []byte{0xff, 0xff, 0x7f},
		[]byte{0x80, 0x80, 0x80, 0x01}, []byte{0xff, 0xff, 0xff, 0x7f}}
	for i, dat := range exData {
		var a_wire bytes.Buffer
		RemainEncode(&a_wire, dat)
		a_b := a_wire.Bytes()
		fmt.Println(a_b)
		fmt.Println(e_wires[i])
		if !reflect.DeepEqual(a_b, e_wires[i]) {
			t.Errorf("got %v\nwant %v", a_b, e_wires[i])
		}
	}

	for i, data := range e_wires {
		r := bytes.NewReader(data)

		var a_data uint32
		a_len, _ := RemainDecode(r, &a_data)
		if a_data != exData[i] {
			t.Errorf("got %v\nwant %v", a_data, exData[i])
		}
		if len(data) != a_len {
			t.Errorf("got %v\nwant %v", a_len, len(data))
		}
	}

}
