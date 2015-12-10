package MQTTg

import (
	"reflect"
	"testing"
)

func TestNewFixedHeader(t *testing.T) {
	tp := Connect
	r, d := true, true
	var q uint8 = 2
	var rl uint32 = 1
	e_fh := &FixedHeader{tp, d, q, r, rl}
	a_fh := NewFixedHeader(tp, d, q, r, rl)
	if !reflect.DeepEqual(e_fh, a_fh) {
		t.Errorf("got %v\nwant %v", a_fh, e_fh)
	}
}

func TestFixedHeader_GetWire(t *testing.T) {
	// TODO: check lonber remain length
	a_fh := NewFixedHeader(Publish, true, 2, true, 1)
	a_wire := a_fh.GetWire()
	e_wire := []byte{0x3d, 0x01}
	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\nwant %v", e_wire, a_wire)
	}
}

func TestParseFixedHeader(t *testing.T) {
	dat := []byte{0x3d, 0x01}
	a_fh, _ := ParseFixedHeader(dat)
	e_fh := NewFixedHeader(Publish, true, 2, true, 1)
	if !reflect.DeepEqual(a_fh, e_fh) {
		t.Errorf("got %v\nwant %v", a_fh, e_fh)
	}
}
