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
	e_fh := &FixedHeader{tp, d, q, r, rl, 0}
	a_fh := NewFixedHeader(tp, d, q, r, rl, 0)
	if !reflect.DeepEqual(e_fh, a_fh) {
		t.Errorf("got %v\nwant %v", a_fh, e_fh)
	}
}

func TestFixedHeader_GetWire(t *testing.T) {
	// TODO: check lonber remain length
	a_fh := NewFixedHeader(Publish, true, 2, true, 1, 0)
	a_wire := a_fh.GetWire()
	e_wire := []byte{0x3d, 0x01}
	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\nwant %v", e_wire, a_wire)
	}
}

func TestParseFixedHeader(t *testing.T) {
	dat := []byte{0x3d, 0x01}
	a_fh, _ := ParseFixedHeader(dat)
	e_fh := NewFixedHeader(Publish, true, 2, true, 1, 0)
	if !reflect.DeepEqual(a_fh, e_fh) {
		t.Errorf("got %v\nwant %v", a_fh, e_fh)
	}
}

/*func TestNewConnectMessage(t *testing.T) {
	keepAlive := uint16(10)
	id := "my-ID"
	cleanSession := false
	will := nil
	user := NewUser("daiki", "pass")
	a_m := NewConnectMessage(keepAlive, id, cleanSession, will, user)
	e_m := &ConnectMessage{
		FixedHeader: NewFixedHeader(
			Connect,
			false, 0, false,
			,
		),
		Protocol:  MQTT_3_1_1,
		Flags:     ,
		KeepAlive: keepAlive,
		ClientID:  id,
		Will:      will,
		User:      user,
	}

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}
}*/
