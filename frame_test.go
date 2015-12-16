package MQTTg

import (
	"encoding/binary"
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

func TestConnectMessage(t *testing.T) {
	keepAlive := uint16(10)
	id := "my-ID"
	cleanSession := false
	flags := Will_Flag | WillQoS_1_Flag | WillRetain_Flag | Password_Flag | UserName_Flag
	will := NewWill("daiki/will", "message", true, 1)
	user := NewUser("daiki", "pass")
	length := uint32(16 + len(MQTT_3_1_1.Name+id+will.Topic+will.Message+user.Name+user.Passwd))
	fh := NewFixedHeader(Connect, false, 0, false, length, 0)
	e_m := &ConnectMessage{
		FixedHeader: fh,
		Protocol:    MQTT_3_1_1,
		Flags:       flags,
		KeepAlive:   keepAlive,
		ClientID:    id,
		Will:        will,
		User:        user,
	}
	a_m := NewConnectMessage(keepAlive, id, cleanSession, will, user)

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

	a_wire, _ := a_m.GetWire()
	e_wire := make([]byte, 10+2+4+2+2+len(id+will.Topic+will.Message+user.Name+user.Passwd))
	UTF8_encode(e_wire, MQTT_3_1_1.Name)
	e_wire[6] = MQTT_3_1_1.Level
	e_wire[7] = byte(flags)
	binary.BigEndian.PutUint16(e_wire[8:], keepAlive)
	UTF8_encode(e_wire[10:], id)
	UTF8_encode(e_wire[17:], will.Topic)
	UTF8_encode(e_wire[29:], will.Message)
	UTF8_encode(e_wire[38:], user.Name)
	UTF8_encode(e_wire[45:], user.Passwd)

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParseConnectMessage(fh, a_wire)
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestConnackMessage(t *testing.T) {
	sessionPresent := false
	code := Accepted
	fh := NewFixedHeader(Connack, false, 0, false, 2, 0)
	e_m := &ConnackMessage{
		FixedHeader:        fh,
		SessionPresentFlag: false,
		ReturnCode:         Accepted,
	}
	a_m := NewConnackMessage(sessionPresent, code)

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

	a_wire, _ := a_m.GetWire()
	e_wire := []byte{0x00, byte(code)}

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParseConnackMessage(fh, a_wire)
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestPublishMessage(t *testing.T) {
	dup := false
	qos := uint8(1)
	retain := true
	topic := "daiki/topic"
	payload := "message-data"
	id := uint16(5)
	length := uint32(2 + len(topic+payload))
	if qos > 0 {
		length += 2
	}

	fh := NewFixedHeader(Publish, dup, qos, retain, length, id)
	e_m := &PublishMessage{
		FixedHeader: fh,
		TopicName:   topic,
		Payload:     []byte(payload),
	}
	a_m := NewPublishMessage(dup, qos, retain, topic, id, []byte(payload))

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

	a_wire, _ := a_m.GetWire()
	e_wire := make([]byte, length)
	UTF8_encode(e_wire, topic)
	binary.BigEndian.PutUint16(e_wire[13:], id)
	copy(e_wire[15:], []byte(payload))

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParsePublishMessage(fh, a_wire)
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestPubackMessage(t *testing.T) {
	id := uint16(5)
	fh := NewFixedHeader(Puback, false, 0, false, 2, id)
	e_m := &PubackMessage{
		FixedHeader: fh,
	}
	a_m := NewPubackMessage(id)

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

	a_wire, _ := a_m.GetWire()
	e_wire := make([]byte, 2)
	binary.BigEndian.PutUint16(e_wire, id)

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParsePubackMessage(fh, a_wire)
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestPubrecMessage(t *testing.T) {
	id := uint16(5)
	fh := NewFixedHeader(Pubrec, false, 0, false, 2, id)
	e_m := &PubrecMessage{
		FixedHeader: fh,
	}
	a_m := NewPubrecMessage(id)

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

	a_wire, _ := a_m.GetWire()
	e_wire := make([]byte, 2)
	binary.BigEndian.PutUint16(e_wire, id)

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParsePubrecMessage(fh, a_wire)
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestPubrelMessage(t *testing.T) {
	id := uint16(5)
	fh := NewFixedHeader(Pubrel, false, 1, false, 2, id)
	e_m := &PubrelMessage{
		FixedHeader: fh,
	}
	a_m := NewPubrelMessage(id)

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

	a_wire, _ := a_m.GetWire()
	e_wire := make([]byte, 2)
	binary.BigEndian.PutUint16(e_wire, id)

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParsePubrelMessage(fh, a_wire)
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestPubcompMessage(t *testing.T) {
	id := uint16(5)
	fh := NewFixedHeader(Pubcomp, false, 0, false, 2, id)
	e_m := &PubcompMessage{
		FixedHeader: fh,
	}
	a_m := NewPubcompMessage(id)

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

	a_wire, _ := a_m.GetWire()
	e_wire := make([]byte, 2)
	binary.BigEndian.PutUint16(e_wire, id)

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParsePubcompMessage(fh, a_wire)
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}
