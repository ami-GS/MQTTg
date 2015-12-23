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
	a_fh, a_fhLen, _ := ParseFixedHeader(dat)
	e_fh := NewFixedHeader(Publish, true, 2, true, 1, 0)
	if !reflect.DeepEqual(a_fh, e_fh) {
		t.Errorf("got %v\nwant %v", a_fh, e_fh)
	}

	if a_fhLen != 2 {
		t.Errorf("got %v\nwant %v", a_fhLen, 2)
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
	fh_wire := fh.GetWire()
	e_wire := make([]byte, len(fh_wire)+10+2+4+2+2+len(id+will.Topic+will.Message+user.Name+user.Passwd))
	copy(e_wire, fh_wire)
	UTF8_encode(e_wire[2:], MQTT_3_1_1.Name)
	e_wire[8] = MQTT_3_1_1.Level
	e_wire[9] = byte(flags)
	binary.BigEndian.PutUint16(e_wire[10:], keepAlive)
	UTF8_encode(e_wire[12:], id)
	UTF8_encode(e_wire[19:], will.Topic)
	UTF8_encode(e_wire[31:], will.Message)
	UTF8_encode(e_wire[40:], user.Name)
	UTF8_encode(e_wire[47:], user.Passwd)

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParseConnectMessage(fh, a_wire[2:])
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
	fh_wire := fh.GetWire()
	e_wire := []byte{fh_wire[0], fh_wire[1], 0x00, byte(code)}

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParseConnackMessage(fh, a_wire[2:])
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
	fh_wire := fh.GetWire()
	e_wire := make([]byte, len(fh_wire)+int(length))
	copy(e_wire, fh_wire)
	UTF8_encode(e_wire[len(fh_wire):], topic)
	binary.BigEndian.PutUint16(e_wire[len(fh_wire)+13:], id)
	copy(e_wire[len(fh_wire)+15:], []byte(payload))

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParsePublishMessage(fh, a_wire[2:])
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
	e_wire := make([]byte, 4)
	copy(e_wire, fh.GetWire())
	binary.BigEndian.PutUint16(e_wire[2:], id)

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParsePubackMessage(fh, a_wire[2:])
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
	e_wire := make([]byte, 4)
	copy(e_wire, fh.GetWire())
	binary.BigEndian.PutUint16(e_wire[2:], id)

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParsePubrecMessage(fh, a_wire[2:])
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
	e_wire := make([]byte, 4)
	copy(e_wire, fh.GetWire())
	binary.BigEndian.PutUint16(e_wire[2:], id)

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParsePubrelMessage(fh, a_wire[2:])
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
	e_wire := make([]byte, 4)
	copy(e_wire, fh.GetWire())
	binary.BigEndian.PutUint16(e_wire[2:], id)

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParsePubcompMessage(fh, a_wire[2:])
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestSubscribeMessage(t *testing.T) {
	id := uint16(5)
	topics := make([]SubscribeTopic, 2)
	topics[0] = *NewSubscribeTopic("daiki/topic1", 1)
	topics[1] = *NewSubscribeTopic("daiki/topic2", 2)
	length := 2 + 3*len(topics)
	for _, v := range topics {
		length += len(v.Topic)
	}
	fh := NewFixedHeader(Subscribe, false, 1, false, uint32(length), id)
	e_m := &SubscribeMessage{
		FixedHeader:     fh,
		SubscribeTopics: topics,
	}
	a_m := NewSubscribeMessage(id, topics)

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

	a_wire, _ := a_m.GetWire()
	fh_wire := fh.GetWire()
	e_wire := make([]byte, len(fh_wire)+int(length))
	copy(e_wire, fh_wire)
	binary.BigEndian.PutUint16(e_wire[len(fh_wire):], id)
	nxt := 2 + len(fh_wire)
	for _, topic := range topics {
		nxt += UTF8_encode(e_wire[nxt:], topic.Topic)
		e_wire[nxt] = topic.QoS
		nxt++
	}

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParseSubscribeMessage(fh, a_wire[len(fh_wire):])
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestSubackMessage(t *testing.T) {
	id := uint16(5)
	codes := []SubscribeReturnCode{AckMaxQoS1, SubscribeFailure}
	length := uint32(len(codes) + 2)
	fh := NewFixedHeader(Suback, false, 0, false, length, id)
	e_m := &SubackMessage{
		FixedHeader: fh,
		ReturnCodes: codes,
	}
	a_m := NewSubackMessage(id, codes)

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

	a_wire, _ := a_m.GetWire()
	fh_wire := fh.GetWire()
	e_wire := make([]byte, len(fh_wire)+int(length))
	copy(e_wire, fh_wire)
	binary.BigEndian.PutUint16(e_wire[len(fh_wire):], id)
	for i, v := range codes {
		e_wire[len(fh_wire)+2+i] = uint8(v)
	}

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParseSubackMessage(fh, a_wire[len(fh_wire):])
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestUnsubscribeMessage(t *testing.T) {
	id := uint16(5)
	topics := []string{"topic/topic1", "topic/topic2"}
	length := uint32(30)
	fh := NewFixedHeader(Unsubscribe, false, 1, false, length, id)
	e_m := &UnsubscribeMessage{
		FixedHeader: fh,
		TopicNames:  topics,
	}
	a_m := NewUnsubscribeMessage(id, topics)

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

	a_wire, _ := a_m.GetWire()
	fh_wire := fh.GetWire()
	e_wire := make([]byte, len(fh_wire)+int(length))
	copy(e_wire, fh_wire)
	binary.BigEndian.PutUint16(e_wire[len(fh_wire):], id)
	UTF8_encode(e_wire[len(fh_wire)+2:], topics[0])
	UTF8_encode(e_wire[len(fh_wire)+4+len(topics[0]):], topics[1])

	if !reflect.DeepEqual(a_wire, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	a_mm, _ := ParseUnsubscribeMessage(fh, a_wire[len(fh_wire):])
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}
