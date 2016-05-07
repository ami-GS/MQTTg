package MQTTg

import (
	"encoding/binary"
	"reflect"
	"testing"
	"bytes"
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

func TestFixedHeader_Write(t *testing.T) {
	// TODO: check lonber remain length
	a_fh := NewFixedHeader(Publish, true, 2, true, 1, 0)
	var a_wire bytes.Buffer
	a_fh.Write(&a_wire)
	e_wire := []byte{0x3d, 0x01}
	if !reflect.DeepEqual(a_wire.Bytes(), e_wire) {
		t.Errorf("got %v\nwant %v", e_wire, a_wire)
	}
}

func TestParseFixedHeader(t *testing.T) {
	dat := []byte{0x3d, 0x01}
	r := bytes.NewReader(dat)
	a_fh, a_fhLen, _ := ParseFixedHeader(r)
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

	var a_wire, e_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&e_wire)
	a_b := a_wire.Bytes()

	UTF8_encode(&e_wire, MQTT_3_1_1.Name)
	binary.Write(&e_wire, binary.BigEndian, byte(MQTT_3_1_1.Level))
	binary.Write(&e_wire, binary.BigEndian, byte(flags))
	binary.Write(&e_wire, binary.BigEndian, keepAlive)
	UTF8_encode(&e_wire, id)
	UTF8_encode(&e_wire, will.Topic)
	UTF8_encode(&e_wire, will.Message)
	UTF8_encode(&e_wire, user.Name)
	UTF8_encode(&e_wire, user.Passwd)
	e_b := e_wire.Bytes()

	if !reflect.DeepEqual(a_b, e_b) {
		t.Errorf("got %v\n\t want %v", a_b, e_b)
	}

	r := bytes.NewReader(a_b[2:])
	a_mm, _ := ParseConnectMessage(fh, r)
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

	var a_wire, fh_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&fh_wire)
	a_b := a_wire.Bytes()
	f_b := fh_wire.Bytes()

	e_wire := []byte{f_b[0], f_b[1], 0x00, byte(code)}

	if !reflect.DeepEqual(a_b, e_wire) {
		t.Errorf("got %v\n\t want %v", a_b, e_wire)
	}

	r := bytes.NewReader(a_b[2:])
	a_mm, _ := ParseConnackMessage(fh, r)
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

	var a_wire, e_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&e_wire)
	a_b := a_wire.Bytes()
	UTF8_encode(&e_wire, topic)
	binary.Write(&e_wire, binary.BigEndian, id)
	e_wire.Write([]byte(payload))
	e_b := e_wire.Bytes()

	if !reflect.DeepEqual(a_b, e_b) {
		t.Errorf("got %v\n\t want %v", a_wire, e_b)
	}

	r := bytes.NewReader(a_b[2:])
	a_mm, _ := ParsePublishMessage(fh, r)
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

	var a_wire, fh_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&fh_wire)
	a_b := a_wire.Bytes()
	f_b := fh_wire.Bytes()
	e_wire := make([]byte, 4)
	copy(e_wire, f_b)
	binary.BigEndian.PutUint16(e_wire[2:], id)

	if !reflect.DeepEqual(a_b, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	r := bytes.NewReader(a_b[2:])
	a_mm, _ := ParsePubackMessage(fh, r)
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

	var a_wire, fh_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&fh_wire)
	a_b := a_wire.Bytes()
	f_b := fh_wire.Bytes()

	e_wire := make([]byte, 4)
	copy(e_wire, f_b)
	binary.BigEndian.PutUint16(e_wire[2:], id)

	if !reflect.DeepEqual(a_b, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	r := bytes.NewReader(a_b[2:])
	a_mm, _ := ParsePubrecMessage(fh, r)
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

	var a_wire, fh_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&fh_wire)
	a_b := a_wire.Bytes()
	f_b := fh_wire.Bytes()

	e_wire := make([]byte, 4)
	copy(e_wire, f_b)
	binary.BigEndian.PutUint16(e_wire[2:], id)

	if !reflect.DeepEqual(a_b, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	r := bytes.NewReader(a_b[2:])
	a_mm, _ := ParsePubrelMessage(fh, r)
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

	var a_wire, fh_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&fh_wire)
	a_b := a_wire.Bytes()
	f_b := fh_wire.Bytes()

	e_wire := make([]byte, 4)
	copy(e_wire, f_b)
	binary.BigEndian.PutUint16(e_wire[2:], id)

	if !reflect.DeepEqual(a_b, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	r := bytes.NewReader(a_b[2:])
	a_mm, _ := ParsePubcompMessage(fh, r)
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestSubscribeMessage(t *testing.T) {
	id := uint16(5)
	topics := make([]*SubscribeTopic, 2)
	topics[0] = NewSubscribeTopic("daiki/topic1", 1)
	topics[1] = NewSubscribeTopic("daiki/topic2", 2)
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

	var a_wire, e_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&e_wire)
	a_b := a_wire.Bytes()
	binary.Write(&e_wire, binary.BigEndian, id)
	for _, topic := range topics {
		UTF8_encode(&e_wire, topic.Topic)
		binary.Write(&e_wire, binary.BigEndian, topic.QoS)
	}
	e_b := e_wire.Bytes()

	if !reflect.DeepEqual(a_b, e_b) {
		t.Errorf("got %v\n\t want %v", a_b, e_b)
	}
	r := bytes.NewReader(a_b[2:])
	a_mm, _ := ParseSubscribeMessage(fh, r)
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

	var a_wire, fh_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&fh_wire)
	a_b := a_wire.Bytes()
	f_b := fh_wire.Bytes()
	e_wire := make([]byte, len(f_b)+int(length))
	copy(e_wire, f_b)
	binary.BigEndian.PutUint16(e_wire[len(f_b):], id)
	for i, v := range codes {
		e_wire[len(f_b)+2+i] = uint8(v)
	}

	if !reflect.DeepEqual(a_b, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	r := bytes.NewReader(a_b[len(f_b):])
	a_mm, _ := ParseSubackMessage(fh, r)
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

	var a_wire, e_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&e_wire)
	a_b := a_wire.Bytes()
	binary.Write(&e_wire, binary.BigEndian, id)
	UTF8_encode(&e_wire, topics[0])
	UTF8_encode(&e_wire, topics[1])
	e_b := e_wire.Bytes()

	if !reflect.DeepEqual(a_b, e_b) {
		t.Errorf("got %v\n\t want %v", a_b, e_b)
	}

	r := bytes.NewReader(a_b[2:])
	a_mm, _ := ParseUnsubscribeMessage(fh, r)
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestUnsubackMessage(t *testing.T) {
	id := uint16(5)
	fh := NewFixedHeader(Unsuback, false, 0, false, 2, id)
	e_m := &UnsubackMessage{
		FixedHeader: fh,
	}
	a_m := NewUnsubackMessage(id)

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

	var a_wire, fh_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&fh_wire)
	a_b := a_wire.Bytes()
	f_b := fh_wire.Bytes()
	e_wire := make([]byte, 4)
	copy(e_wire, f_b)
	binary.BigEndian.PutUint16(e_wire[2:], id)

	if !reflect.DeepEqual(a_b, e_wire) {
		t.Errorf("got %v\n\t want %v", a_wire, e_wire)
	}

	r := bytes.NewReader(a_b[len(f_b):])
	a_mm, _ := ParseUnsubackMessage(fh, r)
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestPingreqMessage(t *testing.T) {
	fh := NewFixedHeader(Pingreq, false, 0, false, 0, 0)
	e_m := &PingreqMessage{
		FixedHeader: fh,
	}
	a_m := NewPingreqMessage()

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}


	var a_wire, fh_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&fh_wire)
	a_b := a_wire.Bytes()
	f_b := fh_wire.Bytes()

	if !reflect.DeepEqual(a_b, f_b) {
		t.Errorf("got %v\n\t want %v", a_b, f_b)
	}

	r := bytes.NewReader(a_b[len(f_b):])
	a_mm, _ := ParsePingreqMessage(fh, r)
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestPingrespMessage(t *testing.T) {
	fh := NewFixedHeader(Pingresp, false, 0, false, 0, 0)
	e_m := &PingrespMessage{
		FixedHeader: fh,
	}
	a_m := NewPingrespMessage()

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

	var a_wire, fh_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&fh_wire)
	a_b := a_wire.Bytes()
	f_b := fh_wire.Bytes()

	if !reflect.DeepEqual(a_b, f_b) {
		t.Errorf("got %v\n\t want %v", a_b, f_b)
	}

	r := bytes.NewReader(a_b[len(f_b):])
	a_mm, _ := ParsePingrespMessage(fh, r)
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestDisconnectMessage(t *testing.T) {
	fh := NewFixedHeader(Disconnect, false, 0, false, 0, 0)
	e_m := &DisconnectMessage{
		FixedHeader: fh,
	}
	a_m := NewDisconnectMessage()

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

	var a_wire, fh_wire bytes.Buffer
	a_m.Write(&a_wire)
	fh.Write(&fh_wire)
	a_b := a_wire.Bytes()
	f_b := fh_wire.Bytes()

	if !reflect.DeepEqual(a_b, f_b) {
		t.Errorf("got %v\n\t want %v", a_b, f_b)
	}

	r := bytes.NewReader(a_b[len(f_b):])
	a_mm, _ := ParseDisconnectMessage(fh, r)
	if !reflect.DeepEqual(a_mm, e_m) {
		t.Errorf("got %v\nwant %v", a_mm, e_m)
	}
}

func TestReadFrame(t *testing.T) {
	keepAlive := uint16(10)
	id := "my-ID"
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

	var e_wire bytes.Buffer
	e_m.Write(&e_wire)
	e_b := e_wire.Bytes()
	r := bytes.NewReader(e_b)
	a_m, _ := ReadFrame(r)

	if !reflect.DeepEqual(a_m, e_m) {
		t.Errorf("got %v\nwant %v", a_m, e_m)
	}

}
