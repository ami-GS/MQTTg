package MQTTg

import (
	"encoding/binary"
)

type MessageType uint8

const (
	Reserved_1 MessageType = iota
	Connect
	Connack
	Publish
	Puback
	Pubrec
	Pubrel
	Pubcomp
	Subscribe
	Suback
	Unsubscribe
	Unsuback
	Pingreq
	Pingresp
	Disconnect
	Reserved_2
)

func (self MessageType) String() string {
	types := []string{
		"Reserved_1",
		"Connect",
		"Connack",
		"Publish",
		"Puback",
		"Pubrec",
		"Pubrel",
		"Pubcomp",
		"Subscribe",
		"Suback",
		"Unsubscribe",
		"Unsuback",
		"Pingreq",
		"Pingresp",
		"Disconnect",
		"Reserved_2",
	}
	return types[int(self)]
}

type FixedHeader struct {
	Type         MessageType
	Dup          bool
	QoS          uint8
	Retain       bool
	RemainLength uint8
}

func NewFixedHeader(mType MessageType, dup bool, qos uint8, retain bool, length uint8) *FixedHeader {
	return &FixedHeader{
		Type:         mType,
		Dup:          dup,
		QoS:          qos,
		Retain:       retain,
		RemainLength: length,
	}
}

func (self *FixedHeader) GetWire() (wire []uint8) {
	wire = make([]uint8, 2)
	wire[0] = uint8(self.Type)
	if self.Dup {
		wire[0] |= 0x08
	}
	wire[0] |= (self.QoS << 1)
	if self.Retain {
		wire[0] |= 0x01
	}
	wire[1] = self.RemainLength

	return
}

func ParseFixedHeader(wire []byte) (h *FixedHeader) {
	var dup, retain bool
	var qos uint8
	mType := MessageType(wire[0] >> 4)
	if mType == Publish {
		if wire[0]&0x08 == 0x08 {
			dup = true
		}
		qos = (wire[0] >> 1) & 0x03

		if wire[0]&0x01 == 0x01 {
			retain = true
		}
	}
	h = NewFixedHeader(mType, dup, qos, retain, wire[1])

	return
}

type VariableHeader interface {
	VHeaderParse(data []byte)
	VHeaderWire() ([]byte, error)
	VHeaderString() string
}

var ParseMessage map[MessageType]interface{} = map[MessageType]interface{}{
	Connect:     ParseConnectMessage,
	Connack:     ParseConnackMessage,
	Publish:     ParsePublishMessage,
	Puback:      ParsePubackMessage,
	Pubrec:      ParsePubrecMessage,
	Pubrel:      ParsePubrelMessage,
	Pubcomp:     ParsePubcompMessage,
	Subscribe:   ParseSubscribeMessage,
	Suback:      ParseSubackMessage,
	Unsubscribe: ParseUnsubscribeMessage,
	Unsuback:    ParseUnsubackMessage,
	Pingreq:     ParsePingreqMessage,
	Pingresp:    ParsePingrespMessage,
	Disconnect:  ParseDisconnectMessage,
}

type Message interface {
	GetWire() ([]byte, error)
	String() string
}

type ConnectFlag uint8

const (
	CleanSession ConnectFlag = 0x02
	WillFlag     ConnectFlag = 0x04
	WillQoS_0    ConnectFlag = 0x00
	WillQoS_1    ConnectFlag = 0x08
	WillQoS_2    ConnectFlag = 0x10
	WillQoS_3    ConnectFlag = 0x18
	WillRetain   ConnectFlag = 0x20
	Password     ConnectFlag = 0x40
	UserName     ConnectFlag = 0x80
)

func (self ConnectFlag) String() (s string) {
	if self&CleanSession == CleanSession {
		s += "CleanSession\n"
	}
	if self&WillFlag == WillFlag {
		s += "WillFlag\n"
	}
	switch self & WillQoS_3 {
	case WillQoS_0:
		s += "WillQoS_0\n"
	case WillQoS_1:
		s += "WillQoS_1\n"
	case WillQoS_2:
		s += "WillQoS_2\n"
	case WillQoS_3:
		s += "WillQoS_3\n"
	}
	if self&Password == Password {
		s += "Password\n"
	}
	if self&UserName == UserName {
		s += "UserName\n"
	}
	return s
}

type ConnectMessage struct {
	*FixedHeader
	Protocol     *Protocol
	KeepAlive    uint16
	ClientID     string
	CleanSession bool
	Will         *Will
	User         *User
}

type Protocol struct {
	Name  string
	Level uint8
}

// TODO this should be const
var MQTT_3_1_1 *Protocol = &Protocol{
	Name:  "MQTT",
	Level: 4,
}

type User struct {
	Name   string
	Passwd string
}

func NewUser(name, pass string) *User {
	return &User{
		Name:   name,
		Passwd: pass,
	}
}

type Will struct {
	Topic   string
	Message string
	Retain  bool
	QoS     uint8
}

func NewWill(topic, message string, retain bool, qos uint8) *Will {
	return &Will{
		Topic:   topic,
		Message: message,
		Retain:  retain,
		QoS:     qos,
	}
}

func NewConnectMessage(keepAlive uint16, clientID string, cleanSession bool, will *Will, user *User) *ConnectMessage {
	length := 6 + len(MQTT_3_1_1.Name) + 2 + len(clientID)
	if will != nil {
		length += 4 + len(will.Topic) + len(will.Message)
	}
	if user != nil {
		// TODO : password encryption here
		length += 4 + len(user.Name) + len(user.Passwd)
	}
	return &ConnectMessage{
		FixedHeader: NewFixedHeader(
			Connect,
			false, 0, false,
			uint8(length),
		),
		Protocol:     MQTT_3_1_1,
		KeepAlive:    keepAlive,
		ClientID:     clientID,
		CleanSession: cleanSession,
		Will:         will,
		User:         user,
	}
}

func (self *ConnectMessage) GetWire() (wire []uint8) {
	wire = make([]uint8, 2+len(self.Protocol.Name)+6)
	cursor := UTF8_encode(wire, self.Protocol.Name)

	wire[cursor] = self.Protocol.Level
	cursor += 2 // skip flag

	binary.BigEndian.PutUint16(wire[cursor:], self.KeepAlive)
	cursor += 2
	cursor += UTF8_encode(wire[cursor:], self.ClientID)

	var flag ConnectFlag
	if self.CleanSession {
		flag |= CleanSession
	}
	if self.Will != nil {
		flag |= WillFlag
		flag |= ConnectFlag(self.Will.QoS << 3)
		if self.Will.Retain {
			flag |= WillRetain
		}
		cursor += UTF8_encode(wire[cursor:], self.Will.Topic)
		cursor += UTF8_encode(wire[cursor:], self.Will.Message)
	}
	if self.User != nil {
		if len(self.User.Name) > 0 {
			flag |= UserName
			cursor += UTF8_encode(wire[cursor:], self.User.Name)
		}
		if len(self.User.Passwd) > 0 {
			flag |= Password
			cursor += UTF8_encode(wire[cursor:], self.User.Passwd)
		}
	}
	wire[3+len(self.Protocol.Name)] = uint8(flag)

	return
}

func ParseConnectMessage(wire []byte) (m *ConnectMessage) {
	cursor, protoName := UTF8_decode(wire)
	level := wire[cursor]
	if MQTT_3_1_1.Name != protoName {
	}
	if MQTT_3_1_1.Level != level {
	}
	// TODO: validate protocol version

	flag := ConnectFlag(wire[cursor+1])
	cursor += 2
	keepAlive := binary.BigEndian.Uint16(wire[cursor:])
	cursor += 2
	cTmp, clientID := UTF8_decode(wire[cursor:])
	cursor += cTmp

	var will *Will = nil
	if flag&WillFlag == WillFlag {
		cTmp1, topic := UTF8_decode(wire[cursor:])
		cTmp, message := UTF8_decode(wire[cursor+cTmp1:])
		cursor += cTmp1 + cTmp
		retain := flag&WillRetain == WillRetain
		qos := uint8(flag&WillQoS_3) >> 3
		will = NewWill(topic, message, retain, qos)
	}
	cleanSession := flag&CleanSession == CleanSession

	var user *User = nil
	if flag&UserName == UserName || flag&Password == Password {
		var name, passwd string
		if flag&UserName == UserName {
			cTmp, name = UTF8_decode(wire[cursor:])
			cursor += cTmp
		}
		if flag&Password == Password {
			cTmp, passwd = UTF8_decode(wire[cursor:])
			cursor += cTmp
		}
		user = NewUser(name, passwd)
	}

	// NOTE: This calculates FixedHeader again, inefficient
	m = NewConnectMessage(keepAlive, clientID, cleanSession, will, user)

	return
}

type ConnectReturnCode uint8

const (
	Accepted ConnectReturnCode = iota
	UnacceptableProtocolVersion
	IdentifierRejected
	ServerUnavailable
	BadUserNameOrPassword
	NotAuthorized
)

func (self ConnectReturnCode) String() string {
	codes := []string{
		"Accepted",
		"UnacceptableProtocolVersion",
		"IdentifierRejected",
		"ServerUnavailable",
		"BadUserNameOrPassword",
		"NotAuthorized",
	}
	return codes[int(self)]
}

type ConnackMessage struct {
	*FixedHeader
	SessionPresentFlag bool
	ReturnCode         ConnectReturnCode
}

func NewConnackMessage(flag bool, code ConnectReturnCode) *ConnackMessage {
	return &ConnackMessage{
		FixedHeader: NewFixedHeader(
			Connack,
			false, 0, false,
			2,
		),
		SessionPresentFlag: flag,
		ReturnCode:         code,
	}
}

func (self *ConnackMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2)
	if self.SessionPresentFlag {
		wire[0] = 0x01
	}
	wire[1] = byte(self.ReturnCode)

	return
}

func ParseConnackMessage(wire []byte) (m *ConnackMessage) {
	if wire[0] == 1 {
		m.SessionPresentFlag = true
	}
	m.ReturnCode = ConnectReturnCode(wire[1])
	return
}

type PublishMessage struct {
	*FixedHeader
	TopicName string
	PacketID  uint16
	Payload   []uint8
}

func NewPublishMessage(dub bool, qos uint8, retain bool, topic string, id uint16, payload []uint8) *PublishMessage {
	length := 4 + len(topic) + len(payload)
	return &PublishMessage{
		FixedHeader: NewFixedHeader(
			Publish,
			dub, qos, retain,
			uint8(length),
		),
		TopicName: topic,
		PacketID:  id,
		Payload:   payload,
	}
}

func (self *PublishMessage) GetWire() (wire []byte) {
	topicLen := len(self.TopicName)
	wire = make([]byte, 4+topicLen+len(self.Payload))
	binary.BigEndian.PutUint16(wire, uint16(topicLen))
	for i, v := range []byte(self.TopicName) {
		wire[2+i] = v
	}
	binary.BigEndian.PutUint16(wire[2+topicLen:], self.PacketID)
	for i, v := range self.Payload {
		wire[4+topicLen+i] = v
	}

	return
}

func ParsePublishMessage(wire []byte) (m *PublishMessage) {
	var topicLen uint16 = uint16((wire[0] << 8) + wire[1])
	m.TopicName = string(wire[:topicLen])
	m.PacketID = binary.BigEndian.Uint16(wire[topicLen : topicLen+2])
	m.Payload = wire[topicLen+1:]

	return
}

type PubackMessage struct {
	*FixedHeader
	PacketID uint16
}

func NewPubackMessage(id uint16) *PubackMessage {
	return &PubackMessage{
		FixedHeader: NewFixedHeader(
			Puback,
			false, 0, false,
			2,
		),
		PacketID: id,
	}
}

func (self *PubackMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2)
	binary.BigEndian.PutUint16(wire, self.PacketID)

	return
}

func ParsePubackMessage(wire []byte) (m *PubackMessage) {
	m.PacketID = binary.BigEndian.Uint16(wire[:2])

	return
}

type PubrecMessage struct {
	*FixedHeader
	PacketID uint16
}

func NewPubrecMessage(id uint16) *PubrecMessage {
	return &PubrecMessage{
		FixedHeader: NewFixedHeader(
			Pubrec,
			false, 0, false,
			2,
		),
		PacketID: id,
	}
}

func (self *PubrecMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2)
	binary.BigEndian.PutUint16(wire, self.PacketID)

	return
}

func ParsePubrecMessage(wire []byte) (m *PubrecMessage) {
	m.PacketID = binary.BigEndian.Uint16(wire[:2])

	return
}

type PubrelMessage struct {
	*FixedHeader
	PacketID uint16
}

func NewPubrelMessage(id uint16) *PubrelMessage {
	return &PubrelMessage{
		FixedHeader: NewFixedHeader(
			Pubrel,
			false, 0, false,
			2,
		),
		PacketID: id,
	}
}

func (self *PubrelMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2)
	binary.BigEndian.PutUint16(wire, self.PacketID)

	return
}

func ParsePubrelMessage(wire []byte) (m *PubrelMessage) {
	m.PacketID = binary.BigEndian.Uint16(wire[:2])

	return
}

type PubcompMessage struct {
	*FixedHeader
	PacketID uint16
}

func NewPubcompMessage(id uint16) *PubcompMessage {
	return &PubcompMessage{
		FixedHeader: NewFixedHeader(
			Pubcomp,
			false, 0, false,
			2,
		),
		PacketID: id,
	}
}

func (self *PubcompMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2)
	binary.BigEndian.PutUint16(wire, self.PacketID)

	return
}

func ParsePubcompMessage(wire []byte) (m *PubcompMessage) {
	m.PacketID = binary.BigEndian.Uint16(wire[:2])

	return
}

type SubscribeTopic struct {
	Topic []uint8
	QoS   uint8
}

func NewSubscribeTopic(topic []uint8, qos uint8) *SubscribeTopic {
	return &SubscribeTopic{
		Topic: topic,
		QoS:   qos,
	}
}

type SubscribeMessage struct {
	*FixedHeader
	PacketID        uint16
	SubscribeTopics []SubscribeTopic
}

func NewSubscribeMessage(id uint16, topics []SubscribeTopic) *SubscribeMessage {
	length := 2 + 3*len(topics)
	for _, v := range topics {
		length += len(v.Topic)
	}

	return &SubscribeMessage{
		FixedHeader: NewFixedHeader(
			Subscribe,
			false, 0, false,
			uint8(length),
		),
		PacketID:        id,
		SubscribeTopics: topics,
	}
}

func (self *SubscribeMessage) GetWire() (wire []byte) {
	topicsLen := 0
	for _, v := range self.SubscribeTopics {
		topicsLen += len(v.Topic)
	}
	wire = make([]byte, 2+3*len(self.SubscribeTopics)+topicsLen)
	binary.BigEndian.PutUint16(wire, self.PacketID)
	cursor := 2
	for _, v := range self.SubscribeTopics {
		topicLen := len(v.Topic)
		binary.BigEndian.PutUint16(wire[cursor:], uint16(topicLen))
		cursor += 2

		for j, b := range []byte(v.Topic) {
			wire[cursor+2+j] = b
		}
		cursor += topicLen
		wire[cursor] = v.QoS
	}

	return
}

func ParseSubscribeMessage(wire []byte) (m *SubscribeMessage) {
	m.PacketID = binary.BigEndian.Uint16(wire[:2])
	allLen := len(wire)
	for i := 2; i < allLen; {
		topicLen := int(binary.BigEndian.Uint16(wire[i : i+2]))
		topic := wire[i+2 : i+2+topicLen]
		m.SubscribeTopics = append(m.SubscribeTopics,
			*NewSubscribeTopic(topic, wire[i+2+topicLen])) // check
		i += 3 + topicLen
	}

	return
}

type SubscribeReturnCode uint8

const (
	AckMaxQoS0 SubscribeReturnCode = iota
	AckMaxQoS1
	AckMaxQoS2
	SubscribeFailure SubscribeReturnCode = 0x80
)

func (self SubscribeReturnCode) String() string {
	codes := map[SubscribeReturnCode]string{
		0x00: "AckMaxQoS0",
		0x01: "AckMaxQoS1",
		0x02: "AckMaxQoS2",
		0x80: "SubscribeFailure",
	}
	return codes[self]
}

type SubackMessage struct {
	*FixedHeader
	PacketID    uint16
	ReturnCodes []SubscribeReturnCode
}

func NewSubackMessage(id uint16, codes []SubscribeReturnCode) *SubackMessage {
	length := 2 + len(codes)
	return &SubackMessage{
		FixedHeader: NewFixedHeader(
			Suback,
			false, 0, false,
			uint8(length),
		),
		PacketID:    id,
		ReturnCodes: codes,
	}
}

func (self *SubackMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2+len(self.ReturnCodes))
	binary.BigEndian.PutUint16(wire, self.PacketID)
	for i, v := range self.ReturnCodes {
		wire[2+i] = byte(v)
	}
	return
}

func ParseSubackMessage(wire []byte) (m *SubackMessage) {
	m.PacketID = binary.BigEndian.Uint16(wire[:2])
	for _, v := range wire[2:] {
		m.ReturnCodes = append(m.ReturnCodes, SubscribeReturnCode(v))
	}

	return
}

type UnsubscribeMessage struct {
	*FixedHeader
	PacketID uint16
	Topics   [][]uint8
}

func NewUnsubscribeMessage(id uint16, topics [][]uint8) *UnsubscribeMessage {
	length := 2 + 2*len(topics)
	for _, v := range topics {
		length = len(v)
	}
	return &UnsubscribeMessage{
		FixedHeader: NewFixedHeader(
			Unsubscribe,
			false, 0, false,
			uint8(length),
		),
		PacketID: id,
		Topics:   topics,
	}
}

func (self *UnsubscribeMessage) GetWire() (wire []byte) {
	allLen := 0
	for _, v := range self.Topics {
		allLen += len(v)
	}
	wire = make([]byte, 2+2*len(self.Topics)+allLen)
	binary.BigEndian.PutUint16(wire, self.PacketID)

	cursor := 2
	for _, v := range self.Topics {
		binary.BigEndian.PutUint16(wire, uint16(len(v)))
		cursor += 2

		for j, b := range v {
			wire[cursor+j] = b
		}
		cursor += len(v)
	}

	return
}

func ParseUnsubscribeMessage(wire []byte) (m *UnsubscribeMessage) {
	m.PacketID = binary.BigEndian.Uint16(wire[:2])
	allLen := len(wire)
	for i := 2; i < allLen; {
		topicLen := int(binary.BigEndian.Uint16(wire[i : i+2]))
		m.Topics = append(m.Topics, wire[i+2:i+2+topicLen])
		i += 2 + topicLen
	}
	return
}

type UnsubackMessage struct {
	*FixedHeader
	PacketID uint16
}

func NewUnsubackMessage(id uint16) *UnsubackMessage {
	return &UnsubackMessage{
		FixedHeader: NewFixedHeader(
			Unsuback,
			false, 0, false,
			2,
		),
		PacketID: id,
	}
}

func (self *UnsubackMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2)
	binary.BigEndian.PutUint16(wire, self.PacketID)

	return
}

func ParseUnsubackMessage(wire []byte) (m *UnsubackMessage) {
	m.PacketID = binary.BigEndian.Uint16(wire[:2])

	return
}

type PingreqMessage struct {
	*FixedHeader
}

func NewPingreqMessage() *PingreqMessage {
	return &PingreqMessage{
		FixedHeader: NewFixedHeader(
			Pingreq,
			false, 0, false,
			0,
		),
	}
}

func (self *PingreqMessage) GetWire() (wire []byte) {
	return
}

func ParsePingreqMessage(wire []byte) (m *PingreqMessage) {
	return
}

type PingrespMessage struct {
	*FixedHeader
}

func NewPingrespMessage() *PingrespMessage {
	return &PingrespMessage{
		FixedHeader: NewFixedHeader(
			Pingresp,
			false, 0, false,
			0,
		),
	}
}

func (self *PingrespMessage) GetWire() (wire []byte) {
	return
}

func ParsePingrespMessage(wire []byte) (m *PingrespMessage) {
	return
}

type DisconnectMessage struct {
	*FixedHeader
}

func NewDisconnectMessage() *DisconnectMessage {
	return &DisconnectMessage{
		FixedHeader: NewFixedHeader(
			Disconnect,
			false, 0, false,
			0,
		),
	}
}

func (self *DisconnectMessage) GetWire() (wire []byte) {
	return
}

func ParseDisconnectMessage(wire []byte) (m *DisconnectMessage) {
	return
}
