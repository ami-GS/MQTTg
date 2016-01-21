package MQTTg

import (
	"encoding/binary"
	"fmt"
	"strings"
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

func ReadFrame(wire []byte) (Message, error) {
	// TODO: The argument should be considered (like io.Reader)
	fh, fhLen, err := ParseFixedHeader(wire) // This causes error
	if err != nil {
		return nil, err
	}
	ms, err := ParseMessage[fh.Type](fh, wire[fhLen:fhLen+int(fh.RemainLength)])
	if err != nil {
		return nil, err
	}

	return ms, nil
}

type FixedHeader struct {
	Type         MessageType
	Dup          bool
	QoS          uint8
	Retain       bool
	RemainLength uint32
	PacketID     uint16 // for easy use
}

func NewFixedHeader(mType MessageType, dup bool, qos uint8, retain bool, length uint32, id uint16) *FixedHeader {
	return &FixedHeader{
		Type:         mType,
		Dup:          dup,
		QoS:          qos,
		Retain:       retain,
		RemainLength: length,
		PacketID:     id,
	}
}

func (self *FixedHeader) GetWire() (wire []byte) {
	remainByteLen := 0
	switch {
	case self.RemainLength <= 0x7f:
		remainByteLen = 1
	case self.RemainLength <= 0x3fff:
		remainByteLen = 2
	case self.RemainLength <= 0x1fffff:
		remainByteLen = 3
	case self.RemainLength <= 0x0fffffff:
		remainByteLen = 4
	}
	wire = make([]uint8, 1+remainByteLen)
	wire[0] = uint8(self.Type) << 4
	if self.Dup {
		wire[0] |= 0x08
	}
	wire[0] |= (self.QoS << 1)
	if self.Retain {
		wire[0] |= 0x01
	}
	_ = RemainEncode(wire[1:], self.RemainLength)

	return
}

func (self *FixedHeader) String() string {
	return fmt.Sprintf("[%s]\nDupulicate=%t, QoS=%d, Retain=%t, Remain Length=%d",
		self.Type.String(), self.Dup, self.QoS, self.Retain, self.RemainLength)
}

func ParseFixedHeader(wire []byte) (*FixedHeader, int, error) {
	mType := MessageType(wire[0] >> 4)
	dup := wire[0]&0x08 == 0x08
	qos := uint8((wire[0] >> 1) & 0x03)
	retain := wire[0]&0x01 == 0x01
	switch mType {
	case Pubrel, Subscribe, Unsubscribe:
		if dup || retain || qos != 1 {
			return nil, 0, MALFORMED_FIXED_HEADER_RESERVED_BIT
		}
	case Publish:
		if qos == 3 {
			return nil, 0, INVALID_QOS_3
		}
	default:
		if dup || retain || qos != 0 {
			return nil, 0, MALFORMED_FIXED_HEADER_RESERVED_BIT
		}
	}

	length, remainPartLen, err := RemainDecode(wire[1:])
	if err != nil {
		return nil, 0, err
	}
	h := NewFixedHeader(mType, dup, qos, retain, length, 0)

	return h, remainPartLen + 1, nil
}

type VariableHeader interface {
	VHeaderParse(data []byte)
	VHeaderWire() ([]byte, error)
	VHeaderString() string
}

type FrameParser func(fh *FixedHeader, wire []byte) (Message, error)

var ParseMessage = map[MessageType]FrameParser{
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
	GetWire() []byte
	String() string
	GetPacketID() uint16
}

type ConnectFlag uint8

const (
	Reserved_Flag     ConnectFlag = 0x01
	CleanSession_Flag ConnectFlag = 0x02
	Will_Flag         ConnectFlag = 0x04
	WillQoS_0_Flag    ConnectFlag = 0x00
	WillQoS_1_Flag    ConnectFlag = 0x08
	WillQoS_2_Flag    ConnectFlag = 0x10
	WillQoS_3_Flag    ConnectFlag = 0x18
	WillRetain_Flag   ConnectFlag = 0x20
	Password_Flag     ConnectFlag = 0x40
	UserName_Flag     ConnectFlag = 0x80
)

func (self ConnectFlag) String() (s string) {
	if self&CleanSession_Flag == CleanSession_Flag {
		s += "\tCleanSession\n"
	}
	if self&Will_Flag == Will_Flag {
		s += "\tWillFlag\n"
	}
	switch self & WillQoS_3_Flag {
	case WillQoS_0_Flag:
		s += "\tWillQoS_0\n"
	case WillQoS_1_Flag:
		s += "\tWillQoS_1\n"
	case WillQoS_2_Flag:
		s += "\tWillQoS_2\n"
	case WillQoS_3_Flag:
		s += "\tWillQoS_3\n"
	}
	if self&WillRetain_Flag == WillRetain_Flag {
		s += "\tWillRetain_Flag\n"
	}
	if self&Password_Flag == Password_Flag {
		s += "\tPassword\n"
	}
	if self&UserName_Flag == UserName_Flag {
		s += "\tUserName\n"
	}
	return s
}

type ConnectMessage struct {
	*FixedHeader
	Protocol  *Protocol
	Flags     ConnectFlag
	KeepAlive uint16
	ClientID  string
	Will      *Will
	User      *User
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

func (self *Will) String() string {
	return fmt.Sprintf("%s:%s, Retain=%t, QoS=%d", self.Topic, self.Message, self.Retain, self.QoS)
}

func NewConnectMessage(keepAlive uint16, clientID string, cleanSession bool, will *Will, user *User) *ConnectMessage {
	length := 6 + len(MQTT_3_1_1.Name) + 2 + len(clientID)
	// The way to deal with flags are inefficient
	flags := ConnectFlag(0)
	if cleanSession {
		flags |= CleanSession_Flag
	}
	if will != nil {
		length += 4 + len(will.Topic) + len(will.Message)
		flags |= Will_Flag | ConnectFlag(will.QoS<<3)
		if will.Retain {
			flags |= WillRetain_Flag
		}
	}
	if user != nil {
		// TODO : password encryption here
		length += 4 + len(user.Name) + len(user.Passwd)
		if len(user.Name) > 0 {
			flags |= UserName_Flag
		}
		if len(user.Passwd) > 0 {
			flags |= Password_Flag
		}
	}
	return &ConnectMessage{
		FixedHeader: NewFixedHeader(
			Connect,
			false, 0, false,
			uint32(length), 0,
		),
		Protocol:  MQTT_3_1_1,
		Flags:     flags,
		KeepAlive: keepAlive,
		ClientID:  clientID,
		Will:      will,
		User:      user,
	}
}

func (self *ConnectMessage) GetWire() []byte {
	fh_wire := self.FixedHeader.GetWire()
	length := 6 + len(self.Protocol.Name) + 2 + len(fh_wire)
	if len(self.ClientID) != 0 {
		length += len(self.ClientID)
	}
	if self.Flags&Will_Flag == Will_Flag {
		length += 4 + len(self.Will.Topic) + len(self.Will.Message)
	}
	if self.Flags&UserName_Flag == UserName_Flag {
		length += 2 + len(self.User.Name)
	}
	if self.Flags&Password_Flag == Password_Flag {
		length += 2 + len(self.User.Passwd)
	}

	wire := make([]uint8, length)
	copy(wire, fh_wire)
	cursor := len(fh_wire)
	cursor += UTF8_encode(wire[cursor:], self.Protocol.Name)

	wire[cursor] = self.Protocol.Level
	wire[cursor+1] = uint8(self.Flags)
	cursor += 2 // skip flag

	binary.BigEndian.PutUint16(wire[cursor:], self.KeepAlive)
	cursor += 2
	cursor += UTF8_encode(wire[cursor:], self.ClientID)

	if self.Flags&Will_Flag == Will_Flag {
		cursor += UTF8_encode(wire[cursor:], self.Will.Topic)
		cursor += UTF8_encode(wire[cursor:], self.Will.Message)
	}
	if self.Flags&UserName_Flag == UserName_Flag {
		cursor += UTF8_encode(wire[cursor:], self.User.Name)
	}
	if self.Flags&Password_Flag == Password_Flag {
		cursor += UTF8_encode(wire[cursor:], self.User.Passwd)
	}

	return wire
}

func ParseConnectMessage(fh *FixedHeader, wire []byte) (Message, error) {
	m := &ConnectMessage{
		FixedHeader: fh,
	}
	cursor, protoName := UTF8_decode(wire)
	level := wire[cursor]
	// TODO: validate protocol version
	m.Protocol = &Protocol{protoName, level}

	m.Flags = ConnectFlag(wire[cursor+1])
	if m.Flags&Reserved_Flag == Reserved_Flag {
		return nil, MALFORMED_CONNECT_FLAG_BIT
	}
	if m.Flags&UserName_Flag != UserName_Flag && m.Flags&Password_Flag == Password_Flag {
		return nil, USERNAME_DOES_NOT_EXIST_WITH_PASSWORD
	}
	cursor += 2
	m.KeepAlive = binary.BigEndian.Uint16(wire[cursor:])
	cursor += 2
	cTmp, clientID := UTF8_decode(wire[cursor:])
	m.ClientID = clientID
	cursor += cTmp

	if m.Flags&Will_Flag == Will_Flag {
		cTmp1, topic := UTF8_decode(wire[cursor:])
		cTmp, message := UTF8_decode(wire[cursor+cTmp1:])
		cursor += cTmp1 + cTmp
		retain := m.Flags&WillRetain_Flag == WillRetain_Flag
		qos := uint8(m.Flags&WillQoS_3_Flag) >> 3
		m.Will = NewWill(topic, message, retain, qos)
	}

	if m.Flags&UserName_Flag == UserName_Flag || m.Flags&Password_Flag == Password_Flag {
		var name, passwd string
		if m.Flags&UserName_Flag == UserName_Flag {
			cTmp, name = UTF8_decode(wire[cursor:])
			cursor += cTmp
		}
		if m.Flags&Password_Flag == Password_Flag {
			cTmp, passwd = UTF8_decode(wire[cursor:])
			cursor += cTmp
		}
		m.User = NewUser(name, passwd)
	}

	return m, nil
}

func (self *ConnectMessage) String() string {
	return fmt.Sprintf("%s\n\tProtocol=%s:%d, Flags=\n%s\t, KeepAlive=%d, ClientID=%s, Will=%s, UserInfo={NAME:%s, PASS:%s}\n",
		self.FixedHeader.String(), self.Protocol.Name, self.Protocol.Level, self.Flags.String(),
		self.KeepAlive, self.ClientID, self.Will.String(), self.User.Name, self.User.Passwd)
}

func (self *ConnectMessage) GetPacketID() uint16 {
	return self.PacketID
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
			2, 0,
		),
		SessionPresentFlag: flag,
		ReturnCode:         code,
	}
}

func (self *ConnackMessage) GetWire() []byte {
	fh_wire := self.FixedHeader.GetWire()
	wire := make([]byte, 4)
	copy(wire, fh_wire)
	if self.SessionPresentFlag {
		wire[2] = 0x01
	}
	wire[3] = byte(self.ReturnCode)

	return wire
}

func (self *ConnackMessage) String() string {
	return fmt.Sprintf("%s\n\tSession presentation=%t, Return code=%s\n",
		self.FixedHeader.String(), self.SessionPresentFlag, self.ReturnCode.String())
}

func (self *ConnackMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParseConnackMessage(fh *FixedHeader, wire []byte) (Message, error) {
	m := &ConnackMessage{
		FixedHeader: fh,
	}
	if wire[0] == 1 {
		m.SessionPresentFlag = true
	}
	m.ReturnCode = ConnectReturnCode(wire[1])
	return m, nil
}

type PublishMessage struct {
	*FixedHeader
	TopicName string
	Payload   []uint8
}

func NewPublishMessage(dup bool, qos uint8, retain bool, topic string, id uint16, payload []uint8) *PublishMessage {
	QoSexistence := 0
	if qos > 0 {
		QoSexistence = 2
	} else if id != 0 {
		id = 0
	}

	length := 2 + QoSexistence + len(topic) + len(payload)
	return &PublishMessage{
		FixedHeader: NewFixedHeader(
			Publish,
			dup, qos, retain,
			uint32(length), id,
		),
		TopicName: topic,
		Payload:   payload,
	}
}

func (self *PublishMessage) GetWire() []byte {
	fh_wire := self.FixedHeader.GetWire()
	QoSexistence := 0
	if self.QoS > 0 {
		QoSexistence = 2
	}
	topicLen := len(self.TopicName)
	wire := make([]byte, 2+QoSexistence+topicLen+len(self.Payload))
	UTF8_encode(wire, self.TopicName)
	if self.QoS > 0 {
		binary.BigEndian.PutUint16(wire[2+topicLen:], self.PacketID)
	}
	copy(wire[2+QoSexistence+topicLen:], self.Payload)

	return append(fh_wire, wire...)
}

func (self *PublishMessage) String() string {
	return fmt.Sprintf("%s\n\tTopic=%s, PacketID=%d, Data=%s\n", self.FixedHeader.String(), self.TopicName, self.PacketID, string(self.Payload))
}

func (self *PublishMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePublishMessage(fh *FixedHeader, wire []byte) (Message, error) {
	m := &PublishMessage{
		FixedHeader: fh,
	}
	cursor, topicName := UTF8_decode(wire)
	if strings.Contains(topicName, "#") || strings.Contains(topicName, "+") {
		return nil, WILDCARD_CHARACTERS_IN_PUBLISH
	}
	m.TopicName = topicName
	if fh.QoS > 0 {
		m.PacketID = binary.BigEndian.Uint16(wire[cursor:])
		cursor += 2
	}
	m.Payload = wire[cursor:]

	return m, nil
}

type PubackMessage struct {
	*FixedHeader
}

func NewPubackMessage(id uint16) *PubackMessage {
	return &PubackMessage{
		FixedHeader: NewFixedHeader(
			Puback,
			false, 0, false,
			2, id,
		),
	}
}

func (self *PubackMessage) GetWire() []byte {
	fh_wire := self.FixedHeader.GetWire()
	wire := make([]byte, 4)
	copy(wire, fh_wire)
	binary.BigEndian.PutUint16(wire[2:], self.PacketID)

	return wire
}

func (self *PubackMessage) String() string {
	return fmt.Sprintf("%s\n\tPacketID=%d\n", self.FixedHeader.String(), self.PacketID)
}

func (self *PubackMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePubackMessage(fh *FixedHeader, wire []byte) (Message, error) {
	m := &PubackMessage{
		FixedHeader: fh,
	}
	m.PacketID = binary.BigEndian.Uint16(wire[:2])

	return m, nil
}

type PubrecMessage struct {
	*FixedHeader
}

func NewPubrecMessage(id uint16) *PubrecMessage {
	return &PubrecMessage{
		FixedHeader: NewFixedHeader(
			Pubrec,
			false, 0, false,
			2, id,
		),
	}
}

func (self *PubrecMessage) GetWire() []byte {
	fh_wire := self.FixedHeader.GetWire()
	wire := make([]byte, 4)
	copy(wire, fh_wire)
	binary.BigEndian.PutUint16(wire[2:], self.PacketID)

	return wire
}

func (self *PubrecMessage) String() string {
	return fmt.Sprintf("%s\n\tPacketID=%d\n", self.FixedHeader.String(), self.PacketID)
}

func (self *PubrecMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePubrecMessage(fh *FixedHeader, wire []byte) (Message, error) {
	m := &PubrecMessage{
		FixedHeader: fh,
	}
	m.PacketID = binary.BigEndian.Uint16(wire[:2])

	return m, nil
}

type PubrelMessage struct {
	*FixedHeader
}

func NewPubrelMessage(id uint16) *PubrelMessage {
	return &PubrelMessage{
		FixedHeader: NewFixedHeader(
			Pubrel,
			false, 1, false,
			2, id,
		),
	}
}

func (self *PubrelMessage) GetWire() []byte {
	fh_wire := self.FixedHeader.GetWire()
	wire := make([]byte, 4)
	copy(wire, fh_wire)
	binary.BigEndian.PutUint16(wire[2:], self.PacketID)

	return wire
}

func (self *PubrelMessage) String() string {
	return fmt.Sprintf("%s\n\tPacketID=%d\n", self.FixedHeader.String(), self.PacketID)
}

func (self *PubrelMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePubrelMessage(fh *FixedHeader, wire []byte) (Message, error) {
	m := &PubrelMessage{
		FixedHeader: fh,
	}
	m.PacketID = binary.BigEndian.Uint16(wire[:2])

	return m, nil
}

type PubcompMessage struct {
	*FixedHeader
}

func NewPubcompMessage(id uint16) *PubcompMessage {
	return &PubcompMessage{
		FixedHeader: NewFixedHeader(
			Pubcomp,
			false, 0, false,
			2, id,
		),
	}
}

func (self *PubcompMessage) GetWire() []byte {
	fh_wire := self.FixedHeader.GetWire()
	wire := make([]byte, 4)
	copy(wire, fh_wire)
	binary.BigEndian.PutUint16(wire[2:], self.PacketID)

	return wire
}

func (self *PubcompMessage) String() string {
	return fmt.Sprintf("%s\n\tPacketID=%d\n", self.FixedHeader.String(), self.PacketID)
}

func (self *PubcompMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePubcompMessage(fh *FixedHeader, wire []byte) (Message, error) {
	m := &PubcompMessage{
		FixedHeader: fh,
	}
	m.PacketID = binary.BigEndian.Uint16(wire[:2])

	return m, nil
}

type SubscribeState uint8

const (
	SubscribeNonAck SubscribeState = iota
	SubscribeAck
	UnSubscribeNonAck
	UnSubscribeAck
)

type SubscribeTopic struct {
	State SubscribeState
	Topic string
	QoS   uint8
}

func NewSubscribeTopic(topic string, qos uint8) *SubscribeTopic {
	return &SubscribeTopic{
		State: SubscribeNonAck,
		Topic: topic,
		QoS:   qos,
	}
}

type SubscribeMessage struct {
	*FixedHeader
	SubscribeTopics []*SubscribeTopic
}

func NewSubscribeMessage(id uint16, topics []*SubscribeTopic) *SubscribeMessage {
	length := 2 + 3*len(topics)
	for _, v := range topics {
		length += len(v.Topic)
	}

	return &SubscribeMessage{
		FixedHeader: NewFixedHeader(
			Subscribe,
			false, 1, false,
			uint32(length), id,
		),
		SubscribeTopics: topics,
	}
}

func (self *SubscribeMessage) GetWire() []byte {
	fh_wire := self.FixedHeader.GetWire()
	topicsLen := 0
	for _, v := range self.SubscribeTopics {
		topicsLen += len(v.Topic)
	}
	wire := make([]byte, len(fh_wire)+2+3*len(self.SubscribeTopics)+topicsLen)
	copy(wire, fh_wire)
	cursor := len(fh_wire)
	binary.BigEndian.PutUint16(wire[cursor:], self.PacketID)
	cursor += 2
	for _, v := range self.SubscribeTopics {
		cursor += UTF8_encode(wire[cursor:], v.Topic)
		wire[cursor] = v.QoS
		cursor++
	}

	return wire
}

func (self *SubscribeMessage) String() string {
	topicStrings := ""
	for i, v := range self.SubscribeTopics {
		topicStrings += fmt.Sprintf("\t%d: Topic=%s, QoS=%d\n", i, v.Topic, v.QoS)
	}
	return fmt.Sprintf("%s\n\tPacketID=%d\n%s",
		self.FixedHeader.String(), self.PacketID, topicStrings)
}

func (self *SubscribeMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParseSubscribeMessage(fh *FixedHeader, wire []byte) (Message, error) {
	if len(wire) == 0 {
		return nil, PROTOCOL_VIOLATION
	}
	m := &SubscribeMessage{
		FixedHeader: fh,
	}
	m.PacketID = binary.BigEndian.Uint16(wire[:2])
	for i := 2; uint32(i) < fh.RemainLength; {
		length, topic := UTF8_decode(wire[i:])
		if wire[i+length] == 3 {
			return nil, INVALID_QOS_3
		} else if wire[i+length] > 3 {
			return nil, MALFORMED_SUBSCRIBE_RESERVED_PART
		}
		qos := wire[i+length] & 0x03
		m.SubscribeTopics = append(m.SubscribeTopics,
			NewSubscribeTopic(topic, qos))
		i += length + 1
	}

	return m, nil
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
	ReturnCodes []SubscribeReturnCode
}

func NewSubackMessage(id uint16, codes []SubscribeReturnCode) *SubackMessage {
	length := 2 + len(codes)
	return &SubackMessage{
		FixedHeader: NewFixedHeader(
			Suback,
			false, 0, false,
			uint32(length), id,
		),
		ReturnCodes: codes,
	}
}

func (self *SubackMessage) GetWire() []byte {
	fh_wire := self.FixedHeader.GetWire()
	wire := make([]byte, len(fh_wire)+2+len(self.ReturnCodes))
	copy(wire, fh_wire)
	cursor := len(fh_wire)
	binary.BigEndian.PutUint16(wire[cursor:], self.PacketID)
	cursor += 2
	for i, v := range self.ReturnCodes {
		wire[cursor+i] = byte(v)
	}
	return wire
}

func (self *SubackMessage) String() string {
	codes := ""
	for i, v := range self.ReturnCodes {
		codes += fmt.Sprintf("\t%d: %s\n", i, v.String())
	}
	return fmt.Sprintf("%s\n\tPacketID=%d\n%s", self.FixedHeader.String(), self.PacketID, codes)
}

func (self *SubackMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParseSubackMessage(fh *FixedHeader, wire []byte) (Message, error) {
	m := &SubackMessage{
		FixedHeader: fh,
	}
	m.PacketID = binary.BigEndian.Uint16(wire[:2])
	for _, v := range wire[2:] {
		m.ReturnCodes = append(m.ReturnCodes, SubscribeReturnCode(v))
	}

	return m, nil
}

type UnsubscribeMessage struct {
	*FixedHeader
	TopicNames []string
}

func NewUnsubscribeMessage(id uint16, topics []string) *UnsubscribeMessage {
	length := 2 + 2*len(topics)
	for _, v := range topics {
		length += len(v)
	}
	return &UnsubscribeMessage{
		FixedHeader: NewFixedHeader(
			Unsubscribe,
			false, 1, false,
			uint32(length), id,
		),
		TopicNames: topics,
	}
}

func (self *UnsubscribeMessage) GetWire() []byte {
	fh_wire := self.FixedHeader.GetWire()
	allLen := 0
	for _, v := range self.TopicNames {
		allLen += len(v)
	}
	cursor := len(fh_wire)
	wire := make([]byte, cursor+2+2*len(self.TopicNames)+allLen)
	copy(wire, fh_wire)
	binary.BigEndian.PutUint16(wire[cursor:], self.PacketID)
	cursor += 2

	for _, v := range self.TopicNames {
		cursor += UTF8_encode(wire[cursor:], v)
	}

	return wire
}

func (self *UnsubscribeMessage) String() string {
	topics := ""
	for i, v := range self.TopicNames {
		topics += fmt.Sprintf("\t%d: %s\n", i, v)
	}
	return fmt.Sprintf("%s\n\tPacketID=%d\n%s", self.FixedHeader.String(), self.PacketID, topics)
}

func (self *UnsubscribeMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParseUnsubscribeMessage(fh *FixedHeader, wire []byte) (Message, error) {
	if len(wire) == 0 {
		return nil, PROTOCOL_VIOLATION
	}
	m := &UnsubscribeMessage{
		FixedHeader: fh,
	}
	m.PacketID = binary.BigEndian.Uint16(wire[:2])
	allLen := len(wire)
	for i := 2; i < allLen; {
		topicLen := int(binary.BigEndian.Uint16(wire[i : i+2]))
		m.TopicNames = append(m.TopicNames, string(wire[i+2:i+2+topicLen]))
		i += 2 + topicLen
	}
	return m, nil
}

type UnsubackMessage struct {
	*FixedHeader
}

func NewUnsubackMessage(id uint16) *UnsubackMessage {
	return &UnsubackMessage{
		FixedHeader: NewFixedHeader(
			Unsuback,
			false, 0, false,
			2, id,
		),
	}
}

func (self *UnsubackMessage) GetWire() []byte {
	fh_wire := self.FixedHeader.GetWire()
	wire := make([]byte, 4)
	copy(wire, fh_wire)
	binary.BigEndian.PutUint16(wire[2:], self.PacketID)

	return wire
}

func (self *UnsubackMessage) String() string {
	return fmt.Sprintf("%s\n\tPacketID=%d\n", self.FixedHeader.String(), self.PacketID)
}

func (self *UnsubackMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParseUnsubackMessage(fh *FixedHeader, wire []byte) (Message, error) {
	m := &UnsubackMessage{
		FixedHeader: fh,
	}
	m.PacketID = binary.BigEndian.Uint16(wire[:2])

	return m, nil
}

type PingreqMessage struct {
	*FixedHeader
}

func NewPingreqMessage() *PingreqMessage {
	return &PingreqMessage{
		FixedHeader: NewFixedHeader(
			Pingreq,
			false, 0, false,
			0, 0,
		),
	}
}

func (self *PingreqMessage) GetWire() []byte {
	wire := self.FixedHeader.GetWire()
	return wire // CHECK: Is this correct?
}

func (self *PingreqMessage) String() string {
	// TODO: need to put time?
	return fmt.Sprintf("%s\n", self.FixedHeader.String())
}

func (self *PingreqMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePingreqMessage(fh *FixedHeader, wire []byte) (Message, error) {
	m := &PingreqMessage{
		FixedHeader: fh,
	}
	return m, nil
}

type PingrespMessage struct {
	*FixedHeader
}

func NewPingrespMessage() *PingrespMessage {
	return &PingrespMessage{
		FixedHeader: NewFixedHeader(
			Pingresp,
			false, 0, false,
			0, 0,
		),
	}
}

func (self *PingrespMessage) GetWire() []byte {
	wire := self.FixedHeader.GetWire()
	return wire // CHECK: Is this correct?
}

func (self *PingrespMessage) String() string {
	// TODO: need to put time?
	return fmt.Sprintf("%s\n", self.FixedHeader.String())
}

func (self *PingrespMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePingrespMessage(fh *FixedHeader, wire []byte) (Message, error) {
	m := &PingrespMessage{
		FixedHeader: fh,
	}
	return m, nil
}

type DisconnectMessage struct {
	*FixedHeader
}

func NewDisconnectMessage() *DisconnectMessage {
	return &DisconnectMessage{
		FixedHeader: NewFixedHeader(
			Disconnect,
			false, 0, false,
			0, 0,
		),
	}
}

func (self *DisconnectMessage) GetWire() []byte {
	wire := self.FixedHeader.GetWire()
	return wire
}

func (self *DisconnectMessage) String() string {
	// TODO: need to put disconnect message?
	return fmt.Sprintf("%s\n", self.FixedHeader.String())
}

func (self *DisconnectMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParseDisconnectMessage(fh *FixedHeader, wire []byte) (Message, error) {
	m := &DisconnectMessage{
		FixedHeader: fh,
	}
	return m, nil
}
