package MQTTg

import (
	"encoding/binary"
	"fmt"
	"io"
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
	return []string{
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
	}[int(self)]
}

func ReadFrame(r io.Reader) (Message, error) {
	// TODO: The argument should be considered (like io.Reader)
	fh, _, err := ParseFixedHeader(r) // This causes error
	if err != nil {
		return nil, err
	}
	ms, err := ParseMessage[fh.Type](fh, r)
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

func (self *FixedHeader) Write(w io.Writer) {
	flags := uint8(self.Type) << 4
	if self.Dup {
		flags |= 0x08
	}
	flags |= (self.QoS << 1)
	if self.Retain {
		flags |= 0x01
	}
	binary.Write(w, binary.BigEndian, &flags)
	RemainEncode(w, self.RemainLength)
}

func (self *FixedHeader) String() string {
	return fmt.Sprintf("[%s]\nDupulicate=%t, QoS=%d, Retain=%t, Remain Length=%d",
		ClFrames[self.Type].Apply(self.Type.String()), self.Dup, self.QoS, self.Retain, self.RemainLength)
}

func ParseFixedHeader(r io.Reader) (*FixedHeader, int, error) {
	fh := &FixedHeader{}
	var tmp byte
	err := binary.Read(r, binary.BigEndian, &tmp)
	if err != nil {
		// EOF only
		return nil, 0, err
	}
	fh.Type = MessageType(tmp >> 4)
	fh.Dup = tmp&0x08 == 0x08
	fh.QoS = byte((tmp >> 1) & 0x03)
	fh.Retain = tmp&0x01 == 0x01
	switch fh.Type {
	case Pubrel, Subscribe, Unsubscribe:
		if fh.Dup || fh.Retain || fh.QoS != 1 {
			return nil, 0, MALFORMED_FIXED_HEADER_RESERVED_BIT
		}
	case Publish:
		if fh.QoS == 3 {
			return nil, 0, INVALID_QOS_3
		}
	default:
		if fh.Dup || fh.Retain || fh.QoS != 0 {
			return nil, 0, MALFORMED_FIXED_HEADER_RESERVED_BIT
		}
	}

	var length int
	length, err = RemainDecode(r, &fh.RemainLength)
	if err != nil {
		return nil, 0, err
	}

	return fh, length + 1, nil
}

type FrameParser func(fh *FixedHeader, r io.Reader) (Message, error)

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
	Write(w io.Writer)
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

func (self *ConnectMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w)
	_ = UTF8_encode(w, self.Protocol.Name)
	binary.Write(w, binary.BigEndian, byte(self.Protocol.Level))
	binary.Write(w, binary.BigEndian, byte(self.Flags))
	binary.Write(w, binary.BigEndian, &self.KeepAlive)
	_ = UTF8_encode(w, self.ClientID)

	if self.Flags&Will_Flag == Will_Flag {
		_ = UTF8_encode(w, self.Will.Topic)
		_ = UTF8_encode(w, self.Will.Message)
	}
	if self.Flags&UserName_Flag == UserName_Flag {
		_ = UTF8_encode(w, self.User.Name)
	}
	if self.Flags&Password_Flag == Password_Flag {
		_ = UTF8_encode(w, self.User.Passwd)
	}
}

func ParseConnectMessage(fh *FixedHeader, r io.Reader) (Message, error) {
	m := &ConnectMessage{
		FixedHeader: fh,
		Protocol:    &Protocol{},
	}
	_ = UTF8_decode(r, &m.Protocol.Name)
	// TODO: validate protocol version
	binary.Read(r, binary.BigEndian, &m.Protocol.Level)
	var tmp_f uint8
	binary.Read(r, binary.BigEndian, &tmp_f)
	m.Flags = ConnectFlag(tmp_f)
	if m.Flags&Reserved_Flag == Reserved_Flag {
		return nil, MALFORMED_CONNECT_FLAG_BIT
	}
	if m.Flags&UserName_Flag != UserName_Flag && m.Flags&Password_Flag == Password_Flag {
		return nil, USERNAME_DOES_NOT_EXIST_WITH_PASSWORD
	}
	binary.Read(r, binary.BigEndian, &m.KeepAlive)

	_ = UTF8_decode(r, &m.ClientID)
	if m.Flags&Will_Flag == Will_Flag {
		m.Will = NewWill("", "", false, 0)
		_ = UTF8_decode(r, &m.Will.Topic)
		_ = UTF8_decode(r, &m.Will.Message)
		m.Will.Retain = m.Flags&WillRetain_Flag == WillRetain_Flag
		m.Will.QoS = uint8(m.Flags&WillQoS_3_Flag) >> 3
	}

	if m.Flags&UserName_Flag == UserName_Flag || m.Flags&Password_Flag == Password_Flag {
		m.User = NewUser("", "")
		if m.Flags&UserName_Flag == UserName_Flag {
			_ = UTF8_decode(r, &m.User.Name)
		}
		if m.Flags&Password_Flag == Password_Flag {
			_ = UTF8_decode(r, &m.User.Passwd)
		}
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
	return []string{
		"Accepted",
		"UnacceptableProtocolVersion",
		"IdentifierRejected",
		"ServerUnavailable",
		"BadUserNameOrPassword",
		"NotAuthorized",
	}[int(self)]
}

func (self ConnectReturnCode) Error() string {
	return self.String()
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

func (self *ConnackMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w)
	var sPresentFlag byte = 0
	if self.SessionPresentFlag {
		sPresentFlag = 0x01
	}
	binary.Write(w, binary.BigEndian, &sPresentFlag)
	binary.Write(w, binary.BigEndian, byte(self.ReturnCode))
}

func (self *ConnackMessage) String() string {
	return fmt.Sprintf("%s\n\tSession presentation=%t, Return code=%s\n",
		self.FixedHeader.String(), self.SessionPresentFlag, self.ReturnCode.String())
}

func (self *ConnackMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParseConnackMessage(fh *FixedHeader, r io.Reader) (Message, error) {
	m := &ConnackMessage{
		FixedHeader: fh,
	}
	tmp := make([]byte, 2)
	r.Read(tmp)
	m.SessionPresentFlag = (tmp[0] == 1)
	m.ReturnCode = ConnectReturnCode(tmp[1])
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

func (self *PublishMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w)
	_ = UTF8_encode(w, self.TopicName)
	if self.QoS > 0 {
		binary.Write(w, binary.BigEndian, &self.PacketID)
	}
	w.Write(self.Payload)
}

func (self *PublishMessage) String() string {
	return fmt.Sprintf("%s\n\tTopic=%s, PacketID=%d, Data=%s\n", self.FixedHeader.String(), self.TopicName, self.PacketID, string(self.Payload))
}

func (self *PublishMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePublishMessage(fh *FixedHeader, r io.Reader) (Message, error) {
	m := &PublishMessage{
		FixedHeader: fh,
	}
	len := UTF8_decode(r, &m.TopicName)
	if strings.Contains(m.TopicName, "#") || strings.Contains(m.TopicName, "+") {
		return nil, WILDCARD_CHARACTERS_IN_PUBLISH
	}
	if fh.QoS > 0 {
		binary.Read(r, binary.BigEndian, &m.PacketID)
		len += 2
	}
	m.Payload = make([]byte, fh.RemainLength-uint32(len))
	r.Read(m.Payload)

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

func (self *PubackMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w)
	binary.Write(w, binary.BigEndian, &self.PacketID)
}

func (self *PubackMessage) String() string {
	return fmt.Sprintf("%s\n\tPacketID=%d\n", self.FixedHeader.String(), self.PacketID)
}

func (self *PubackMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePubackMessage(fh *FixedHeader, r io.Reader) (Message, error) {
	m := &PubackMessage{
		FixedHeader: fh,
	}
	binary.Read(r, binary.BigEndian, &m.PacketID)

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

func (self *PubrecMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w)
	binary.Write(w, binary.BigEndian, &self.PacketID)

}

func (self *PubrecMessage) String() string {
	return fmt.Sprintf("%s\n\tPacketID=%d\n", self.FixedHeader.String(), self.PacketID)
}

func (self *PubrecMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePubrecMessage(fh *FixedHeader, r io.Reader) (Message, error) {
	m := &PubrecMessage{
		FixedHeader: fh,
	}
	binary.Read(r, binary.BigEndian, &m.PacketID)

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

func (self *PubrelMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w)
	binary.Write(w, binary.BigEndian, &self.PacketID)
}

func (self *PubrelMessage) String() string {
	return fmt.Sprintf("%s\n\tPacketID=%d\n", self.FixedHeader.String(), self.PacketID)
}

func (self *PubrelMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePubrelMessage(fh *FixedHeader, r io.Reader) (Message, error) {
	m := &PubrelMessage{
		FixedHeader: fh,
	}
	binary.Read(r, binary.BigEndian, &m.PacketID)

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

func (self *PubcompMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w)
	binary.Write(w, binary.BigEndian, &self.PacketID)
}

func (self *PubcompMessage) String() string {
	return fmt.Sprintf("%s\n\tPacketID=%d\n", self.FixedHeader.String(), self.PacketID)
}

func (self *PubcompMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePubcompMessage(fh *FixedHeader, r io.Reader) (Message, error) {
	m := &PubcompMessage{
		FixedHeader: fh,
	}
	binary.Read(r, binary.BigEndian, &m.PacketID)

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

func (self *SubscribeMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w)
	binary.Write(w, binary.BigEndian, &self.PacketID)

	for _, v := range self.SubscribeTopics {
		_ = UTF8_encode(w, v.Topic)
		binary.Write(w, binary.BigEndian, &v.QoS)
	}
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

func ParseSubscribeMessage(fh *FixedHeader, r io.Reader) (Message, error) {
	m := &SubscribeMessage{
		FixedHeader: fh,
	}
	err := binary.Read(r, binary.BigEndian, &m.PacketID)
	if err == io.EOF {
		return nil, PROTOCOL_VIOLATION
	}
	for i := 2; uint32(i) < fh.RemainLength; {
		subTopic := NewSubscribeTopic("", 0)
		length := UTF8_decode(r, &subTopic.Topic)
		var tmp byte
		binary.Read(r, binary.BigEndian, &tmp)
		if tmp == 3 {
			return nil, INVALID_QOS_3
		} else if tmp > 3 {
			return nil, MALFORMED_SUBSCRIBE_RESERVED_PART
		}
		subTopic.QoS = tmp & 0x03
		m.SubscribeTopics = append(m.SubscribeTopics, subTopic)
		i += int(length) + 1
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
	return map[SubscribeReturnCode]string{
		0x00: "AckMaxQoS0",
		0x01: "AckMaxQoS1",
		0x02: "AckMaxQoS2",
		0x80: "SubscribeFailure",
	}[self]
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

func (self *SubackMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w)
	binary.Write(w, binary.BigEndian, &self.PacketID)

	for _, v := range self.ReturnCodes {
		binary.Write(w, binary.BigEndian, byte(v))
	}
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

func ParseSubackMessage(fh *FixedHeader, r io.Reader) (Message, error) {
	m := &SubackMessage{
		FixedHeader: fh,
	}
	binary.Read(r, binary.BigEndian, &m.PacketID)
	var tmp byte
	for i := uint32(2); i < fh.RemainLength; i++ {
		binary.Read(r, binary.BigEndian, &tmp)
		m.ReturnCodes = append(m.ReturnCodes, SubscribeReturnCode(tmp))
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

func (self *UnsubscribeMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w)
	binary.Write(w, binary.BigEndian, &self.PacketID)

	for _, v := range self.TopicNames {
		_ = UTF8_encode(w, v)
	}
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

func ParseUnsubscribeMessage(fh *FixedHeader, r io.Reader) (Message, error) {
	m := &UnsubscribeMessage{
		FixedHeader: fh,
	}
	err := binary.Read(r, binary.BigEndian, &m.PacketID)
	if err == io.EOF {
		return nil, PROTOCOL_VIOLATION
	}
	var topicName string
	for i := uint32(2); i < fh.RemainLength; {
		len := UTF8_decode(r, &topicName)
		m.TopicNames = append(m.TopicNames, topicName)
		i += uint32(len)
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

func (self *UnsubackMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w)
	binary.Write(w, binary.BigEndian, &self.PacketID)
}

func (self *UnsubackMessage) String() string {
	return fmt.Sprintf("%s\n\tPacketID=%d\n", self.FixedHeader.String(), self.PacketID)
}

func (self *UnsubackMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParseUnsubackMessage(fh *FixedHeader, r io.Reader) (Message, error) {
	m := &UnsubackMessage{
		FixedHeader: fh,
	}
	binary.Read(r, binary.BigEndian, &m.PacketID)

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

func (self *PingreqMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w) // CHECK: Is this correct?
}

func (self *PingreqMessage) String() string {
	// TODO: need to put time?
	return fmt.Sprintf("%s\n", self.FixedHeader.String())
}

func (self *PingreqMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePingreqMessage(fh *FixedHeader, r io.Reader) (Message, error) {
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

func (self *PingrespMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w) // CHECK: Is this correct?
}

func (self *PingrespMessage) String() string {
	// TODO: need to put time?
	return fmt.Sprintf("%s\n", self.FixedHeader.String())
}

func (self *PingrespMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParsePingrespMessage(fh *FixedHeader, r io.Reader) (Message, error) {
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

func (self *DisconnectMessage) Write(w io.Writer) {
	self.FixedHeader.Write(w)
}

func (self *DisconnectMessage) String() string {
	// TODO: need to put disconnect message?
	return fmt.Sprintf("%s\n", self.FixedHeader.String())
}

func (self *DisconnectMessage) GetPacketID() uint16 {
	return self.PacketID
}

func ParseDisconnectMessage(fh *FixedHeader, r io.Reader) (Message, error) {
	m := &DisconnectMessage{
		FixedHeader: fh,
	}
	return m, nil
}
