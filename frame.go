package MQTTg

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
	wire = new([]uint8, 2)
	wire[0] = uint8(self.Type)
	if self.Dub {
		wire[0] |= 0x08
	}
	self.wire[0] |= (self.QoS << 1)
	if self.Retain {
		wire[0] |= 0x01
	}
	wire[1] = self.RemainLength

	return
}

type VariableHeader interface {
	VHeaderParse(data []byte)
	VHeaderWire() ([]byte, error)
	VHeaderString() string
}

type Message interface {
	Parse(data []byte)
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
	ProtoName    string
	ProtoLevel   uint8
	ConnectFlags ConnectFlag
	KeepAlive    uint16
}

func NewConnectMessage(connectFlags ConnectFlags, keepAlive uint16) *ConnectMessage {
	return &ConnectMessage{
		FixedHeader: NewFixedHeader(
			Connect,
			false, 0, false,
			0, // TODO:check
		),
		ProtoName:    "MQTT",
		ProtoLevel:   4,
		ConnectFlags: connectFlags,
		KeepAlive:    keepAlive,
	}
}

func (self *ConnectMessage) GetWire() (wire []uint8) {
	var protoLen uint16 = len(self.ProtoName)
	wire = new([]uint8, protoLen+6)
	for i := 0; i < 2; i++ {
		wire[i] = uint8(protoLen >> ((1 - i) * 8))
	}

	for i := 0; i < protoLen; i++ {
		wire[2+i] = uint8(self.ProtoName[i])
	}
	wire[2+protoLen] = self.ProtoLevel
	wire[3+protoLen] = uint8(self.ConnectFlags)
	for i := 0; i < 2; i++ {
		wire[4+i] = uint8(self.KeepAlive >> ((1 - i) * 8))
	}

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
			0, // TODO:check
		),
		SessionPresentFlag: flag,
		ReturnCode:         code,
	}
}

type PulishMessage struct {
	*FixedHeader
	TopicName string
	PacketID  uint16
	Payload   []uint8
}

func NewPublishMessage(dub bool, qos uint8, retain bool, topic string, id uint16, payload []uint8) *PublishMessage {
	return &PublishMessage{
		FixedHeader: NewFixedHeader(
			Publish,
			dub, qos, retain,
			0, // TODO:check
		),
		TopicName: topic,
		PacketID:  id,
		Payload:   payload,
	}
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
			0, // TODO:check
		),
		PacketID: id,
	}
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
			0, // TODO:check
		),
		PacketID: id,
	}
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
			0, // TODO:check
		),
		PacketID: id,
	}
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
			0, // TODO:check
		),
		PacketID: id,
	}
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
	return &SubscribeMessage{
		FixedHeader: NewFixedHeader(
			Subscribe,
			false, 0, false,
			0, // TODO:check
		),
		PacketID:        id,
		SubscribeTopics: topics,
	}
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
	return &SubackMessage{
		FixedHeader: NewFixedHeader(
			Suback,
			false, 0, false,
			0, // TODO:check
		),
		PacketID:    id,
		ReturnCodes: codes,
	}
}

type UnsubscribeMessage struct {
	*FixedHeader
	PacketID uint16
	Topics   [][]uint8
}

func NewUnsubscribeMessage(id uint16, topics [][]uint8) *UnsubscribeMessage {
	return &UnsubscribeMessage{
		FixedHeader: NewFixedHeader(
			Unsubscribe,
			false, 0, false,
			0, // TODO:check
		),
		PacketID: id,
		Topics:   topics,
	}
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
			0, // TODO:check
		),
		PacketID: id,
	}
}

type PingreqMessage struct {
	*FixedHeader
}

func NewPingreqMessage() *PingreqMessage {
	return &PingreqMessage{
		FixedHeader: NewFixedHeader(
			Pingreq,
			false, 0, false,
			0, // TODO:check
		),
	}
}

type PingrespMessage struct {
	*FixedHeader
}

func NewPingrespMessage() *PingrespMessage {
	return &PingrespMessage{
		FixedHeader: NewFixedHeader(
			Pingresp,
			false, 0, false,
			0, // TODO:check
		),
	}
}

type DisconnectMessage struct {
	*FixedHeader
}

func NewDisconnectMessage() *DisconnectMessage {
	return &DisconnectMessage{
		FixedHeader: NewFixedHeader(
			Disconnect,
			false, 0, false,
			0, // TODO:check
		),
	}
}
