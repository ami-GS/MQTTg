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

func NewConnectMessage(connectFlags ConnectFlag, keepAlive uint16) *ConnectMessage {
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
	protoLen := len(self.ProtoName)
	wire = make([]uint8, protoLen+6)
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

func (self *ConnackMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2)
	if self.SessionPresentFlag {
		wire[0] = 0x01
	}
	wire[1] = byte(self.ReturnCode)

	return
}

type PublishMessage struct {
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

func (self *PublishMessage) GetWire() (wire []byte) {
	topicLen := len(self.TopicName)
	wire = make([]byte, 4+topicLen+len(self.Payload))
	for i := 0; i < 2; i++ {
		wire[i] = byte(topicLen >> ((1 - i) * 8))
	}
	for i, v := range []byte(self.TopicName) {
		wire[2+i] = v
	}
	for i := 0; i < 2; i++ {
		wire[2+topicLen+i] = byte(self.PacketID >> ((1 - i) * 8))
	}
	for i, v := range self.Payload {
		wire[4+topicLen+i] = v
	}

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
			0, // TODO:check
		),
		PacketID: id,
	}
}

func (self *PubackMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2)
	for i := 0; i < 2; i++ {
		wire[i] = byte(self.PacketID >> ((1 - i) * 8))
	}

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
			0, // TODO:check
		),
		PacketID: id,
	}
}

func (self *PubrecMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2)
	for i := 0; i < 2; i++ {
		wire[i] = byte(self.PacketID >> ((1 - i) * 8))
	}

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
			0, // TODO:check
		),
		PacketID: id,
	}
}

func (self *PubrelMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2)
	for i := 0; i < 2; i++ {
		wire[i] = byte(self.PacketID >> ((1 - i) * 8))
	}

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
			0, // TODO:check
		),
		PacketID: id,
	}
}

func (self *PubcompMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2)
	for i := 0; i < 2; i++ {
		wire[i] = byte(self.PacketID >> ((1 - i) * 8))
	}

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

func (self *SubscribeMessage) GetWire() (wire []byte) {
	topicsLen := 0
	for _, v := range self.SubscribeTopics {
		topicsLen += len(v.Topic)
	}
	wire = make([]byte, 2+3*len(self.SubscribeTopics)+topicsLen)

	for i := 0; i < 2; i++ {
		wire[i] = byte(self.PacketID >> ((1 - i) * 8))
	}
	cursor := 2
	for _, v := range self.SubscribeTopics {
		topicLen := len(v.Topic)
		for j := 0; j < 2; j++ {
			wire[cursor+j] = byte(topicLen >> ((1 - j) * 8))
		}
		cursor += 2

		for j, b := range []byte(v.Topic) {
			wire[cursor+2+j] = b
		}
		cursor += topicLen
		wire[cursor] = v.QoS
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

func (self *SubackMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2+len(self.ReturnCodes))
	for i := 0; i < 2; i++ {
		wire[i] = byte(self.PacketID >> ((1 - i) * 8))
	}
	for i, v := range self.ReturnCodes {
		wire[2+i] = byte(v)
	}
	return
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

func (self *UnsubscribeMessage) GetWire() (wire []byte) {
	allLen := 0
	for _, v := range self.Topics {
		allLen += len(v)
	}
	wire = make([]byte, 2+2*len(self.Topics)+allLen)
	for i := 0; i < 2; i++ {
		wire[i] = byte(self.PacketID >> ((1 - i) * 8))
	}

	cursor := 2
	for _, v := range self.Topics {
		for j := 0; j < 2; j++ {
			wire[cursor+j] = byte(self.PacketID >> ((1 - j) * 8))
		}
		cursor += 2

		for j, b := range v {
			wire[cursor+j] = b
		}
		cursor += len(v)
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
			0, // TODO:check
		),
		PacketID: id,
	}
}

func (self *UnsubackMessage) GetWire() (wire []byte) {
	wire = make([]byte, 2)
	for i := 0; i < 2; i++ {
		wire[i] = byte(self.PacketID >> ((1 - i) * 8))
	}

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
			0, // TODO:check
		),
	}
}

func (self *PingreqMessage) GetWire() (wire []byte) {
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
			0, // TODO:check
		),
	}
}

func (self *PingrespMessage) GetWire() (wire []byte) {
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
			0, // TODO:check
		),
	}
}

func (self *DisconnectMessage) GetWire() (wire []byte) {
	return
}
