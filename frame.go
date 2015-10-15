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
