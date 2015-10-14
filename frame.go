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

type Message interface {
	Parse(data []byte)
	GetWire() ([]byte, error)
	String() string
}

type Connect struct {
}

func NewConnect() {

}
