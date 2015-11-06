package MQTTg

import (
	"net"
)

type Transport struct {
	conn     net.Conn
	isBroker bool
}

func (self *Transport) SendMessage(m Message) error {
	wire, err := m.GetWire()
	if err != nil {
		return err
	}
	// TODO: Send the wire
	return nil
}

func (self *Transport) ReadMessage() (Message, error) {
	wire := make([]byte, 65535) //TODO: should be optimized
	len, err := self.conn.Read(wire)
	if err != nil {
		return nil, err
	}
	m, err := ReadFrame(wire[:len])
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (self *Transport) ReadLoop() error {
	for {
		m, err := self.ReadMessage()
		if err != nil {
			return err
		}

		switch message := m.(type) {
		case *ConnectMessage:
		case *ConnackMessage:
		case *PublishMessage:
			if message.Dup == 0 {
				// first time delivery
			} else if message.Dup == 1 {
				// re-delivered
			}

			switch message.QoS {
			case 0:
			case 1:
			case 2:
			case 3:
				// error
				// close connection
			}
		case *PubackMessage:
		case *PubrecMessage:
		case *PubrelMessage:
		case *PubcompMessage:
		case *SubscribeMessage:
		case *SubackMessage:
		case *UnsubscribeMessage:
		case *UnsubackMessage:
		case *PingreqMessage:
		case *PingrespMessage:
		case *DisconnectMessage:
		}

	}
}
