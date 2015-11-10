package MQTTg

import (
	"net"
)

type Transport struct {
	conn net.UDPConn
	//sm   *Sender
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
