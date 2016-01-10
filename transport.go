package MQTTg

import (
	"net"
)

type Transport struct {
	conn *net.TCPConn
}

func (self *Transport) SendMessage(m Message) error {
	wire := m.GetWire()
	_, err = self.conn.Write(wire)
	if err != nil {
		return err
	}
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
