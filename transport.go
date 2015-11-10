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

func (self *Transport) ReadMessageFrom() (Message, *net.UDPAddr, error) {
	wire := make([]byte, 65535) //TODO: should be optimized
	len, addr, err := self.conn.ReadFromUDP(wire)
	if err != nil {
		return nil, addr, err
	}
	m, err := ReadFrame(wire[:len])
	if err != nil {
		return nil, addr, err
	}

	return m, addr, nil
}
