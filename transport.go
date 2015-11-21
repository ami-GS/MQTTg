package MQTTg

import (
	"net"
)

type Transport struct {
	conn *net.UDPConn
	//sm   *Sender
}

func (self *Transport) SendMessage(m Message, distAddr *net.UDPAddr) error {
	wire, err := m.GetWire()
	if err != nil {
		return err
	}
	_, err = self.conn.WriteToUDP(wire, distAddr)
	return err
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
