package MQTTg

import (
	"net"
)

type Transport struct {
	conn net.Conn
}

func (self *Transport) SendMessage(m Message) error {
	wire, err := m.GetWire()
	if err != nil {
		return err
	}
	// TODO: Send the wire
	return nil
}
