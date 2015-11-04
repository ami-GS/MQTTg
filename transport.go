package MQTTg

import (
	"net"
)

type Transport struct {
	conn net.Conn
}

func (self *Transport) ReadFrame() error {
	return nil
}
