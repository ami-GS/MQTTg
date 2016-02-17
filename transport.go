package MQTTg

import (
	"fmt"
	"net"
)

type Transport struct {
	conn *net.TCPConn
}

func NewTransport(addPair string) (*Transport, error) {
	rAddr, err := net.ResolveTCPAddr("tcp4", addPair)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp4", nil, rAddr)
	if err != nil {
		return nil, err
	}
	return &Transport{conn}, nil
}

func (self *Transport) SendMessage(m Message) error {
	wire := m.GetWire()
	_, err := self.conn.Write(wire)
	if err != nil {
		return err
	}
	if FrameDebug {
		fmt.Println(ClSend.Apply("Send")+":"+self.conn.LocalAddr().String()+" ---> "+self.conn.RemoteAddr().String(), m.String())
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

	if FrameDebug {
		fmt.Println(ClRecv.Apply("Recv")+":"+self.conn.LocalAddr().String()+" <--- "+self.conn.RemoteAddr().String(), m.String())
	}

	return m, nil
}
