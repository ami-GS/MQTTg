package MQTTg

import (
	"fmt"
	"net"
)

type Transport struct {
	conn *net.TCPConn
}

func NewTransport() (*Transport) {
	// TODO: do some certification, authentication
	return &Transport{}
}

func (self *Transport) Connect(url string) error {
	rAddr, err := net.ResolveTCPAddr("tcp4", url)
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp4", nil, rAddr)
	if err != nil {
		return err
	}
	self.conn = conn
	return nil
}

func (self *Transport) SendMessage(m Message) error {
	m.Write(self.conn)
	if FrameDebug {
		fmt.Println(ClSend.Apply("Send")+":"+self.conn.LocalAddr().String()+" ---> "+self.conn.RemoteAddr().String(), m.String())
	}
	return nil
}

func (self *Transport) ReadMessage() (Message, error) {
	m, err := ReadFrame(self.conn)
	if err != nil {
		return nil, err
	}

	if FrameDebug {
		fmt.Println(ClRecv.Apply("Recv")+":"+self.conn.LocalAddr().String()+" <--- "+self.conn.RemoteAddr().String(), m.String())
	}

	return m, nil
}
