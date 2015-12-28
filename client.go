package MQTTg

import (
	"net"
	"strconv"
	"strings"
	"time"
)

type User struct {
	Name   string
	Passwd string
}

func NewUser(name, pass string) *User {
	return &User{
		Name:   name,
		Passwd: pass,
	}
}

type Client struct {
	Ct           *Transport
	IsConnecting bool
	//Addr         *net.UDPAddr
	RemoteAddr   *net.UDPAddr
	ID           string
	User         *User
	KeepAlive    uint16
	Will         *Will
	SubTopics    []SubscribeTopic
	PingBegin    time.Time
	PacketIDMap  map[uint16]Message
	CleanSession bool
}

func NewClient(t *Transport, addr *net.UDPAddr, id string, user *User, keepAlive uint16, will *Will, cleanSession bool) *Client {
	// TODO: when id is empty, then apply random
	return &Client{
		Ct:           t,
		IsConnecting: false,
		RemoteAddr:   addr,
		ID:           id,
		User:         user,
		KeepAlive:    keepAlive,
		Will:         will,
		SubTopics:    make([]SubscribeTopic, 0),
		PacketIDMap:  make(map[uint16]Message, 0),
		CleanSession: cleanSession,
	}
}

func (self *Client) SendMessage(m Message) error {
	if !self.IsConnecting {
		return NOT_CONNECTED
	}
	err := self.Ct.SendMessage(m, self.RemoteAddr)
	if err == nil {
		switch mess := m.(type) {
		case *PublishMessage:
			if mess.PacketID > 0 {
				self.PacketIDMap[mess.PacketID] = m
			}
		case *PubrecMessage:
			self.PacketIDMap[mess.PacketID] = m
		case *PubrelMessage:
			self.PacketIDMap[mess.PacketID] = m
		case *SubscribeMessage:
			self.PacketIDMap[mess.PacketID] = m
		case *UnsubscribeMessage:
			self.PacketIDMap[mess.PacketID] = m
		}
	}

	return err
}

func (self *Client) AckSubscribeTopic(order int, code SubscribeReturnCode) error {
	if code != SubscribeFailure {
		self.SubTopics[order].QoS = uint8(code)
		self.SubTopics[order].State = SubscribeAck
	} else {
		//failed
	}
	return nil
}

func (self *Client) Connect(addPair string) error {
	pair := strings.Split(addPair, ":")
	if len(pair) != 2 {
		return nil // TODO: apply error
	}
	port, err := strconv.Atoi(pair[1])
	if err != nil {
		return err
	}
	udpaddr := &net.UDPAddr{
		IP:   net.IP(pair[0]),
		Port: port,
		Zone: "", // TODO: check
	}
	// TODO: local address might be input
	conn, err := net.DialUDP("udp4", nil, udpaddr)
	if err != nil {
		return err
	}
	self.RemoteAddr = udpaddr
	self.Ct.conn = conn
	err = self.SendMessage(NewConnectMessage(10, "dummyID",
		false, nil, NewUser("name", "pass")))
	return err
}

func (self *Client) Subscribe(topics []SubscribeTopic) error {
	// TODO: id should be considered
	err := self.SendMessage(NewSubscribeMessage(0, topics))
	if err == nil {
		self.SubTopics = append(self.SubTopics, topics...)
	}
	return err
}

func (self *Client) Unsubscribe(topics []string) error {
	for _, t := range self.SubTopics {
		exist := false
		for _, name := range topics {
			if string(t.Topic) == string(name) {
				exist = true
			}
		}
		if exist {
			t.State = UnSubscribeNonAck
		} else {
			// error? or warnning
			// return error
		}
	}
	// id should be conidered
	err := self.SendMessage(NewUnsubscribeMessage(0, topics))
	return err
}

func (self *Client) keepAlive() error {
	err := self.SendMessage(NewPingreqMessage())
	if err == nil {
		self.PingBegin = time.Now()
	}
	return err
}

func (self *Client) Disconnect() error {
	err := self.SendMessage(NewDisconnectMessage())
	if err != nil {
		return err
	}
	err = self.Ct.conn.Close()
	self.IsConnecting = false
	return err
}

func (self *Client) AckMessage(id uint16) error {
	_, ok := self.PacketIDMap[id]
	if !ok {
		return PACKET_ID_DOES_NOT_EXIST
	}
	delete(self.PacketIDMap, id)
	return nil
}

func (self *Client) Redelivery() (err error) {
	// TODO: Should the DUP flag be 1 ?
	if !self.CleanSession && len(self.PacketIDMap) > 0 {
		for _, v := range self.PacketIDMap {
			err = self.SendMessage(v)
		}
	}
	return err
}

func (self *Client) recvConnectMessage(m *ConnectMessage, addr *net.UDPAddr) (err error) {
	return INVALID_MESSAGE_CAME
}
func (self *Client) recvConnackMessage(m *ConnackMessage, addr *net.UDPAddr) (err error) {
	self.AckMessage(m.PacketID)
	self.IsConnecting = true
	self.keepAlive()
	self.Redelivery()
	return err
}
func (self *Client) recvPublishMessage(m *PublishMessage, addr *net.UDPAddr) (err error) {
	if m.Dup {
		// re-delivered
	} else if m.Dup {
		// first time delivery
	}

	if m.Retain {
		// retained message comes
	} else {
		// non retained message
	}

	switch m.QoS {
	// in any case, Dub must be 0
	case 0:
	case 1:
		err = self.SendMessage(NewPubackMessage(m.PacketID))
	case 2:
		err = self.SendMessage(NewPubrecMessage(m.PacketID))
	}
	return err
}

func (self *Client) recvPubackMessage(m *PubackMessage, addr *net.UDPAddr) (err error) {
	// acknowledge the sent Publish packet
	if m.PacketID > 0 {
		err = self.AckMessage(m.PacketID)
	}
	return err
}

func (self *Client) recvPubrecMessage(m *PubrecMessage, addr *net.UDPAddr) (err error) {
	// acknowledge the sent Publish packet
	err = self.AckMessage(m.PacketID)
	err = self.SendMessage(NewPubrelMessage(m.PacketID))
	return err
}

func (self *Client) recvPubrelMessage(m *PubrelMessage, addr *net.UDPAddr) (err error) {
	// acknowledge the sent Pubrel packet
	err = self.AckMessage(m.PacketID)
	err = self.SendMessage(NewPubcompMessage(m.PacketID))
	return err
}

func (self *Client) recvPubcompMessage(m *PubcompMessage, addr *net.UDPAddr) (err error) {
	// acknowledge the sent Pubrel packet
	err = self.AckMessage(m.PacketID)
	return err
}

func (self *Client) recvSubscribeMessage(m *SubscribeMessage, addr *net.UDPAddr) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *Client) recvSubackMessage(m *SubackMessage, addr *net.UDPAddr) (err error) {
	// acknowledge the sent subscribe packet
	self.AckMessage(m.PacketID)
	for i, code := range m.ReturnCodes {
		_ = self.AckSubscribeTopic(i, code)
	}
	return err
}
func (self *Client) recvUnsubscribeMessage(m *UnsubscribeMessage, addr *net.UDPAddr) (err error) {
	return INVALID_MESSAGE_CAME
}
func (self *Client) recvUnsubackMessage(m *UnsubackMessage, addr *net.UDPAddr) (err error) {
	// acknowledged the sent unsubscribe packet
	err = self.AckMessage(m.PacketID)
	return err
}

func (self *Client) recvPingreqMessage(m *PingreqMessage, addr *net.UDPAddr) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *Client) recvPingrespMessage(m *PingrespMessage, addr *net.UDPAddr) (err error) {
	elapsed := time.Since(self.PingBegin)
	self.Ct.duration = elapsed
	if elapsed.Seconds() >= float64(self.KeepAlive) {
		// TODO: this must be 'reasonable amount of time'
		err = self.SendMessage(NewDisconnectMessage())
	} else {
		self.keepAlive() // TODO: this make impossible to send ping manually
	}
	return err
}

func (self *Client) recvDisconnectMessage(m *DisconnectMessage, addr *net.UDPAddr) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *Client) ReadMessageFrom() (Message, *net.UDPAddr, error) {
	return self.Ct.ReadMessageFrom()
}
