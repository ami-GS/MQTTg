package MQTTg

import (
	"math/rand"
	"net"
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
	Ct             *Transport
	IsConnecting   bool
	ID             string
	User           *User
	KeepAlive      uint16
	Will           *Will
	SubTopics      []SubscribeTopic
	PingBegin      time.Time
	PacketIDMap    map[uint16]Message
	CleanSession   bool
	KeepAliveTimer *time.Timer
	Duration       time.Duration
}

func NewClient(id string, user *User, keepAlive uint16, will *Will) *Client {
	// TODO: when id is empty, then apply random
	return &Client{
		IsConnecting:   false,
		ID:             id,
		User:           user,
		KeepAlive:      keepAlive,
		Will:           will,
		SubTopics:      make([]SubscribeTopic, 0),
		PacketIDMap:    make(map[uint16]Message, 0),
		CleanSession:   false,
		KeepAliveTimer: nil,
		Duration:       0,
	}
}

func (self *Client) ResetTimer() {
	self.KeepAliveTimer.Reset(self.Duration)
}

func (self *Client) SendMessage(m Message) error {
	if !self.IsConnecting {
		return NOT_CONNECTED
	}
	id := m.GetPacketID()
	_, ok := self.PacketIDMap[id]
	if ok {
		return PACKET_ID_IS_USED_ALREADY
	}

	err := self.Ct.SendMessage(m)
	if err == nil {
		switch m.(type) {
		case *PublishMessage:
			if id > 0 {
				self.PacketIDMap[id] = m
			}
		case *PubrecMessage, *PubrelMessage, *SubscribeMessage, *UnsubscribeMessage:
			self.PacketIDMap[id] = m
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

func (self *Client) getUsablePacketID() (uint16, error) {
	ok := true
	var id uint16
	for trial := 0; ok; trial++ {
		if trial == 5 {
			return 0, FAIL_TO_SET_PACKET_ID
		}
		id = uint16(1 + rand.Int31n(65535))
		_, ok = self.PacketIDMap[id]
	}
	return id, nil
}

func (self *Client) Connect(addPair string, cleanSession bool) error {
	rAddr, err := net.ResolveTCPAddr("tcp4", addPair)
	if err != nil {
		return err
	}
	lAddr, err := GetLocalAddr()
	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp4", lAddr, rAddr)
	if err != nil {
		return err
	}
	self.Ct = &Transport{conn}
	self.CleanSession = cleanSession
	go ReadLoop(self)
	// below can avoid first IsConnecting validation
	err = self.Ct.SendMessage(NewConnectMessage(self.KeepAlive,
		self.ID, cleanSession, self.Will, self.User))
	return err
}

func (self *Client) Publish(topic, data string, qos uint8, retain bool) error {
	if qos >= 3 {
		return INVALID_QOS_3
	}
	if strings.Contains(topic, "#") || strings.Contains(topic, "+") {
		return WILDCARD_CHARACTERS_IN_PUBLISH
	}

	id, err := self.getUsablePacketID()
	if err != nil {
		return err
	}
	err = self.SendMessage(NewPublishMessage(false, qos, retain,
		topic, id, []uint8(data)))
	return err
}

func (self *Client) Subscribe(topics []SubscribeTopic) error {
	id, err := self.getUsablePacketID()
	if err != nil {
		return err
	}
	err = self.SendMessage(NewSubscribeMessage(id, topics))
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

	id, err := self.getUsablePacketID()
	if err != nil {
		return err
	}
	err = self.SendMessage(NewUnsubscribeMessage(id, topics))
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

func (self *Client) recvConnectMessage(m *ConnectMessage) (err error) {
	return INVALID_MESSAGE_CAME
}
func (self *Client) recvConnackMessage(m *ConnackMessage) (err error) {
	self.AckMessage(m.PacketID)
	self.IsConnecting = true
	self.keepAlive()
	self.Redelivery()
	return err
}
func (self *Client) recvPublishMessage(m *PublishMessage) (err error) {
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

func (self *Client) recvPubackMessage(m *PubackMessage) (err error) {
	// acknowledge the sent Publish packet
	if m.PacketID > 0 {
		err = self.AckMessage(m.PacketID)
	}
	return err
}

func (self *Client) recvPubrecMessage(m *PubrecMessage) (err error) {
	// acknowledge the sent Publish packet
	err = self.AckMessage(m.PacketID)
	err = self.SendMessage(NewPubrelMessage(m.PacketID))
	return err
}

func (self *Client) recvPubrelMessage(m *PubrelMessage) (err error) {
	// acknowledge the sent Pubrel packet
	err = self.AckMessage(m.PacketID)
	err = self.SendMessage(NewPubcompMessage(m.PacketID))
	return err
}

func (self *Client) recvPubcompMessage(m *PubcompMessage) (err error) {
	// acknowledge the sent Pubrel packet
	err = self.AckMessage(m.PacketID)
	return err
}

func (self *Client) recvSubscribeMessage(m *SubscribeMessage) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *Client) recvSubackMessage(m *SubackMessage) (err error) {
	// acknowledge the sent subscribe packet
	self.AckMessage(m.PacketID)
	for i, code := range m.ReturnCodes {
		_ = self.AckSubscribeTopic(i, code)
	}
	return err
}
func (self *Client) recvUnsubscribeMessage(m *UnsubscribeMessage) (err error) {
	return INVALID_MESSAGE_CAME
}
func (self *Client) recvUnsubackMessage(m *UnsubackMessage) (err error) {
	// acknowledged the sent unsubscribe packet
	err = self.AckMessage(m.PacketID)
	return err
}

func (self *Client) recvPingreqMessage(m *PingreqMessage) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *Client) recvPingrespMessage(m *PingrespMessage) (err error) {
	elapsed := time.Since(self.PingBegin)
	// TODO: suspicious
	self.Duration = elapsed
	if elapsed.Seconds() >= float64(self.KeepAlive) {
		// TODO: this must be 'reasonable amount of time'
		err = self.SendMessage(NewDisconnectMessage())
	} else {
		self.keepAlive() // TODO: this make impossible to send ping manually
	}
	return err
}

func (self *Client) recvDisconnectMessage(m *DisconnectMessage) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *Client) ReadMessage() (Message, error) {
	return self.Ct.ReadMessage()
}
