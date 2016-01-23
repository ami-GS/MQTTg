package MQTTg

import (
	"io"
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
	SubTopics      []*SubscribeTopic
	PingBegin      time.Time
	PacketIDMap    map[uint16]Message
	CleanSession   bool
	KeepAliveTimer *time.Timer
	Duration       time.Duration
	LoopQuit       chan bool
	ReadChan       chan Message
}

func NewClient(id string, user *User, keepAlive uint16, will *Will) *Client {
	// TODO: when id is empty, then apply random
	return &Client{
		IsConnecting:   false,
		ID:             id,
		User:           user,
		KeepAlive:      keepAlive,
		Will:           will,
		SubTopics:      make([]*SubscribeTopic, 0),
		PacketIDMap:    make(map[uint16]Message, 0),
		CleanSession:   false,
		KeepAliveTimer: nil,
		Duration:       0,
		LoopQuit:       nil,
		ReadChan:       nil,
	}
}

func (self *Client) ResetTimer() {
	self.KeepAliveTimer.Reset(self.Duration)
}

func (self *Client) StartPingLoop() {
	t := time.NewTicker(time.Duration(self.KeepAlive) * time.Second)
	for {
		select {
		case <-t.C:
			EmitError(self.keepAlive())
		case <-self.LoopQuit:
			t.Stop()
			return
		}
	}
	t.Stop()

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
		self.SubTopics = append(self.SubTopics, &SubscribeTopic{})
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

func (self *Client) setPreviousSession(prevSession *Client) {
	self.SubTopics = prevSession.SubTopics
	self.PacketIDMap = prevSession.PacketIDMap
	self.CleanSession = prevSession.CleanSession
	self.Will = prevSession.Will
	self.Duration = prevSession.Duration
	self.KeepAliveTimer = time.NewTimer(self.Duration)
	self.KeepAlive = prevSession.KeepAlive
	// TODO: authorize here
	self.User = prevSession.User
}

func (self *Client) Connect(addPair string, cleanSession bool) error {
	if len(self.ID) == 0 && !cleanSession {
		// TODO: here should be warnning
		EmitError(CLEANSESSION_MUST_BE_TRUE)
		cleanSession = true
	}

	rAddr, err := net.ResolveTCPAddr("tcp4", addPair)
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp4", nil, rAddr)
	if err != nil {
		return err
	}
	self.Ct = &Transport{conn}
	self.LoopQuit = make(chan bool)
	self.ReadChan = make(chan Message)
	self.CleanSession = cleanSession
	go self.ReadMessage()
	go ReadLoop(self, self.ReadChan)
	// below can avoid first IsConnecting validation
	err = self.Ct.SendMessage(NewConnectMessage(self.KeepAlive,
		self.ID, cleanSession, self.Will, self.User))
	return err
}

func (self *Client) Publish(topic, data string, qos uint8, retain bool) (err error) {
	if qos >= 3 {
		return INVALID_QOS_3
	}
	if strings.Contains(topic, "#") || strings.Contains(topic, "+") {
		return WILDCARD_CHARACTERS_IN_PUBLISH
	}

	var id uint16
	if qos > 0 {
		id, err = self.getUsablePacketID()
		if err != nil {
			return err
		}
	}

	err = self.SendMessage(NewPublishMessage(false, qos, retain,
		topic, id, []uint8(data)))
	return err
}

func (self *Client) Subscribe(topics []*SubscribeTopic) error {
	id, err := self.getUsablePacketID()
	if err != nil {
		return err
	}
	for _, topic := range topics {
		parts := strings.Split(topic.Topic, "/")
		for i, part := range parts {
			if part == "#" && i != len(parts)-1 {
				return MULTI_LEVEL_WILDCARD_MUST_BE_ON_TAIL
			} else if strings.HasSuffix(part, "#") || strings.HasSuffix(part, "+") {
				return WILDCARD_MUST_NOT_BE_ADJACENT_TO_NAME
			}
		}
	}
	err = self.SendMessage(NewSubscribeMessage(id, topics))
	if err == nil {
		self.SubTopics = append(self.SubTopics, topics...)
	}
	return err
}

func (self *Client) Unsubscribe(topics []string) error {
	for _, name := range topics {
		exist := false
		for _, t := range self.SubTopics {
			if string(t.Topic) == name {
				t.State = UnSubscribeNonAck
				exist = true
				break
			}
		}
		if !exist {
			return UNSUBSCRIBE_TO_NON_SUBSCRIBE_TOPIC
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
	err = self.disconnectProcessing()
	return err
}

func (self *Client) disconnectProcessing() (err error) {
	// TODO: this might be not good way to close channel only once
	if self.IsConnecting {
		self.IsConnecting = false
		close(self.ReadChan)
		// need to unify these condition for both side
		if self.KeepAliveTimer != nil {
			self.KeepAliveTimer.Stop() // for broker side
		} else {
			self.LoopQuit <- true // for client side
			close(self.LoopQuit)
		}
		err = self.Ct.conn.Close()
	}
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
			EmitError(err)
		}
	}
	return err
}

func (self *Client) recvConnectMessage(m *ConnectMessage) (err error) {
	return INVALID_MESSAGE_CAME
}
func (self *Client) recvConnackMessage(m *ConnackMessage) (err error) {
	if m.ReturnCode != Accepted {
		return m.ReturnCode
	}
	self.IsConnecting = true
	if self.KeepAlive != 0 {
		go self.StartPingLoop()
	}
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
		if m.PacketID != 0 {
			return PACKET_ID_SHOULD_BE_ZERO
		}
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
	if err != nil {
		return err
	}
	err = self.SendMessage(NewPubrelMessage(m.PacketID))
	return err
}

func (self *Client) recvPubrelMessage(m *PubrelMessage) (err error) {
	// acknowledge the sent Pubrel packet
	err = self.AckMessage(m.PacketID)
	if err != nil {
		return err
	}
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
	}
	return err
}

func (self *Client) recvDisconnectMessage(m *DisconnectMessage) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *Client) ReadMessage() {
	for {
		m, err := self.Ct.ReadMessage()
		// the condition below is not cool
		if err == io.EOF {
			// when disconnect from beoker
			// TODO: server cannnot delete client session successfully
			// delete(self.Clients, self.ID) should work well
			err := self.disconnectProcessing()
			EmitError(err)
			return
		} else if err != nil {
			// when disconnect from client
			return
		}
		EmitError(err)
		if m != nil {
			self.ReadChan <- m
		}
	}
}
