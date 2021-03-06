package MQTTg

import (
	"fmt"
	"io"
	"math/rand"
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

type ClientInfo struct {
	Ct             *Transport
	IsConnecting   bool
	ID             string
	User           *User
	KeepAlive      uint16
	Will           *Will
	PacketIDMap    map[uint16]Message
	CleanSession   bool
	KeepAliveTimer *time.Timer
	Duration       time.Duration
	LoopQuit       chan bool
	WriteChan      chan Message
}

type Client struct {
	*ClientInfo
	PingBegin time.Time
}

func NewClient(id string, user *User, keepAlive uint16, will *Will) *Client {
	// TODO: when id is empty, then apply random
	return &Client{
		ClientInfo: &ClientInfo{
			IsConnecting:   false,
			ID:             id,
			User:           user,
			KeepAlive:      keepAlive,
			Will:           will,
			PacketIDMap:    make(map[uint16]Message, 0),
			CleanSession:   false,
			KeepAliveTimer: time.NewTimer(0),
			Duration:       0,
			LoopQuit:       nil,
			WriteChan:      nil,
		},
	}
}

func (self *ClientInfo) ResetTimer() {
	self.KeepAliveTimer.Reset(self.Duration)
}

func (self *Client) StartPingLoop() {
	t := time.NewTicker(time.Duration(self.KeepAlive) * time.Second)
	for {
		select {
		case <-t.C:
			self.keepAlive()
		case <-self.LoopQuit:
			t.Stop()
			return
		}
	}
	t.Stop()

}

type Edge interface {
	recvConnectMessage(*ConnectMessage) error
	recvConnackMessage(*ConnackMessage) error
	recvPublishMessage(*PublishMessage) error
	recvPubackMessage(*PubackMessage) error
	recvPubrecMessage(*PubrecMessage) error
	recvPubrelMessage(*PubrelMessage) error
	recvPubcompMessage(*PubcompMessage) error
	recvSubscribeMessage(*SubscribeMessage) error
	recvSubackMessage(*SubackMessage) error
	recvUnsubscribeMessage(*UnsubscribeMessage) error
	recvUnsubackMessage(*UnsubackMessage) error
	recvPingreqMessage(*PingreqMessage) error
	recvPingrespMessage(*PingrespMessage) error
	recvDisconnectMessage(*DisconnectMessage) error
	disconnectProcessing() error
}

func (self *ClientInfo) ReadLoop(edge Edge) (err error) {
	for {
		m, err := self.Ct.ReadMessage()
		EmitError(err)
		if err == io.EOF {
			EmitError(edge.disconnectProcessing())
			return err
		} else if err != nil {
			// ?
			return err
		}
		if m != nil {
			switch m := m.(type) {
			case *ConnectMessage:
				err = edge.recvConnectMessage(m)
			case *ConnackMessage:
				err = edge.recvConnackMessage(m)
			case *PublishMessage:
				err = edge.recvPublishMessage(m)
			case *PubackMessage:
				err = edge.recvPubackMessage(m)
			case *PubrecMessage:
				err = edge.recvPubrecMessage(m)
			case *PubrelMessage:
				err = edge.recvPubrelMessage(m)
			case *PubcompMessage:
				err = edge.recvPubcompMessage(m)
			case *SubscribeMessage:
				err = edge.recvSubscribeMessage(m)
			case *SubackMessage:
				err = edge.recvSubackMessage(m)
			case *UnsubscribeMessage:
				err = edge.recvUnsubscribeMessage(m)
			case *UnsubackMessage:
				err = edge.recvUnsubackMessage(m)
			case *PingreqMessage:
				err = edge.recvPingreqMessage(m)
			case *PingrespMessage:
				err = edge.recvPingrespMessage(m)
			case *DisconnectMessage:
				err = edge.recvDisconnectMessage(m)
			}
		}
		EmitError(err)
	}
	return
}

func (self *ClientInfo) WriteLoop() (err error) {
	for m := range self.WriteChan {
		if !self.IsConnecting {
			return NOT_CONNECTED
		}
		id := m.GetPacketID()
		_, ok := self.PacketIDMap[id]
		if ok {
			return PACKET_ID_IS_USED_ALREADY
		}
		switch m.(type) {
		case *PublishMessage:
			if id > 0 {
				self.PacketIDMap[id] = m
			}
		case *PubrecMessage, *PubrelMessage, *SubscribeMessage, *UnsubscribeMessage:
			if id == 0 {
				return PACKET_ID_SHOULD_NOT_BE_ZERO
			}
			self.PacketIDMap[id] = m
		}

		err = self.Ct.SendMessage(m)
		if err != nil {
			EmitError(err)
			return err
		}
	}
	return
}

func (self *ClientInfo) getUsablePacketID() (uint16, error) {
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
	if len(self.ID) == 0 && !cleanSession {
		// TODO: here should be warnning
		EmitError(CLEANSESSION_MUST_BE_TRUE)
		cleanSession = true
	}

	t := NewTransport()
	err := t.Connect(addPair)
	if err != nil {
		return err
	}

	self.Ct = t
	self.LoopQuit = make(chan bool)
	self.WriteChan = make(chan Message)
	self.CleanSession = cleanSession
	go self.ReadLoop(self) // TODO: use single Loop function
	go self.WriteLoop()
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

	pub := NewPublishMessage(false, qos, retain, topic, id, []uint8(data))
	self.WriteChan <- pub
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
	sub := NewSubscribeMessage(id, topics)
	self.WriteChan <- sub
	return err
}

func (self *Client) Unsubscribe(topics []string) error {
	for _, name := range topics {
		parts := strings.Split(name, "/")
		for i, part := range parts {
			if part == "#" && i != len(parts)-1 {
				return MULTI_LEVEL_WILDCARD_MUST_BE_ON_TAIL
			} else if strings.HasSuffix(part, "#") || strings.HasSuffix(part, "+") {
				return WILDCARD_MUST_NOT_BE_ADJACENT_TO_NAME
			}
		}
	}

	id, err := self.getUsablePacketID()
	if err != nil {
		return err
	}
	unsub := NewUnsubscribeMessage(id, topics)
	self.WriteChan <- unsub
	return err
}

func (self *Client) keepAlive() {
	ping := NewPingreqMessage()
	self.WriteChan <- ping
	// TODO: ping begin should be start if the delivery is nicely done?
	/*
		if err == nil {
			self.PingBegin = time.Now()
		}
	*/
}

func (self *Client) Disconnect() {
	discon := NewDisconnectMessage()
	self.WriteChan <- discon

	go func() {
		// wait broker side detect the DisconnectMessage
		// for further Will message delivery
		time.Sleep(self.Duration * 2)
		self.disconnectProcessing()
	}()
}

func (self *ClientInfo) disconnectBase() (err error) {
	close(self.WriteChan)
	if self.IsConnecting {
		self.IsConnecting = false
		self.Will = nil
	}
	err = self.Ct.conn.Close()
	return err
}

func (self *Client) disconnectProcessing() (err error) {
	if self.IsConnecting {
		self.LoopQuit <- true // for client side
		close(self.LoopQuit)
	}
	err = self.disconnectBase()
	return err
}

func (self *ClientInfo) AckMessage(id uint16) error {
	_, ok := self.PacketIDMap[id]
	if !ok {
		return PACKET_ID_DOES_NOT_EXIST
	}
	delete(self.PacketIDMap, id)
	return nil
}

func (self *ClientInfo) Redelivery() {
	if !self.CleanSession && len(self.PacketIDMap) > 0 {
		for _, v := range self.PacketIDMap {
			switch m := v.(type) {
			case *PublishMessage:
				// Only Publish Message's DUP is set
				m.Dup = true
				self.WriteChan <- m
			default:
				self.WriteChan <- v
			}
		}
	}
}

func (self *Client) recvConnectMessage(m *ConnectMessage) (err error) {
	return INVALID_MESSAGE_CAME
}
func (self *Client) recvConnackMessage(m *ConnackMessage) (err error) {
	if m.ReturnCode != Accepted {
		self.disconnectProcessing()
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
		puback := NewPubackMessage(m.PacketID)
		self.WriteChan <- puback
	case 2:
		pubrec := NewPubrecMessage(m.PacketID)
		self.WriteChan <- pubrec
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
	pubrel := NewPubrelMessage(m.PacketID)
	self.WriteChan <- pubrel
	return err
}

func (self *Client) recvPubrelMessage(m *PubrelMessage) (err error) {
	// acknowledge the sent Pubrel packet
	err = self.AckMessage(m.PacketID)
	if err != nil {
		return err
	}
	pubcomp := NewPubcompMessage(m.PacketID)
	self.WriteChan <- pubcomp
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
	self.Duration = time.Since(self.PingBegin)
	// TODO: suspicious
	if FrameDebug {
		fmt.Printf("Ping RTT is %s\n\n", self.Duration)
	}
	if self.Duration.Seconds() >= float64(self.KeepAlive) {
		// TODO: this must be 'reasonable amount of time'
		discon := NewDisconnectMessage()
		self.WriteChan <- discon
		return SERVER_TIMED_OUT
	}
	return err
}

func (self *Client) recvDisconnectMessage(m *DisconnectMessage) (err error) {
	return INVALID_MESSAGE_CAME
}
