package MQTTg

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

type Broker struct {
	MyAddr *net.TCPAddr
	// TODO: check whether not good to use addr as key
	Clients   map[string]*BrokerSideClient //map[clientID]*BrokerSideClient
	TopicRoot *TopicNode
}

func (self *Broker) Start() error {
	addr, err := GetLocalAddr()
	fmt.Println(addr)
	if err != nil {
		return err
	}
	self.MyAddr = addr
	listener, err := net.ListenTCP("tcp4", addr)
	if err != nil {
		// TODO: use channel to return error
		return err
	}
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			// TODO: use channel to return error
			EmitError(err)
			continue
		}
		bc := NewBrokerSideClient(&Transport{conn}, self)
		go bc.ReadLoop(bc) // TODO: use single Loop function
		go bc.WriteLoop()
	}
}

func (self *BrokerSideClient) disconnectProcessing() (err error) {
	w := self.Will
	broker := self.Broker
	if w != nil {
		if w.Retain {
			broker.TopicRoot.ApplyRetain(w.Topic, w.QoS, w.Message)
		}
		nodes, _ := broker.TopicRoot.GetTopicNodes(w.Topic, true)
		for subscriberID, reqQoS := range nodes[0].Subscribers {
			subscriber, _ := broker.Clients[subscriberID]
			self.Broker.checkQoSAndPublish(subscriber, w.QoS, reqQoS, w.Retain, w.Topic, []uint8(w.Message))
		}
	}
	if self.IsConnecting {
		self.KeepAliveTimer.Stop()
		if self.CleanSession {
			delete(broker.Clients, self.ID)
		}
	}
	err = self.disconnectBase()
	return err
}

func (self *Broker) checkQoSAndPublish(requestClient *BrokerSideClient, publisherQoS, requestedQoS uint8, retain bool, topic string, message []uint8) {
	var id uint16 = 0
	var err error
	qos := publisherQoS
	if requestedQoS < publisherQoS {
		// QoS downgrade
		qos = requestedQoS
	}
	if qos > 0 {
		id, err = requestClient.getUsablePacketID()
		if err != nil {
			panic(err)
		}
	}
	pub := NewPublishMessage(false, qos, retain, topic, id, message)
	requestClient.WriteChan <- pub
}

func (self *Broker) ApplyDummyClientID() string {
	return "DummyClientID:" + strconv.Itoa(len(self.Clients)+1)
}

type BrokerSideClient struct {
	*ClientInfo
	SubTopics []*SubscribeTopic
	Broker    *Broker
}

func NewBrokerSideClient(ct *Transport, broker *Broker) *BrokerSideClient {
	return &BrokerSideClient{
		ClientInfo: &ClientInfo{
			Ct:             ct,
			IsConnecting:   false,
			ID:             "",
			User:           nil,
			KeepAlive:      0,
			Will:           nil,
			PacketIDMap:    make(map[uint16]Message, 0),
			CleanSession:   false,
			KeepAliveTimer: time.NewTimer(0),
			Duration:       0,
			WriteChan:      make(chan Message),
		},
		SubTopics: make([]*SubscribeTopic, 0),
		Broker:    broker,
	}
}

func (self *BrokerSideClient) RunClientTimer() {
	<-self.KeepAliveTimer.C
	EmitError(CLIENT_TIMED_OUT)
	self.disconnectProcessing()
	// TODO: logging?
}

func (self *BrokerSideClient) setPreviousSession(prevSession *BrokerSideClient) {
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

func (self *BrokerSideClient) recvConnectMessage(m *ConnectMessage) (err error) {
	// NOTICE: when connection error is sent to client, self.Ct.SendMessage()
	//         should be used for avoiding Isconnecting validation
	if m.Protocol.Name != MQTT_3_1_1.Name {
		// server MAY disconnect
		self.disconnectProcessing()
		return INVALID_PROTOCOL_NAME
	}

	if m.Protocol.Level != MQTT_3_1_1.Level {
		// CHECK: Is false correct?
		err = self.Ct.SendMessage(NewConnackMessage(false, UnacceptableProtocolVersion))
		self.disconnectProcessing()
		return INVALID_PROTOCOL_LEVEL
	}

	c, ok := self.Broker.Clients[m.ClientID]
	if ok && c.IsConnecting {
		// TODO: this might cause problem
		// TODO; which should be disconnected, connecting one? or trying to connect one?
		err = self.Ct.SendMessage(NewConnackMessage(false, IdentifierRejected))
		self.disconnectProcessing()
		return CLIENT_ID_IS_USED_ALREADY
	}
	cleanSession := m.Flags&CleanSession_Flag == CleanSession_Flag
	if ok && !cleanSession {
		self.setPreviousSession(c)
	} else if !cleanSession && len(m.ClientID) == 0 {
		err = self.Ct.SendMessage(NewConnackMessage(false, IdentifierRejected))
		self.disconnectProcessing()
		return CLEANSESSION_MUST_BE_TRUE
	}

	sessionPresent := ok
	if cleanSession || !ok {
		// TODO: need to manage QoS base processing
		self.Duration = time.Duration(float32(m.KeepAlive)*1.5) * time.Second
		if len(m.ClientID) == 0 {
			m.ClientID = self.Broker.ApplyDummyClientID()
		}
		self.ID = m.ClientID
		self.User = m.User
		self.KeepAlive = m.KeepAlive
		self.Will = m.Will
		self.CleanSession = cleanSession
		self.KeepAliveTimer = time.NewTimer(self.Duration)
		sessionPresent = false
	}
	self.Broker.Clients[m.ClientID] = self

	if m.Flags&Will_Flag == Will_Flag {
		self.Will = m.Will
		// TODO: consider QoS and Retain as broker need
	} else {

	}

	if m.KeepAlive != 0 {
		go self.RunClientTimer()
	}
	self.IsConnecting = true
	connack := NewConnackMessage(sessionPresent, Accepted)
	self.WriteChan <- connack
	self.Redelivery()
	return err
}

func (self *BrokerSideClient) recvConnackMessage(m *ConnackMessage) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *BrokerSideClient) recvPublishMessage(m *PublishMessage) (err error) {
	if m.Dup {
		// re-delivered
	} else {
		// first time delivery
	}

	if m.Retain {
		// store tehe application message to designated topic
		data := string(m.Payload)
		if m.QoS == 0 && len(data) > 0 {
			// TODO: warnning, in this case data cannot be stored.
			// discard retained message
			data = ""
		}
		self.Broker.TopicRoot.ApplyRetain(m.TopicName, m.QoS, data)
	}

	nodes, err := self.Broker.TopicRoot.GetTopicNodes(m.TopicName, true)
	if err != nil {
		return err
	}
	for subscriberID, reqQoS := range nodes[0].Subscribers {
		subscriber, ok := self.Broker.Clients[subscriberID]
		if !ok {
			continue
		}
		self.Broker.checkQoSAndPublish(subscriber, m.QoS, reqQoS, false, m.TopicName, m.Payload)
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

func (self *BrokerSideClient) recvPubackMessage(m *PubackMessage) (err error) {
	// acknowledge the sent Publish packet
	if m.PacketID > 0 {
		err = self.AckMessage(m.PacketID)
	}
	return err
}

func (self *BrokerSideClient) recvPubrecMessage(m *PubrecMessage) (err error) {
	// acknowledge the sent Publish packet
	err = self.AckMessage(m.PacketID)
	if err != nil {
		return err
	}
	pubrel := NewPubrelMessage(m.PacketID)
	self.WriteChan <- pubrel
	return err
}

func (self *BrokerSideClient) recvPubrelMessage(m *PubrelMessage) (err error) {
	// acknowledge the sent Pubrel packet
	err = self.AckMessage(m.PacketID)
	if err != nil {
		return err
	}
	pubcomp := NewPubcompMessage(m.PacketID)
	self.WriteChan <- pubcomp
	return err
}

func (self *BrokerSideClient) recvPubcompMessage(m *PubcompMessage) (err error) {
	// acknowledge the sent Pubrel packet
	err = self.AckMessage(m.PacketID)
	return err
}

func (self *BrokerSideClient) recvSubscribeMessage(m *SubscribeMessage) (err error) {
	// TODO: check The wild card is permitted
	returnCodes := make([]SubscribeReturnCode, len(m.SubscribeTopics))
	for i, subTopic := range m.SubscribeTopics {
		// TODO: need to validate wheter there are same topics or not
		edges, err := self.Broker.TopicRoot.GetTopicNodes(subTopic.Topic, true)
		code := SubscribeReturnCode(subTopic.QoS)
		if err != nil {
			code = SubscribeFailure
		} else {
			for _, edge := range edges {
				edge.Subscribers[self.ID] = subTopic.QoS
				self.SubTopics = append(self.SubTopics,
					&SubscribeTopic{SubscribeAck,
						edge.FullPath,
						uint8(subTopic.QoS),
					})
				if len(edge.RetainMessage) > 0 {
					// publish retain
					// TODO: check all arguments
					self.Broker.checkQoSAndPublish(self, edge.RetainQoS, subTopic.QoS, true, edge.FullPath, []uint8(edge.RetainMessage))
					EmitError(err)
				}
			}
		}
		returnCodes[i] = code
	}
	// TODO: check whether the number of return codes are correct?
	suback := NewSubackMessage(m.PacketID, returnCodes)
	self.WriteChan <- suback
	return err
}

func (self *BrokerSideClient) recvSubackMessage(m *SubackMessage) (err error) {
	return INVALID_MESSAGE_CAME
}
func (self *BrokerSideClient) recvUnsubscribeMessage(m *UnsubscribeMessage) (err error) {
	if len(m.TopicNames) == 0 {
		return PROTOCOL_VIOLATION
	}

	result := []*SubscribeTopic{}
	for _, name := range m.TopicNames {
		self.Broker.TopicRoot.DeleteSubscriber(self.ID, name)
		for _, t := range self.SubTopics {
			if string(t.Topic) == string(name) {
				result = append(result, t)
			}
		}
	}
	self.SubTopics = result
	unsuback := NewUnsubackMessage(m.PacketID)

	self.WriteChan <- unsuback
	return err
}
func (self *BrokerSideClient) recvUnsubackMessage(m *UnsubackMessage) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *BrokerSideClient) recvPingreqMessage(m *PingreqMessage) (err error) {
	// Pingresp
	// TODO: calc elapsed time from previous pingreq.
	//       and store the time to duration of Transport
	pingresp := NewPingrespMessage()
	self.WriteChan <- pingresp
	if self.KeepAlive != 0 {
		self.ResetTimer()
		go self.RunClientTimer()
	}
	return err
}

func (self *BrokerSideClient) recvPingrespMessage(m *PingrespMessage) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *BrokerSideClient) recvDisconnectMessage(m *DisconnectMessage) (err error) {
	self.Will = nil
	self.disconnectProcessing()
	// close the client
	return err
}
