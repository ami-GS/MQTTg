package MQTTg

import (
	"net"
	"strconv"
	"time"
)

type Broker struct {
	MyAddr *net.TCPAddr
	// TODO: check whether not good to use addr as key
	Clients   map[string]*Client //map[clientID]*CLient
	TopicRoot *TopicNode
}

func (self *Broker) Start() error {
	addr, err := GetLocalAddr()
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
		client := NewClient("", nil, 0, nil)
		client.Ct = &Transport{conn}
		client.ReadChan = make(chan Message)
		bc := &BrokerSideClient{client, self}
		go bc.ReadMessage()
		go ReadLoop(bc, bc.ReadChan)
	}
}

func (self *BrokerSideClient) DisconnectFromBroker() {
	self.Will = nil
	self.disconnectProcessing()
	if self.CleanSession {
		delete(self.Clients, self.ID)
	}

}

func (self *BrokerSideClient) RunClientTimer() {
	<-self.KeepAliveTimer.C
	EmitError(CLIENT_TIMED_OUT)
	self.DisconnectFromBroker()
	// TODO: logging?
}

func (self *Broker) ApplyDummyClientID() string {
	return "DummyClientID:" + strconv.Itoa(len(self.Clients)+1)
}

type BrokerSideClient struct {
	*Client
	*Broker
}

func (self *BrokerSideClient) recvConnectMessage(m *ConnectMessage) (err error) {
	// NOTICE: when connection error is sent to client, self.Ct.SendMessage()
	//         should be used for avoiding Isconnecting validation
	if m.Protocol.Name != MQTT_3_1_1.Name {
		// server MAY disconnect
		return INVALID_PROTOCOL_NAME
	}

	if m.Protocol.Level != MQTT_3_1_1.Level {
		// CHECK: Is false correct?
		err = self.Ct.SendMessage(NewConnackMessage(false, UnacceptableProtocolVersion))
		return INVALID_PROTOCOL_LEVEL
	}

	c, ok := self.Clients[m.ClientID]
	if ok && c.IsConnecting {
		// TODO: this might cause problem
		// TODO; which should be disconnected, connecting one? or trying to connect one?
		err = self.Ct.SendMessage(NewConnackMessage(false, IdentifierRejected))
		return CLIENT_ID_IS_USED_ALREADY
	}
	cleanSession := m.Flags&CleanSession_Flag == CleanSession_Flag
	if ok && !cleanSession {
		self.Client.setPreviousSession(c)
	} else if !cleanSession && len(m.ClientID) == 0 {
		err = self.Ct.SendMessage(NewConnackMessage(false, IdentifierRejected))
		return CLEANSESSION_MUST_BE_TRUE
	}

	sessionPresent := ok
	if cleanSession || !ok {
		// TODO: need to manage QoS base processing
		self.Client.Duration = time.Duration(float32(m.KeepAlive)*1.5) * time.Second
		if len(m.ClientID) == 0 {
			m.ClientID = self.ApplyDummyClientID()
		}
		self.Client.ID = m.ClientID
		self.Client.User = m.User
		self.Client.KeepAlive = m.KeepAlive
		self.Client.Will = m.Will
		self.Client.CleanSession = cleanSession
		self.Client.KeepAliveTimer = time.NewTimer(self.Client.Duration)
		sessionPresent = false
	}
	self.Clients[m.ClientID] = self.Client

	if m.Flags&Will_Flag == Will_Flag {
		self.Client.Will = m.Will
		// TODO: consider QoS and Retain as broker need
	} else {

	}

	if m.KeepAlive != 0 {
		go self.RunClientTimer()
	}
	self.Client.IsConnecting = true
	err = self.Client.SendMessage(NewConnackMessage(sessionPresent, Accepted))
	self.Client.Redelivery()
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
		self.TopicRoot.ApplyRetain(m.TopicName, m.QoS, data)
	}

	nodes, err := self.TopicRoot.GetTopicNodes(m.TopicName)
	if err != nil {
		return err
	}
	for subscriberID, qos := range nodes[0].Subscribers {
		subscriber, _ := self.Clients[subscriberID]
		subscriber.Publish(m.TopicName, string(m.Payload), qos, false)
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
	err = self.SendMessage(NewPubrelMessage(m.PacketID))
	return err
}

func (self *BrokerSideClient) recvPubrelMessage(m *PubrelMessage) (err error) {
	// acknowledge the sent Pubrel packet
	err = self.AckMessage(m.PacketID)
	if err != nil {
		return err
	}
	err = self.SendMessage(NewPubcompMessage(m.PacketID))
	return err
}

func (self *BrokerSideClient) recvPubcompMessage(m *PubcompMessage) (err error) {
	// acknowledge the sent Pubrel packet
	err = self.AckMessage(m.PacketID)
	return err
}

func (self *BrokerSideClient) recvSubscribeMessage(m *SubscribeMessage) (err error) {
	// TODO: check The wild card is permitted
	returnCodes := make([]SubscribeReturnCode, 0)
	for _, subTopic := range m.SubscribeTopics {
		// TODO: need to validate wheter there are same topics or not
		edges, err := self.TopicRoot.GetTopicNodes(subTopic.Topic)
		codes := make([]SubscribeReturnCode, len(edges))
		if err != nil {
			for i, _ := range codes {
				codes[i] = SubscribeFailure
			}
		} else {
			for i, edge := range edges {
				edge.Subscribers[self.ID] = subTopic.QoS
				codes[i] = SubscribeReturnCode(subTopic.QoS)
				self.SubTopics = append(self.SubTopics,
					&SubscribeTopic{SubscribeAck,
						edge.FullPath,
						uint8(subTopic.QoS),
					})
				if len(edge.RetainMessage) > 0 {
					// publish retain
					// TODO: check all arguments
					id, err := self.getUsablePacketID()
					EmitError(err)
					err = self.SendMessage(NewPublishMessage(false, edge.RetainQoS, true,
						edge.FullPath, id, []uint8(edge.RetainMessage)))
					EmitError(err)
					// TODO: error validation
				}
			}
		}
		returnCodes = append(returnCodes, codes...)
	}
	// TODO: check whether the number of return codes are correct?
	err = self.SendMessage(NewSubackMessage(m.PacketID, returnCodes))
	return err
}

func (self *BrokerSideClient) recvSubackMessage(m *SubackMessage) (err error) {
	return INVALID_MESSAGE_CAME
}
func (self *BrokerSideClient) recvUnsubscribeMessage(m *UnsubscribeMessage) (err error) {
	if len(m.TopicNames) == 0 {
		// protocol violation
	}
	// TODO: optimize here
	result := []*SubscribeTopic{}
	for _, t := range self.SubTopics {
		del := false
		for _, name := range m.TopicNames {
			if string(t.Topic) == string(name) {
				del = true
			}
		}
		if !del {
			result = append(result, t)
		}
	}
	self.SubTopics = result
	err = self.SendMessage(NewUnsubackMessage(m.PacketID))
	return err
}
func (self *BrokerSideClient) recvUnsubackMessage(m *UnsubackMessage) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *BrokerSideClient) recvPingreqMessage(m *PingreqMessage) (err error) {
	// Pingresp
	// TODO: calc elapsed time from previous pingreq.
	//       and store the time to duration of Transport
	err = self.SendMessage(NewPingrespMessage())
	if err != nil {
		return err
	}
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
	self.DisconnectFromBroker()
	// close the client
	return err
}
