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
	addr.Port = 8883 // MQTT default, TODO: set by config file
	if err != nil {
		return err
	}
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
		client := NewClient(&Transport{conn}, "", nil, 0, nil)
		go ReadLoop(self, client)
	}
}

func (self *Broker) DisconnectFromBroker(client *Client) {
	client.Will = nil
	client.IsConnecting = false
	client.KeepAliveTimer.Stop()
	if client.CleanSession {
		delete(self.Clients, client.ID)
	}

}

func (self *Broker) RunClientTimer(client *Client) {
	<-client.KeepAliveTimer.C
	self.DisconnectFromBroker(client)
	// TODO: logging?
}

func (self *Broker) ApplyDummyClientID() string {
	return "DummyClientID:" + strconv.Itoa(len(self.Clients)+1)
}

func (self *Broker) recvConnectMessage(m *ConnectMessage, c *Client) (err error) {
	if m.Protocol.Name != MQTT_3_1_1.Name {
		// server MAY disconnect
		return INVALID_PROTOCOL_NAME
	}

	if m.Protocol.Level != MQTT_3_1_1.Level {
		// CHECK: Is false correct?
		err = c.SendMessage(NewConnackMessage(false, UnacceptableProtocolVersion))
		return INVALID_PROTOCOL_LEVEL
	}

	c, ok := self.Clients[m.ClientID]
	if ok {
		// TODO: this might cause problem
		err = c.SendMessage(NewConnackMessage(false, IdentifierRejected))
		return CLIENT_ID_IS_USED_ALREADY
	}
	if len(m.ClientID) == 0 {
		m.ClientID = self.ApplyDummyClientID()
	}
	// TODO: authorization

	sessionPresent := ok
	cleanSession := m.Flags&CleanSession_Flag == CleanSession_Flag
	if cleanSession || !ok {
		// TODO: need to manage QoS base processing
		c.Duration = time.Duration(float32(m.KeepAlive) * 100000000 * 1.5)
		c.ID = m.ClientID
		c.User = m.User
		c.KeepAlive = m.KeepAlive
		c.Will = m.Will
		c.CleanSession = cleanSession
		self.Clients[m.ClientID] = c
		sessionPresent = false
	}

	if m.Flags&Will_Flag == Will_Flag {
		c.Will = m.Will
		// TODO: consider QoS and Retain as broker need
	} else {

	}

	if m.KeepAlive != 0 {
		go self.RunClientTimer(c)
	}
	err = c.SendMessage(NewConnackMessage(sessionPresent, Accepted))
	c.IsConnecting = true
	c.Redelivery()
	return err
}

func (self *Broker) recvConnackMessage(m *ConnackMessage, c *Client) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *Broker) recvPublishMessage(m *PublishMessage, c *Client) (err error) {
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
	case 1:
		err = c.SendMessage(NewPubackMessage(m.PacketID))
	case 2:
		err = c.SendMessage(NewPubrecMessage(m.PacketID))
	}
	return err
}

func (self *Broker) recvPubackMessage(m *PubackMessage, c *Client) (err error) {
	// acknowledge the sent Publish packet
	if m.PacketID > 0 {
		err = c.AckMessage(m.PacketID)
	}
	return err
}

func (self *Broker) recvPubrecMessage(m *PubrecMessage, c *Client) (err error) {
	// acknowledge the sent Publish packet
	err = c.AckMessage(m.PacketID)
	err = c.SendMessage(NewPubrelMessage(m.PacketID))
	return err
}

func (self *Broker) recvPubrelMessage(m *PubrelMessage, c *Client) (err error) {
	// acknowledge the sent Pubrel packet
	err = c.AckMessage(m.PacketID)
	err = c.SendMessage(NewPubcompMessage(m.PacketID))
	return err
}

func (self *Broker) recvPubcompMessage(m *PubcompMessage, c *Client) (err error) {
	// acknowledge the sent Pubrel packet
	err = c.AckMessage(m.PacketID)
	return err
}

func (self *Broker) recvSubscribeMessage(m *SubscribeMessage, c *Client) (err error) {
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
				edge.Subscribers[c.ID] = subTopic.QoS
				codes[i] = SubscribeReturnCode(subTopic.QoS)
				c.SubTopics = append(c.SubTopics,
					SubscribeTopic{SubscribeAck,
						edge.FullPath,
						uint8(subTopic.QoS),
					})
				if len(edge.RetainMessage) > 0 {
					// publish retain
					// TODO: check all arguments
					err = c.SendMessage(NewPublishMessage(false, edge.RetainQoS, true,
						edge.FullPath, m.PacketID, []uint8(edge.RetainMessage)))
					// TODO: error validation
				}
			}
		}
		returnCodes = append(returnCodes, codes...)
	}
	// TODO: check whether the number of return codes are correct?
	err = c.SendMessage(NewSubackMessage(m.PacketID, returnCodes))
	return err
}

func (self *Broker) recvSubackMessage(m *SubackMessage, c *Client) (err error) {
	return INVALID_MESSAGE_CAME
}
func (self *Broker) recvUnsubscribeMessage(m *UnsubscribeMessage, c *Client) (err error) {
	if len(m.TopicNames) == 0 {
		// protocol violation
	}
	// TODO: optimize here
	result := []SubscribeTopic{}
	for _, t := range c.SubTopics {
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
	c.SubTopics = result
	err = c.SendMessage(NewUnsubackMessage(m.PacketID))
	return err
}
func (self *Broker) recvUnsubackMessage(m *UnsubackMessage, c *Client) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *Broker) recvPingreqMessage(m *PingreqMessage, c *Client) (err error) {
	// Pingresp
	// TODO: calc elapsed time from previous pingreq.
	//       and store the time to duration of Transport
	err = c.SendMessage(NewPingrespMessage())
	if c.KeepAlive != 0 {
		c.ResetTimer()
		go self.RunClientTimer(c)
	}
	return err
}

func (self *Broker) recvPingrespMessage(m *PingrespMessage, c *Client) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *Broker) recvDisconnectMessage(m *DisconnectMessage, c *Client) (err error) {
	self.DisconnectFromBroker(c)
	// close the client
	return err
}

func (self *Broker) ReadMessage() (Message, error) {
	// TODO: should be removed
	return nil, nil
}
