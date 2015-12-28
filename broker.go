package MQTTg

import (
	"net"
	"strconv"
	"time"
)

type Broker struct {
	Bt *Transport
	// TODO: check whether not good to use addr as key
	Clients   map[*net.UDPAddr]*ClientInfo // map[addr]*ClientInfo
	ClientIDs map[string]*ClientInfo       // map[clientID]*ClientInfo
	TopicRoot *TopicNode
}

func (self *Broker) DisconnectFromBroker(client *ClientInfo) {
	client.Will = nil
	client.IsConnecting = false
	client.KeepAliveTimer.Stop()
	if client.CleanSession {
		delete(self.Clients, client.RemoteAddr)
		delete(self.ClientIDs, client.ID)
	}

}

func (self *Broker) RunClientTimer(client *ClientInfo) {
	<-client.KeepAliveTimer.C
	self.DisconnectFromBroker(client)
	// TODO: logging?
}

type ClientInfo struct {
	*Client
	KeepAliveTimer *time.Timer
	Duration       time.Duration
}

func NewClientInfo(c *Client, duration time.Duration) *ClientInfo {
	return &ClientInfo{
		Client:         c,
		KeepAliveTimer: time.NewTimer(duration),
		Duration:       duration,
	}
}

func (self *ClientInfo) ResetTimer() {
	self.KeepAliveTimer.Reset(self.Duration)
}

func (self *Broker) ApplyDummyClientID() string {
	return "DummyClientID:" + strconv.Itoa(len(self.ClientIDs)+1)
}

func (self *Broker) recvConnectMessage(m *ConnectMessage, addr *net.UDPAddr) (err error) {
	client, ok := self.Clients[addr]
	if ok && client.IsConnecting {
		self.DisconnectFromBroker(client)
		return PROTOCOL_VIOLATION
	}

	if m.Protocol.Name != MQTT_3_1_1.Name {
		// server MAY disconnect
		return INVALID_PROTOCOL_NAME
	}

	if m.Protocol.Level != MQTT_3_1_1.Level {
		// CHECK: Is false correct?
		err = self.Bt.SendMessage(NewConnackMessage(false, UnacceptableProtocolVersion), addr)
		return INVALID_PROTOCOL_LEVEL
	}

	_, ok = self.ClientIDs[m.ClientID]
	if ok {
		// TODO: this might cause problem
		err = self.Bt.SendMessage(NewConnackMessage(false, IdentifierRejected), addr)
		return CLIENT_ID_IS_USED_ALREADY
	}
	if len(m.ClientID) == 0 {
		m.ClientID = self.ApplyDummyClientID()
	}

	if m.Flags&Reserved_Flag == Reserved_Flag {
		// TODO: disconnect the connection
		return MALFORMED_CONNECT_FLAG_BIT
	}

	if m.Flags&UserName_Flag != UserName_Flag && m.Flags&Password_Flag == Password_Flag {
		return USERNAME_DOES_NOT_EXIST_WITH_PASSWORD
	}
	// TODO: authorization

	client, ok = self.Clients[addr]
	sessionPresent := ok
	cleanSession := m.Flags&CleanSession_Flag == CleanSession_Flag
	if cleanSession || !ok {
		// TODO: need to manage QoS base processing
		duration := time.Duration(float32(m.KeepAlive) * 100000000 * 1.5)
		client = NewClientInfo(NewClient(self.Bt, addr, m.ClientID,
			m.User, m.KeepAlive, m.Will, cleanSession), duration)
		self.Clients[addr] = client
		self.ClientIDs[m.ClientID] = client
		sessionPresent = false
	}

	if m.Flags&Will_Flag == Will_Flag {
		client.Will = m.Will
		// TODO: consider QoS and Retain as broker need
	} else {

	}

	if m.KeepAlive != 0 {
		go self.RunClientTimer(client)
	}
	err = self.Bt.SendMessage(NewConnackMessage(sessionPresent, Accepted), addr)
	client.IsConnecting = true
	client.Redelivery()
	return err
}

func (self *Broker) recvConnackMessage(m *ConnackMessage, addr *net.UDPAddr) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *Broker) recvPublishMessage(m *PublishMessage, addr *net.UDPAddr) (err error) {
	client, ok := self.Clients[addr]
	if !ok {
		return CLIENT_NOT_EXIST
	}
	if m.QoS == 3 {
		self.DisconnectFromBroker(client)
		return INVALID_QOS_3
	}

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

	switch m.QoS {
	// in any case, Dub must be 0
	case 0:
	case 1:
		err = client.SendMessage(NewPubackMessage(m.PacketID))
	case 2:
		err = client.SendMessage(NewPubrecMessage(m.PacketID))
	}
	return err
}

func (self *Broker) recvPubackMessage(m *PubackMessage, addr *net.UDPAddr) (err error) {
	client, ok := self.Clients[addr]
	if !ok {
		return CLIENT_NOT_EXIST
	}
	// acknowledge the sent Publish packet
	if m.PacketID > 0 {
		err = client.AckMessage(m.PacketID)
	}
	return err
}

func (self *Broker) recvPubrecMessage(m *PubrecMessage, addr *net.UDPAddr) (err error) {
	client, ok := self.Clients[addr]
	if !ok {
		return CLIENT_NOT_EXIST
	}
	// acknowledge the sent Publish packet
	err = client.AckMessage(m.PacketID)
	err = client.SendMessage(NewPubrelMessage(m.PacketID))
	return err
}

func (self *Broker) recvPubrelMessage(m *PubrelMessage, addr *net.UDPAddr) (err error) {
	client, ok := self.Clients[addr]
	if !ok {
		return CLIENT_NOT_EXIST
	}
	// acknowledge the sent Pubrel packet
	err = client.AckMessage(m.PacketID)
	err = client.SendMessage(NewPubcompMessage(m.PacketID))
	return err
}

func (self *Broker) recvPubcompMessage(m *PubcompMessage, addr *net.UDPAddr) (err error) {
	client, ok := self.Clients[addr]
	if !ok {
		return CLIENT_NOT_EXIST
	}
	// acknowledge the sent Pubrel packet
	err = client.AckMessage(m.PacketID)
	return err
}

func (self *Broker) recvSubscribeMessage(m *SubscribeMessage, addr *net.UDPAddr) (err error) {
	// TODO: check The wild card is permitted
	client, ok := self.Clients[addr]
	if !ok {
		return CLIENT_NOT_EXIST
	}
	returnCodes := make([]SubscribeReturnCode, 0)
	for _, subTopic := range m.SubscribeTopics {
		// TODO: need to validate wheter there are same topics or not
		edges, err := self.TopicRoot.GetTopicNodes(subTopic.Topic)
		codes := make([]SubscribeReturnCode, len(edges))
		if err != nil || !ok {
			for i, _ := range codes {
				codes[i] = SubscribeFailure
			}
		} else {
			for i, edge := range edges {
				edge.Subscribers[client.ID] = subTopic.QoS
				codes[i] = SubscribeReturnCode(subTopic.QoS)
				client.SubTopics = append(client.SubTopics,
					SubscribeTopic{SubscribeAck,
						edge.FullPath,
						uint8(subTopic.QoS),
					})
				if len(edge.RetainMessage) > 0 {
					// publish retain
					// TODO: check all arguments
					err = client.SendMessage(NewPublishMessage(false, edge.RetainQoS, true,
						edge.FullPath, m.PacketID, []uint8(edge.RetainMessage)))
					// TODO: error validation
				}
			}
		}
		returnCodes = append(returnCodes, codes...)
	}
	// TODO: check whether the number of return codes are correct?
	err = client.SendMessage(NewSubackMessage(m.PacketID, returnCodes))
	return err
}

func (self *Broker) recvSubackMessage(m *SubackMessage, addr *net.UDPAddr) (err error) {
	return INVALID_MESSAGE_CAME
}
func (self *Broker) recvUnsubscribeMessage(m *UnsubscribeMessage, addr *net.UDPAddr) (err error) {
	client, ok := self.Clients[addr]
	if !ok {
		return CLIENT_NOT_EXIST
	}
	if len(m.TopicNames) == 0 {
		// protocol violation
	}
	// TODO: optimize here
	result := []SubscribeTopic{}
	for _, t := range client.SubTopics {
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
	client.SubTopics = result
	err = client.SendMessage(NewUnsubackMessage(m.PacketID))
	return err
}
func (self *Broker) recvUnsubackMessage(m *UnsubackMessage, addr *net.UDPAddr) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *Broker) recvPingreqMessage(m *PingreqMessage, addr *net.UDPAddr) (err error) {
	// Pingresp
	// TODO: calc elapsed time from previous pingreq.
	//       and store the time to duration of Transport
	client, ok := self.Clients[addr]
	if !ok {
		return CLIENT_NOT_EXIST
	}
	err = client.SendMessage(NewPingrespMessage())
	if client.KeepAlive != 0 {
		client.ResetTimer()
		go self.RunClientTimer(client)
	}
	return err
}

func (self *Broker) recvPingrespMessage(m *PingrespMessage, addr *net.UDPAddr) (err error) {
	return INVALID_MESSAGE_CAME
}

func (self *Broker) recvDisconnectMessage(m *DisconnectMessage, addr *net.UDPAddr) (err error) {
	client, ok := self.Clients[addr]
	if !ok {
		return CLIENT_NOT_EXIST
	}
	self.DisconnectFromBroker(client)
	// close the client
	return err
}

func (self *Broker) ReadMessageFrom() (Message, *net.UDPAddr, error) {
	return self.Bt.ReadMessageFrom()

}
