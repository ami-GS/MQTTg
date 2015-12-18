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

func (self *Broker) ReadLoop() error {
	for {
		m, addr, err := self.Bt.ReadMessageFrom()
		if err != nil {
			EmitError(err)
			continue
		}

		client, ok := self.Clients[addr]

		switch message := m.(type) {
		case *ConnectMessage:
			if message.Protocol.Level != MQTT_3_1_1.Level {
				// CHECK: Is false correct?
				err = self.Bt.SendMessage(NewConnackMessage(false, UnacceptableProtocolVersion), addr)
				EmitError(INVALID_PROTOCOL_LEVEL)
				continue
			}

			_, ok = self.ClientIDs[message.ClientID]
			if ok {
				// TODO: this might cause problem
				err = self.Bt.SendMessage(NewConnackMessage(false, IdentifierRejected), addr)
				EmitError(CLIENT_ID_IS_USED_ALREADY)
				continue
			}
			if len(message.ClientID) == 0 {
				message.ClientID = self.ApplyDummyClientID()
			}

			if message.Flags&Reserved_Flag == Reserved_Flag {
				// TODO: disconnect the connection
				continue
			}

			if message.Flags&UserName_Flag != UserName_Flag && message.Flags&Password_Flag == Password_Flag {
				EmitError(USERNAME_DOES_NOT_EXIST_WITH_PASSWORD)
				continue
			}
			// TODO: authorization

			client, ok = self.Clients[addr]
			sessionPresent := ok
			cleanSession := message.Flags&CleanSession_Flag == CleanSession_Flag
			if cleanSession || !ok {
				// TODO: need to manage QoS base processing
				duration := time.Duration(float32(message.KeepAlive) * 100000000 * 1.5)
				client = NewClientInfo(NewClient(self.Bt, addr, message.ClientID,
					message.User, message.KeepAlive, message.Will, cleanSession), duration)
				self.Clients[addr] = client
				self.ClientIDs[message.ClientID] = client
				sessionPresent = false
			}

			if message.Flags&Will_Flag == Will_Flag {
				client.Will = message.Will
				// TODO: consider QoS and Retain as broker need
			} else {

			}

			go self.RunClientTimer(client)
			err = self.Bt.SendMessage(NewConnackMessage(sessionPresent, Accepted), addr)
			client.IsConnecting = true
		case *PublishMessage:
			if !ok {
				EmitError(CLIENT_NOT_EXIST)
				continue
			}
			if message.QoS == 3 {
				EmitError(INVALID_QOS_3)
				self.DisconnectFromBroker(client)
				continue
			}

			if message.Dup {
				// re-delivered
			} else {
				// first time delivery
			}

			if message.Retain {
				// store tehe application message to designated topic
				data := string(message.Payload)
				if message.QoS == 0 && len(data) > 0 {
					// TODO: warnning, in this case data cannot be stored.
					// discard retained message
					data = ""
				}
				self.TopicRoot.ApplyRetain(message.TopicName, message.QoS, data)
			}

			switch message.QoS {
			// in any case, Dub must be 0
			case 0:
			case 1:
				err = client.SendMessage(NewPubackMessage(message.PacketID))
			case 2:
				err = client.SendMessage(NewPubrecMessage(message.PacketID))
			}
		case *PubackMessage:
			// acknowledge the sent Publish packet
			err = client.AckMessage(message.PacketID)
		case *PubrecMessage:
			// acknowledge the sent Publish packet
			err = client.AckMessage(message.PacketID)
			err = client.SendMessage(NewPubrelMessage(message.PacketID))
		case *PubrelMessage:
			// acknowledge the sent Pubrec packet
			err = client.AckMessage(message.PacketID)
			err = client.SendMessage(NewPubcompMessage(message.PacketID))
		case *PubcompMessage:
			// acknowledge the sent Pubrel packet
			err = client.AckMessage(message.PacketID)
		case *SubscribeMessage:
			// TODO: check The wild card is permitted
			if !ok {
				EmitError(CLIENT_NOT_EXIST)
			}
			returnCodes := make([]SubscribeReturnCode, 0)
			for _, subTopic := range message.SubscribeTopics {
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
								edge.FullPath, message.PacketID, []uint8(edge.RetainMessage)))
							// TODO: error validation
						}
					}
				}
				returnCodes = append(returnCodes, codes...)
			}
			// TODO: check whether the number of return codes are correct?
			err = client.SendMessage(NewSubackMessage(message.PacketID, returnCodes))
		case *UnsubscribeMessage:
			if !ok {
				EmitError(CLIENT_NOT_EXIST)
				continue // TODO: ?
			}
			if len(message.TopicNames) == 0 {
				// protocol violation
			}
			// TODO: optimize here
			result := []SubscribeTopic{}
			for _, t := range client.SubTopics {
				del := false
				for _, name := range message.TopicNames {
					if string(t.Topic) == string(name) {
						del = true
					}
				}
				if !del {
					result = append(result, t)
				}
			}
			client.SubTopics = result
			err = client.SendMessage(NewUnsubackMessage(message.PacketID))
		case *PingreqMessage:
			// Pingresp
			// TODO: calc elapsed time from previous pingreq.
			//       and store the time to duration of Transport
			if !ok {
				EmitError(CLIENT_NOT_EXIST)
				continue
			}
			err = client.SendMessage(NewPingrespMessage())
			client.ResetTimer()
			go self.RunClientTimer(client)
		case *DisconnectMessage:
			if !ok {
				EmitError(CLIENT_NOT_EXIST)
				continue
			}
			self.DisconnectFromBroker(client)
			// close the client
		default:
			// when invalid messages come
			err = INVALID_MESSAGE_CAME
		}
		if err != nil {
			EmitError(err)
		}
	}
}
