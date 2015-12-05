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

func (self *ClientInfo) RunTimer() {
	<-self.KeepAliveTimer.C
	self.DisconnectFromBroker()
	// TODO: logging?
}

func (self *ClientInfo) DisconnectFromBroker() {
	self.Will = nil
	self.IsConnecting = false
	self.KeepAliveTimer.Stop()
	// TODO: free used clientID due to clean session?
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
			sessionPresent := false
			if message.Flags&CleanSession_Flag == CleanSession_Flag || !ok {
				// TODO: need to manage QoS base processing
				duration := time.Duration(float32(message.KeepAlive) * 100000000 * 1.5)
				client = NewClientInfo(NewClient(self.Bt, addr, message.ClientID,
					message.User, message.KeepAlive, message.Will), duration)

				self.Clients[addr] = client
				self.ClientIDs[message.ClientID] = client
			} else if message.Flags&CleanSession_Flag != CleanSession_Flag || ok {
				sessionPresent = true
			}

			if message.Flags&Will_Flag == Will_Flag {
				client.Will = message.Will
				// TODO: consider QoS and Retain as broker need
			} else {

			}

			go client.RunTimer()
			err = self.Bt.SendMessage(NewConnackMessage(sessionPresent, Accepted), addr)
		case *PublishMessage:
			if !ok {
				EmitError(CLIENT_NOT_EXIST)
				continue
			}
			if message.QoS == 3 {
				EmitError(INVALID_QOS_3)
				client.DisconnectFromBroker()
				continue
			}

			if message.Dup {
				// re-delivered
			} else {
				// first time delivery
			}

			if message.Retain {
				// store the application message to designated topic
				if len(message.Payload) == 0 {
					// discard(remove) retained message
				}

				if message.QoS == 0 {
					// discard retained message
				}
			}

			switch message.QoS {
			// in any case, Dub must be 0
			case 0:
			case 1:
				err = self.Bt.SendMessage(NewPubackMessage(message.PacketID), addr)
			case 2:
				err = self.Bt.SendMessage(NewPubrecMessage(message.PacketID), addr)
			}
		case *PubackMessage:
			// acknowledge the sent Publish packet
		case *PubrecMessage:
			// acknowledge the sent Publish packet
			err = self.Bt.SendMessage(NewPubrelMessage(message.PacketID), addr)
		case *PubrelMessage:
			err = self.Bt.SendMessage(NewPubcompMessage(message.PacketID), addr)
		case *PubcompMessage:
			// acknowledge the sent Pubrel packet
		case *SubscribeMessage:
			// TODO: check The wild card is permitted
			codes := make([]SubscribeReturnCode, len(message.SubscribeTopics))
			if !ok {
				EmitError(CLIENT_NOT_EXIST)
				for i, _ := range message.SubscribeTopics {
					codes[i] = SubscribeFailure
				}
			} else {
				for i, subTopic := range message.SubscribeTopics {
					// TODO: need to validate wheter there are same topics or not
					retains, code := self.TopicRoot.ApplySubscriber(client.ID, string(subTopic.Topic), subTopic.QoS)
					codes[i] = code
					client.SubTopics = append(client.SubTopics,
						SubscribeTopic{SubscribeAck,
							subTopic.Topic,
							uint8(code),
						})
					if len(retains) > 0 {
						for k, v := range retains {
							//Publish(k,v)
						}
					}
				}
			}
			err = self.Bt.SendMessage(NewSubackMessage(message.PacketID, codes), addr)
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
			err = self.Bt.SendMessage(NewUnsubackMessage(message.PacketID), addr)
		case *PingreqMessage:
			// Pingresp
			// TODO: calc elapsed time from previous pingreq.
			//       and store the time to duration of Transport
			if !ok {
				EmitError(CLIENT_NOT_EXIST)
				continue
			}
			err = self.Bt.SendMessage(NewPingrespMessage(), addr)
			client.ResetTimer()
			go client.RunTimer()
		case *DisconnectMessage:
			if !ok {
				EmitError(CLIENT_NOT_EXIST)
				continue
			}
			client.DisconnectFromBroker()
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
