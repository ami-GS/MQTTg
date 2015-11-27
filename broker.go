package MQTTg

import (
	"net"
	"strconv"
)

type Broker struct {
	Bt *Transport
	// TODO: check whether not good to use addr as key
	Clients   map[*net.UDPAddr]*Client // map[addr]*Client
	ClientIDs map[string]*Client       // map[clientID]*Client
	TopicRoot *TopicNode
}

func (self *Broker) ApplyDummyClientID() string {
	return "DummyClientID:" + strconv.Itoa(len(self.ClientIDs)+1)
}

func (self *Broker) ReadLoop() error {
	for {
		m, addr, err := self.Bt.ReadMessageFrom()
		if err != nil {
			return err
		}
		switch message := m.(type) {
		case *ConnectMessage:
			if message.Protocol.Level != MQTT_3_1_1.Level {
				// CHECK: Is false correct?
				// self.Bt.SendMessage(NewConnackMessage(false, UnacceptableProtocolVersion), addr)
				continue
			}
			//if message.ClientIDs {} // TODO:check ID's validatino
			// self.Bt.SendMessage(NewConnackMessage(false, IdentifierRejected), addr)
			// continue

			//if self.status?
			// self.Bt.SendMessage(NewConnackMessage(false, ServerUnavailable), addr)
			// self.Connack(false, ServerUnavailable)
			_, ok := self.ClientIDs[message.ClientID]
			if ok {
				// TODO: this might cause problem
				self.Bt.SendMessage(NewConnackMessage(false, IdentifierRejected), addr)
				EmitError(CLIENT_ID_IS_USED_ALREADY)
				continue
			}
			if len(message.ClientID) == 0 {
				message.ClientID = self.ApplyDummyClientID()
			}

			// if message.User.Name and if message.User.Password
			// self.Bt.SendMessage(NewConnackMessage(false, BadUserNameOrPassword), addr)
			// continue

			// if authorized
			// self.Bt.SendMessage(NewConnackMessage(false, NotAuthorized), addr)
			// continue

			// CHECK: Is self.Bt needed?. Is nil enough?

			client, ok := self.Clients[addr]
			sessionPresent := false
			if message.Flags&CleanSession_Flag == CleanSession_Flag || !ok {
				// TODO: need to manage QoS base processing
				client := NewClient(self.Bt, addr, message.ClientID,
					message.User, message.KeepAlive, message.Will)
				self.Clients[addr] = client
				self.ClientIDs[message.ClientID] = client
			} else if message.Flags&CleanSession_Flag != CleanSession_Flag || ok {
				sessionPresent = true
			}
			self.Bt.SendMessage(NewConnackMessage(sessionPresent, Accepted), addr)
		case *PublishMessage:
			if message.QoS == 3 {
				// error
				// close connection
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
				self.Bt.SendMessage(NewPubackMessage(message.PacketID), addr)
			case 2:
				self.Bt.SendMessage(NewPubrecMessage(message.PacketID), addr)
			}
		case *PubackMessage:
			// acknowledge the sent Publish packet
		case *PubrecMessage:
			// acknowledge the sent Publish packet
			self.Bt.SendMessage(NewPubrelMessage(message.PacketID), addr)
		case *PubrelMessage:
			self.Bt.SendMessage(NewPubcompMessage(message.PacketID), addr)
		case *PubcompMessage:
			// acknowledge the sent Pubrel packet
		case *SubscribeMessage:
			// TODO: check The wild card is permitted
			codes := make([]SubscribeReturnCode, len(message.SubscribeTopics))
			for i, subTopic := range message.SubscribeTopics {
				// TODO: need to validate wheter there are same topics or not
				client, ok := self.Clients[addr]
				if ok {
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
				} else {
					codes[i] = SubscribeFailure // TODO: correct?
				}

			}
			self.Bt.SendMessage(NewSubackMessage(message.PacketID, codes), addr)
		case *UnsubscribeMessage:
			client, ok := self.Clients[addr]
			if !ok {
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
			self.Bt.SendMessage(NewUnsubackMessage(message.PacketID), addr)
		case *PingreqMessage:
			// Pingresp
			self.Bt.SendMessage(NewPingrespMessage(), addr)
		case *DisconnectMessage:
			client, ok := self.Clients[addr]
			if !ok {
				// TODO: emit error/warnning
				continue
			}
			client.IsConnecting = false

			// close the client
			// MUST discard WILL message
			// associate with the connection
		default:
			// when invalid messages come
		}
	}
}
