package MQTTg

import (
	"net"
)

type Broker struct {
	Bt *Transport
	// TODO: check whether not good to use addr as key
	Clients   map[*net.UDPAddr]*Client // map[addr]*Client
	TopicRoot *TopicNode
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
				Connack(false, UnacceptableProtocolVersion)
				continue
			}
			//if message.ClientIDs {} // TODO:check ID's validatino
			// Connack(false, IdentifierRejected)
			// continue

			//if self.status?
			// Connack(false, ServerUnavailable)
			// continue

			// if message.User.Name and if message.User.Password
			// Connack(false, BadUserNameOrPassword)
			// continue

			// if authorized
			// Connack(false, NotAuthorized)
			// continue

			// CHECK: Is self.Bt needed?. Is nil enough?
			self.Clients[addr] = NewClient(self.Bt, addr, message.ClientID,
				message.User, message.KeepAlive, message.Will)
			sessionPresent := false
			if message.Flags&CleanSession != CleanSession {
				// TODO: Is it better to check disconnect or not?
				_, exist := self.Clients[addr]
				//If the Server has stored Session state, it MUST set Session Present to 1
				if exist {
					sessionPresent = true
				}
			}
			Connack(sessionPresent, Accepted)
		case *ConnackMessage:
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
				Puback(message.PacketID)
			case 2:
				Pubrec(message.PacketID)
			}

		case *PubackMessage:
			// acknowledge the sent Publish packet
		case *PubrecMessage:
			// acknowledge the sent Publish packet
			Pubrel(message.PacketID)
		case *PubrelMessage:
			Pubcomp(message.PacketID)
		case *PubcompMessage:
			// acknowledge the sent Pubrel packet
		case *SubscribeMessage:
			// TODO: check The wild card is permitted
			codes := make([]SubscribeReturnCode, len(message.SubscribeTopics))
			for i, subTopic := range message.SubscribeTopics {
				// TODO: need to validate wheter there are same topics or not
				client, ok := self.Clients[addr]
				if ok {
					retains, code := self.TopicRoot.ApplySubscriber(client.ClientID, string(subTopic.Topic), subTopic.QoS)
					codes[i] = code
					client.AckSubscribeTopic(subTopic, code)
					if len(retains) > 0 {
						for k, v := range retains {
							//Publish(k,v)
						}
					}
				} else {
					codes[i] = SubscribeFailure // TODO: correct?
				}

			}
			Suback(message.PacketID, codes)
		case *SubackMessage:
		case *UnsubscribeMessage:
		case *UnsubackMessage:
		case *PingreqMessage:
			// Pingresp
		case *PingrespMessage:
		case *DisconnectMessage:
		}
	}
}
