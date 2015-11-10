package MQTTg

type Broker struct {
	Bt      *Transport
	Clients map[string]*Client // map[ClientIDs]*Client
	//Topics map
}

func (self *Broker) ReadLoop() error {
	for {
		m, err := self.Bt.ReadMessage()
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

			// CHECK: Is self.Bt needed?. Is nil enough?
			self.Clients[message.ClientID] = NewClient(self.Bt, message.ClientID,
				message.User, message.KeepAlive, message.Will)
			sessionPresent := false
			if message.Flags&CleanSession != CleanSession {
				// TODO: Is it better to check disconnect or not?
				_, exist := self.Clients[message.ClientID]
				//If the Server has stored Session state, it MUST set Session Present to 1
				if exist {
					sessionPresent = true
				}
			}
			Connack(sessionPresent, Accepted)
		case *ConnackMessage:
		case *PublishMessage:
			if message.Dup {
				// re-delivered
			} else if message.Dup {
				// first time delivery
			}

			switch message.QoS {
			case 0:
			case 1:
			case 2:
			case 3:
				// error
				// close connection
			}
		case *PubackMessage:
		case *PubrecMessage:
		case *PubrelMessage:
		case *PubcompMessage:
		case *SubscribeMessage:
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
