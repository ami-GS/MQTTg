package MQTTg

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
	ReadMessage() (Message, error)
}

func ReadLoop(edge Edge, readChan chan Message) {
	for m := range readChan {
		var err error
		switch {
		case m != nil:
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
}
