package MQTTg

type Edge interface {
	recvConnectMessage(*ConnectMessage, *Client) error
	recvConnackMessage(*ConnackMessage, *Client) error
	recvPublishMessage(*PublishMessage, *Client) error
	recvPubackMessage(*PubackMessage, *Client) error
	recvPubrecMessage(*PubrecMessage, *Client) error
	recvPubrelMessage(*PubrelMessage, *Client) error
	recvPubcompMessage(*PubcompMessage, *Client) error
	recvSubscribeMessage(*SubscribeMessage, *Client) error
	recvSubackMessage(*SubackMessage, *Client) error
	recvUnsubscribeMessage(*UnsubscribeMessage, *Client) error
	recvUnsubackMessage(*UnsubackMessage, *Client) error
	recvPingreqMessage(*PingreqMessage, *Client) error
	recvPingrespMessage(*PingrespMessage, *Client) error
	recvDisconnectMessage(*DisconnectMessage, *Client) error
	ReadMessage() (Message, error)
}

func ReadLoop(edge Edge, c *Client) error {
	for {
		m, err := edge.ReadMessage()
		if err != nil {
			EmitError(err)
			continue
		}
		switch m := m.(type) {
		case *ConnectMessage:
			err = edge.recvConnectMessage(m, c)
		case *ConnackMessage:
			err = edge.recvConnackMessage(m, c)
		case *PublishMessage:
			err = edge.recvPublishMessage(m, c)
		case *PubackMessage:
			err = edge.recvPubackMessage(m, c)
		case *PubrecMessage:
			err = edge.recvPubrecMessage(m, c)
		case *PubrelMessage:
			err = edge.recvPubrelMessage(m, c)
		case *PubcompMessage:
			err = edge.recvPubcompMessage(m, c)
		case *SubscribeMessage:
			err = edge.recvSubscribeMessage(m, c)
		case *SubackMessage:
			err = edge.recvSubackMessage(m, c)
		case *UnsubscribeMessage:
			err = edge.recvUnsubscribeMessage(m, c)
		case *UnsubackMessage:
			err = edge.recvUnsubackMessage(m, c)
		case *PingreqMessage:
			err = edge.recvPingreqMessage(m, c)
		case *PingrespMessage:
			err = edge.recvPingrespMessage(m, c)
		case *DisconnectMessage:
			err = edge.recvDisconnectMessage(m, c)

		}
		EmitError(err)
	}
}
