package MQTTg

import (
	"net"
)

type Edge interface {
	recvConnectMessage(*ConnectMessage, *net.UDPAddr) error
	recvConnackMessage(*ConnackMessage, *net.UDPAddr) error
	recvPublishMessage(*PublishMessage, *net.UDPAddr) error
	recvPubackMessage(*PubackMessage, *net.UDPAddr) error
	recvPubrecMessage(*PubrecMessage, *net.UDPAddr) error
	recvPubrelMessage(*PubrelMessage, *net.UDPAddr) error
	recvPubcompMessage(*PubcompMessage, *net.UDPAddr) error
	recvSubscribeMessage(*SubscribeMessage, *net.UDPAddr) error
	recvSubackMessage(*SubackMessage, *net.UDPAddr) error
	recvUnsubscribeMessage(*UnsubscribeMessage, *net.UDPAddr) error
	recvUnsubackMessage(*UnsubackMessage, *net.UDPAddr) error
	recvPingreqMessage(*PingreqMessage, *net.UDPAddr) error
	recvPingrespMessage(*PingrespMessage, *net.UDPAddr) error
	recvDisconnectMessage(*DisconnectMessage, *net.UDPAddr) error
	ReadMessageFrom() (Message, *net.UDPAddr, error)
}

func ReadLoop(edge Edge) error {
	for {
		m, addr, err := edge.ReadMessageFrom()
		if err != nil {
			EmitError(err)
			continue
		}
		switch m := m.(type) {
		case *ConnectMessage:
			err = edge.recvConnectMessage(m, addr)
		case *ConnackMessage:
			err = edge.recvConnackMessage(m, addr)
		case *PublishMessage:
			err = edge.recvPublishMessage(m, addr)
		case *PubackMessage:
			err = edge.recvPubackMessage(m, addr)
		case *PubrecMessage:
			err = edge.recvPubrecMessage(m, addr)
		case *PubrelMessage:
			err = edge.recvPubrelMessage(m, addr)
		case *PubcompMessage:
			err = edge.recvPubcompMessage(m, addr)
		case *SubscribeMessage:
			err = edge.recvSubscribeMessage(m, addr)
		case *SubackMessage:
			err = edge.recvSubackMessage(m, addr)
		case *UnsubscribeMessage:
			err = edge.recvUnsubscribeMessage(m, addr)
		case *UnsubackMessage:
			err = edge.recvUnsubackMessage(m, addr)
		case *PingreqMessage:
			err = edge.recvPingreqMessage(m, addr)
		case *PingrespMessage:
			err = edge.recvPingrespMessage(m, addr)
		case *DisconnectMessage:
			err = edge.recvDisconnectMessage(m, addr)

		}
		EmitError(err)
	}
}
