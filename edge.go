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
	disconnectProcessing() error
}
