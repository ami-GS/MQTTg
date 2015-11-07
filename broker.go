package MQTTg

type Broker struct {
	Bt        *Transport
	ClientIDs []string
	Users     map[string]*User // map[ClientIDs]*User
}
