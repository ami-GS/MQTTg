package MQTTg

type Broker struct {
	Bt      *Transport
	Clients map[string]*Client // map[ClientIDs]*Client
	//Topics map
}
}
