package main

import (
	"github.com/ami-GS/MQTTg"
)

func main() {
	b := MQTTg.Broker{
		MyAddr:  nil,
		Clients: make(map[string]*MQTTg.BrokerSideClient),
		TopicRoot: &MQTTg.TopicNode{
			make(map[string]*MQTTg.TopicNode),
			"", "/", "", 0, make(map[string]uint8)},
	}
	b.Start()
}
