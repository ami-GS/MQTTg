package main

import (
	"github.com/ami-GS/MQTTg"
	"time"
)

func main() {
	c := MQTTg.NewClient("GS-ID", &MQTTg.User{"daiki", "passwd"},
		10, &MQTTg.Will{"w-topic", "w-message", false, 1})

	c.Connect("10.150.0.47:8883", false)
	time.Sleep(1 * time.Second)
	c.Publish("p-topic", "p-data", 1, true)
	time.Sleep(1 * time.Second)
	s := []*MQTTg.SubscribeTopic{&MQTTg.SubscribeTopic{0, "p-topic", 1}}
	s = append(s, &MQTTg.SubscribeTopic{0, "s-topic", 1})
	c.Subscribe(s)
	time.Sleep(1 * time.Second)
	time.Sleep(2 * time.Second)
}
