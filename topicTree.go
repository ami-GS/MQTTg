package MQTTg

import (
	"strings"
)

type TopicNode struct {
	Nodes map[string]*TopicNode
	//FullPath      string // if needed
	RetainMessage string
	Subscribers   map[string]uint8 // map[clientID]QoS
}

func (self *TopicNode) GetTopicNodes(topic string) []*TopicNode {
	// this topic may have wildcard +*
	parts := strings.Split(topic, "/")
	nxt := self
	for i, part := range parts {
		// TODO: this is only for one topic, this should be changed
		bef := nxt
		nxt, exist := bef.Nodes[part]
		if !exist && i == len(parts)-1 {
			bef.ApplyNewTopic(part)
			nxt, _ = bef.Nodes[part]
		} else if !exist {
			// not exist topic
			return nil // TODO: this shuld return specific error
		}
	}
	return []*TopicNode{nxt}

}

func (self *TopicNode) ApplySubscriber(clientID, topic string, qos uint8) (map[string]string, SubscribeReturnCode) {
	// find topic edge and apply the clientID
	edges := self.GetTopicNodes(topic)
	retains := make(map[string]string)
	for _, edge := range edges {
		edge.Subscribers[clientID] = qos
		if len(edge.RetainMessage) > 0 {
			// TODO: this is only for one topic,
			// this should be adjust for wildcard
			retains[topic] = edge.RetainMessage
		}
	}
	return retains, SubscribeReturnCode(qos)
}

func (self *TopicNode) ApplyRetain(topic, retain string) error {
	edges := self.GetTopicNodes(topic)
	for _, edge := range edges {
		// for debug to store all retain mesage
		// edge.RetainMessage = append(edge.RetainMessage, retain)
		edge.RetainMessage = retain
	}

	return nil
}

func (self *TopicNode) ApplyNewTopic(topic string) error {
	self.Nodes[topic] = &TopicNode{
		Nodes:         make(map[string]*TopicNode),
		RetainMessage: "",
		Subscribers:   make(map[string]uint8),
	}
	return nil
}
