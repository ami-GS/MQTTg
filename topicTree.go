package MQTTg

import (
	"strings"
)

type TopicNode struct {
	Nodes         map[string]*TopicNode
	FullPath      string
	RetainMessage string
	RetainQoS     uint8
	Subscribers   map[string]uint8 // map[clientID]QoS
}

func (self *TopicNode) GetNodesByNumberSign() (out []*TopicNode) {
	out = []*TopicNode{self}
	if len(self.Nodes) > 0 {
		for key, node := range self.Nodes {
			if strings.HasPrefix(key, "$") {
				continue
			}
			out = append(out, node.GetNodesByNumberSign()...)
		}
	}
	return out
}

func (self *TopicNode) GetTopicNodes(topic string) (out []*TopicNode, e error) {
	// this topic may have wildcard +*
	parts := strings.Split(topic, "/")
	nxt := self
	exist := false
	currentPath := ""
	for i, part := range parts {
		bef := nxt
		if i != len(parts)-1 {
			currentPath += part + "/"
		}

		if part == "+" {
			if i == len(parts)-1 {
				// e.g.) A/B/+
				for _, node := range bef.Nodes {
					out = append(out, node)
				}
			} else {
				// e.g.) A/+/C/D
				// TODO: optimize here
				for key, node := range bef.Nodes {
					if strings.HasPrefix(key, "$") {
						continue
					}
					tmp, err := node.GetTopicNodes(strings.Join(parts[i+1:], "/"))
					if err != nil {
						return nil, err
					}
					out = append(out, tmp...)
				}
				return out, nil
			}
		} else if part == "#" {
			if i != len(parts)-1 {
				return nil, MULTI_LEVEL_WILDCARD_MUST_BE_ON_TAIL
			}
			out = append(out, self.GetNodesByNumberSign()...)
		} else {
			if strings.HasSuffix(part, "#") && strings.HasSuffix(part, "+") {
				return nil, WILDCARD_MUST_NOT_BE_ADJACENT_TO_NAME
			}
			nxt, exist = bef.Nodes[part]
			if !exist {
				// TODO: this has bug through after case 'A/+/C/D'
				bef.ApplyNewTopic(part, currentPath)
				nxt, _ = bef.Nodes[part]
			}
			if i != len(parts)-1 {
				out = append(out, nxt)
			}

		}
	}
	return out, nil

}

func (self *TopicNode) ApplySubscriber(clientID, topic string, qos uint8) (map[string]string, SubscribeReturnCode, error) {
	// find topic edge and apply the clientID
	edges, err := self.GetTopicNodes(topic)
	if err != nil {
		return nil, SubscribeFailure, err
	}
	retains := make(map[string]string)
	for _, edge := range edges {
		edge.Subscribers[clientID] = qos
		if len(edge.RetainMessage) > 0 {
			// TODO: this is only for one topic,
			// this should be adjust for wildcard
			retains[topic] = edge.RetainMessage
		}
	}
	return retains, SubscribeReturnCode(qos), nil
}

func (self *TopicNode) DeleteSubscriber(clientID, topic string) error {
	edges, err := self.GetTopicNodes(topic)
	if err != nil {
		return err
	}
	for _, edge := range edges {
		delete(edge.Subscribers, clientID)
	}
	return nil
}

func (self *TopicNode) ApplyRetain(topic string, qos uint8, retain string) error {
	edges, err := self.GetTopicNodes(topic)
	if err != nil {
		return err
	}
	for _, edge := range edges {
		// for debug to store all retain mesage
		// edge.RetainMessage = append(edge.RetainMessage, retain)
		edge.RetainMessage = retain
		edge.RetainQoS = qos
	}

	return nil
}

func (self *TopicNode) ApplyNewTopic(topic, fullPath string) error {
	self.Nodes[topic] = &TopicNode{
		Nodes:         make(map[string]*TopicNode),
		FullPath:      fullPath,
		RetainMessage: "",
		RetainQoS:     0,
		Subscribers:   make(map[string]uint8),
	}
	return nil
}
