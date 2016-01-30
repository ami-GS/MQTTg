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
	self.FullPath = strings.TrimSuffix(self.FullPath, "/")
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

func (self *TopicNode) GetTopicNodes(topic string, addNewNodes bool) (out []*TopicNode, e error) {
	// this topic may have wildcard +*
	parts := strings.Split(topic, "/")
	nxt := self
	ok := false
	currentPath := ""
	for i, part := range parts {
		bef := nxt
		switch part {
		case "+":
			// e.g.) A/+/C/D
			for key, _ := range bef.Nodes {
				if strings.HasPrefix(key, "$") {
					continue
				}
				tmp, err := self.GetTopicNodes(strings.Replace(topic, "+", key, 1), addNewNodes)
				if err != nil {
					return nil, err
				}
				out = append(out, tmp...)
			}
			return out, nil
		case "#":
			if i != len(parts)-1 {
				return nil, MULTI_LEVEL_WILDCARD_MUST_BE_ON_TAIL
			}
			out = append(out, bef.GetNodesByNumberSign()...)
		default:
			if strings.HasSuffix(part, "#") || strings.HasSuffix(part, "+") {
				return nil, WILDCARD_MUST_NOT_BE_ADJACENT_TO_NAME
			}
			currentPath += part
			if i != len(parts)-1 {
				currentPath += "/"
			}
			nxt, ok = bef.Nodes[part]
			if !ok && addNewNodes {
				bef.ApplyNewTopic(part, currentPath)
				nxt, _ = bef.Nodes[part]
			}
			if len(parts)-1 == i && nxt != nil {
				out = append(out, nxt)
			}
		}
	}
	return out, nil
}

func (self *TopicNode) ApplySubscriber(clientID, topic string, qos uint8) ([]*TopicNode, []SubscribeReturnCode, error) {
	// find topic edge and apply the clientID
	edges, err := self.GetTopicNodes(topic, true)
	if err != nil {
		return nil, []SubscribeReturnCode{SubscribeFailure}, err
	}
	codes := make([]SubscribeReturnCode, len(edges))
	for i, edge := range edges {
		// TODO: the return code should be managed by broker
		edge.Subscribers[clientID] = qos
		codes[i] = SubscribeReturnCode(qos)
	}
	return edges, codes, nil
}

func (self *TopicNode) DeleteSubscriber(clientID, topic string) error {
	edges, err := self.GetTopicNodes(topic, false)
	if err != nil {
		return err
	}
	for _, edge := range edges {
		delete(edge.Subscribers, clientID)
	}
	return nil
}

func (self *TopicNode) ApplyRetain(topic string, qos uint8, retain string) error {
	edges, err := self.GetTopicNodes(topic, true)
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

func (self *TopicNode) ApplyNewTopic(topic, fullPath string) {
	self.Nodes[topic] = &TopicNode{
		Nodes:         make(map[string]*TopicNode),
		FullPath:      fullPath,
		RetainMessage: "",
		RetainQoS:     0,
		Subscribers:   make(map[string]uint8),
	}
}

func (self *TopicNode) DumpTree() (str string) {
	if len(self.Nodes) == 0 {
		return self.FullPath + "\n"
	}
	for _, v := range self.Nodes {
		str += v.DumpTree()
	}
	return str

}
