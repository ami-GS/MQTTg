package MQTTg

import (
	"testing"
)

var Topics []string = []string{
	"a/b/c/d/e", "aa/b/c/d/e", "a/bb/c/d/e", "a/b/cc/d/e", "a/b/c/dd/e",
	"a/b/c/d/ee", "aa/bb/c/d/e", "aa/b/cc/d/e", "aa/b/c/dd/e", "aa/b/c/d/ed",
	"a/bb/cc/d/e", "a/bb/c/dd/e", "a/bb/c/d/ee", "a/b/cc/dd/e", "a/b/cc/d/ee",
	"a/b/c/dd/ee", "aa/bb/cc/d/e", "aa/bb/c/dd/e", "aa/bb/c/d/ee", "aa/b/cc/dd/e",
	"aa/b/cc/d/ee", "aa/b/c/dd/ee", "a/bb/cc/dd/e", "a/bb/cc/d/ee", "a/bb/c/dd/ee",
	"a/b/cc/dd/ee", "aa/bb/cc/dd/e", "aa/bb/cc/d/ee", "aa/bb/c/dd/ee",
	"aa/b/cc/dd/ee", "a/bb/cc/dd/ee", "aa/bb/cc/dd/ee",
}

func TestGetTopicNodes(t *testing.T) {
	// This might include ApplyNewTopic, GetNodeByNumberSign
	root := TopicNode{
		make(map[string]*TopicNode),
		"",
		"",
		0,
		make(map[string]uint8),
	}
	for _, topic := range Topics {
		// set topics
		root.GetTopicNodes(topic, true)
	}

	searchTopics := []string{
		"a/b/c/d/e", "a/b/c/d/+", "a/b/+/d/e",
		"a/+/c/+/e", "a/b/c/d/#", "a/b/+/d/#",
	}
	e_topics := [][]string{
		[]string{"a/b/c/d/e"},
		[]string{"a/b/c/d/e", "a/b/c/d/ee"},
		[]string{"a/b/c/d/e", "a/b/cc/d/e"},
		[]string{"a/b/c/d/e", "a/bb/c/d/e", "a/bb/c/dd/e", "a/b/c/dd/e"},
		[]string{"a/b/c/d", "a/b/c/d/e", "a/b/c/d/ee"},
		[]string{"a/b/c/d", "a/b/cc/d", "a/b/c/d/e", "a/b/cc/d/e", "a/b/c/d/ee", "a/b/cc/d/ee"},
	}

	for i, topic := range searchTopics {
		topicNodes, _ := root.GetTopicNodes(topic, true)
		if len(topicNodes) != len(e_topics[i]) {
			t.Errorf("got %v\nwant %v", len(topicNodes), len(e_topics[i]))
			//continue
		}
		pass := true
		for _, e_topic := range e_topics[i] {
			pass = false
			var node *TopicNode
			for _, node = range topicNodes {
				if node.FullPath == e_topic {
					pass = true
				}
			}
			if !pass {
				t.Errorf("got '%s'\nnot in %v", node.FullPath, e_topics[i])
			}
		}
	}

}
