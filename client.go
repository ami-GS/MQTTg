package MQTTg

import (
	"net"
)

type User struct {
	Name   string
	Passwd string
}

func NewUser(name, pass string) *User {
	return &User{
		Name:   name,
		Passwd: pass,
	}
}

type Client struct {
	Ct        *Transport
	Addr      *net.UDPAddr
	ID        string
	User      *User
	KeepAlive uint16
	Will      *Will
	SubTopics []SubscribeTopic
}

func NewClient(t *Transport, addr *net.UDPAddr, id string, user *User, keepAlive uint16, will *Will) *Client {
	// TODO: when id is empty, then apply random
	return &Client{
		Ct:        t,
		Addr:      addr,
		ID:        id,
		User:      user,
		KeepAlive: keepAlive,
		Will:      will,
		SubTopics: make([]SubscribeTopic, 0),
	}
}

func (self *Client) AckSubscribeTopic(order int, code SubscribeReturnCode) error {
	if code != SubscribeFailure {
		self.SubTopics[order].QoS = uint8(code)
		self.SubTopics[order].State = SubscribeAck
	} else {
		//failed
	}
	return nil
}

func (self *Client) Subsclibe(topics []SubscribeTopic) error {
	// TODO: id should be considered
	sub := NewSubscribeMessage(0, topics)
	err := self.Ct.SendMessage(sub)
	if err == nil {
		self.SubTopics = append(self.SubTopics, topics...)
	}
	return err
}

func (self *Client) Unsubscribe(topics [][]uint8) error {
	for i, t := range self.SubTopics {
		exist := false
		for j, name := range topics {
			if string(t.Topic) == string(name) {
				exist = true
			}
		}
		if exist {
			t.State = UnSubscribeNonAck
		} else {
			// error? or warnning
			// return error
		}
	}
	// id should be conidered
	unsub := NewUnsubscribeMessage(0, topics)
	err := self.Ct.SendMessage(unsub)
	return err
}

func (self *Client) Ping() error {
	ping := NewPingreqMessage()
	err := self.Ct.SendMessage(ping)
	return err
}

func (self *Client) Disconnect() error {
	dc := NewDisconnectMessage()
	err := self.Ct.SendMessage(dc)
	// TODO: close connection
	return err
}

func (self *Client) ReadLoop() error {
	for {
		m, _, err := self.Ct.ReadMessageFrom()
		if err != nil {
			return err
		}
		switch message := m.(type) {
		case *ConnackMessage:
		case *PublishMessage:
			if message.QoS == 3 {
				// error
				// close connection
				continue
			}

			if message.Dup {
				// re-delivered
			} else if message.Dup {
				// first time delivery
			}

			if message.Retain {
				// retained message comes
			} else {
				// non retained message
			}

			switch message.QoS {
			// in any case, Dub must be 0
			case 0:
			case 1:
				Puback(message.PacketID)
			case 2:
				Pubrec(message.PacketID)
			}

		case *PubackMessage:
			// acknowledge the sent Publish packet
		case *PubrecMessage:
			// acknowledge the sent Publish packet
			Pubrel(message.PacketID)
		case *PubrelMessage:
			Pubcomp(message.PacketID)
		case *PubcompMessage:
			// acknowledge the sent Pubrel packet
		case *SubackMessage:
			// acknowledge the sent subscribe packet
			for i, code := range message.ReturnCodes {
				_ = self.AckSubscribeTopic(i, code)
			}
		case *UnsubackMessage:
			// acknowledged the sent unsubscribe packet
		case *PingrespMessage:
		default:
			// when invalid messages come
		}
	}
}
