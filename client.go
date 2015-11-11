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
	ClientID  string
	User      *User
	KeepAlive uint16
	Will      *Will
}

func NewClient(t *Transport, addr *net.UDPAddr, id string, user *User, keepAlive uint16, will *Will) *Client {
	// TODO: when id is empty, then apply random
	return &Client{
		Ct:        t,
		Addr:      addr,
		ClientID:  id,
		User:      user,
		KeepAlive: keepAlive,
		Will:      will,
	}
}

func (self *Client) Publish(dup bool, qos uint8, retain bool, topic string, data string) error {
	// TODO: id shold be considered
	pub := NewPublishMessage(dup, qos, retain, topic, 0, []uint8(data))
	err := self.Ct.SendMessage(pub)
	return err
}

func (self *Client) Subsclibe(topics []SubscribeTopic) error {
	// TODO: id should be considered
	sub := NewSubscribeMessage(0, topics)
	err := self.Ct.SendMessage(sub)
	return err
}

func (self *Client) Disconnect() error {
	dc := NewDisconnectMessage()
	err := self.Ct.SendMessage(dc)
	return err
}

func (self *Client) ReadLoop() error {
	for {
		m, _, err := self.Ct.ReadMessageFrom()
		if err != nil {
			return err
		}
		switch message := m.(type) {
		case *ConnectMessage:
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
			case 0:
			case 1:
				Puback()
			case 2:
				Pubrec()
			}

		case *PubackMessage:
		case *PubrecMessage:
		case *PubrelMessage:
		case *PubcompMessage:
		case *SubscribeMessage:
		case *SubackMessage:
		case *UnsubscribeMessage:
		case *UnsubackMessage:
		case *PingreqMessage:
		case *PingrespMessage:
		case *DisconnectMessage:
		}
	}
}
