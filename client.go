package MQTTg

import (
	"net"
	"strconv"
	"strings"
	"time"
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
	Ct           *Transport
	IsConnecting bool
	Addr         *net.UDPAddr
	RemoteAddr   *net.UDPAddr
	ID           string
	User         *User
	KeepAlive    uint16
	Will         *Will
	SubTopics    []SubscribeTopic
	PingBegin    time.Time
}

func NewClient(t *Transport, addr *net.UDPAddr, id string, user *User, keepAlive uint16, will *Will) *Client {
	// TODO: when id is empty, then apply random
	return &Client{
		Ct:           t,
		IsConnecting: false,
		Addr:         addr,
		ID:           id,
		User:         user,
		KeepAlive:    keepAlive,
		Will:         will,
		SubTopics:    make([]SubscribeTopic, 0),
	}
}

func (self *Client) SendMessage(m Message) error {
	if !self.IsConnecting {
		return nil // TODO: apply error. like NOT_CONNECTED
	}
	err := self.Ct.SendMessage(m, self.RemoteAddr)
	return err
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

func (self *Client) Connect(addPair string) error {
	pair := strings.Split(addPair, ":")
	if len(pair) != 2 {
		return nil // TODO: apply error
	}
	port, err := strconv.Atoi(pair[1])
	if err != nil {
		return err
	}
	udpaddr := &net.UDPAddr{
		IP:   net.IP(pair[0]),
		Port: port,
		Zone: "", // TODO: check
	}
	// TODO: local address might be input
	conn, err := net.DialUDP("udp4", nil, udpaddr)
	if err != nil {
		return err
	}
	self.RemoteAddr = udpaddr
	self.Ct.conn = conn
	self.SendMessage(NewConnectMessage(10, "dummyID",
		false, nil, NewUser("name", "pass")))
	return nil
}

func (self *Client) Subsclibe(topics []SubscribeTopic) error {
	// TODO: id should be considered
	err := self.SendMessage(NewSubscribeMessage(0, topics))
	if err == nil {
		self.SubTopics = append(self.SubTopics, topics...)
	}
	return err
}

func (self *Client) Unsubscribe(topics [][]uint8) error {
	for _, t := range self.SubTopics {
		exist := false
		for _, name := range topics {
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
	err := self.SendMessage(NewUnsubscribeMessage(0, topics))
	return err
}

func (self *Client) Ping() error {
	err := self.SendMessage(NewPingreqMessage())
	if err == nil {
		self.PingBegin = time.Now()
	}
	return err
}

func (self *Client) Disconnect() error {
	if !self.IsConnecting {
		return nil // TODO: apply error. like NOT_CONNECTED
	}
	err := self.SendMessage(NewDisconnectMessage())
	if err != nil {
		return err
	}
	err = self.Ct.conn.Close()
	self.IsConnecting = false
	return err
}

func (self *Client) ReadLoop() error {
	for {
		m, addr, err := self.Ct.ReadMessageFrom()
		if err != nil {
			return err
		}
		// TODO: the comparison should be deepequal?
		if self.RemoteAddr != addr {
			continue
		}
		switch message := m.(type) {
		case *ConnackMessage:
			self.IsConnecting = true
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
				self.SendMessage(NewPubackMessage(message.PacketID))
			case 2:
				self.SendMessage(NewPubrecMessage(message.PacketID))
			}

		case *PubackMessage:
			// acknowledge the sent Publish packet
		case *PubrecMessage:
			// acknowledge the sent Publish packet
			self.SendMessage(NewPubrelMessage(message.PacketID))
		case *PubrelMessage:
			self.SendMessage(NewPubcompMessage(message.PacketID))
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
			elapsed := time.Since(self.PingBegin)
		default:
			// when invalid messages come
		}
	}
}
