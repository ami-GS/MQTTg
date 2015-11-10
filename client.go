package MQTTg

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
	ClientID  string
	User      *User
	KeepAlive uint16
	Will      *Will
}

func NewClient(t *Transport, id string, user *User, keepAlive uint16, will *Will) *Client {
	// TODO: when id is empty, then apply random
	return &Client{
		Ct:        t,
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
		m, err := self.Ct.ReadMessage()
		if err != nil {
			return err
		}
		switch message := m.(type) {
		case *ConnectMessage:
		case *ConnackMessage:
		case *PublishMessage:
			if message.Dup {
				// re-delivered
			} else if message.Dup {
				// first time delivery
			}

			switch message.QoS {
			case 0:
			case 1:
			case 2:
			case 3:
				// error
				// close connection
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
