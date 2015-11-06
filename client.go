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
	Ct       *Transport
	ClientID string
	User     *User
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

