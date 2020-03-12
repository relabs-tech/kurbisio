package iot

// MessagePublisher is an interface to publish MQTT message
type MessagePublisher interface {
	PublishMessageQ1(topic string, payload []byte)
}
