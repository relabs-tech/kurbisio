// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package iot

// MessagePublisher is an interface to publish MQTT message
type MessagePublisher interface {
	PublishMessageQ1(topic string, payload []byte)
}
