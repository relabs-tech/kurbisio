// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package core

// Notifier is an interface to receive database notifications
type Notifier interface {
	Notify(resource string, operation Operation, payload []byte)
}
