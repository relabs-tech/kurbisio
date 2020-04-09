package core

// Notifier is an interface to receive database notifications
type Notifier interface {
	Notify(resource string, operation Operation, state string, payload []byte)
}
