package core

// Operation represents a modifying backend storage operation, one of Create, Read, Update,Delete
//
type Operation string

// all supported database operations
const (
	OperationCreate Operation = "create"
	OperationRead   Operation = "read"
	OperationUpdate Operation = "update"
	OperationDelete Operation = "delete"
)

// Notifier is an interface to receive database notifications
type Notifier interface {
	Notify(resource string, operation Operation, payload []byte)
}
