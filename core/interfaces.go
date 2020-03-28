package core

// Operation represents a database operation, one of Create,Update,Delete
type Operation string

// all supported database operations
const (
	OperationCreate Operation = "create"
	OperationUpdate Operation = "update"
	OperationDelete Operation = "delete"
)

// Notifier is an interface to receive database notifications
type Notifier interface {
	Notify(resource string, operation Operation, payload []byte)
}
