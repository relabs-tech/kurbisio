package core

// Operation represents a database operation, one of Create,Update,Delete
type Operation string

// all supported database operations
const (
	OperationCreate Operation = "create"
	OperationRead   Operation = "read"
	OperationUpdate Operation = "update"
	OperationDelete Operation = "delete"
	OperationList   Operation = "list"
)

// Notifier is an interface to receive database notifications
type Notifier interface {
	Notify(resource string, operation Operation, payload []byte)
}
