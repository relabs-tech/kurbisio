package core

import (
	"encoding/json"
	"fmt"
)

// Operation represents a modifying backend storage operation, one of Create, Read, Update, Delete, List
//
type Operation string

// all supported database operations
const (
	OperationCreate Operation = "create"
	OperationRead   Operation = "read"
	OperationUpdate Operation = "update"
	OperationDelete Operation = "delete"
	OperationList   Operation = "list"
)

// UnmarshalJSON is a custom JSON unmarshaller
func (o *Operation) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*o = Operation(s)
	switch *o {
	case OperationCreate, OperationRead, OperationUpdate, OperationDelete, OperationList:
		return nil
	default:
		return fmt.Errorf("%s is not valid Operation", s)
	}
}

// Notifier is an interface to receive database notifications
type Notifier interface {
	Notify(resource string, operation Operation, state string, payload []byte)
}
