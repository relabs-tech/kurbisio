package core

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Operation represents a modifying backend storage operation, one of Create, Read, Update, Delete, List, Clear
//
type Operation string

// all supported database operations
const (
	OperationCreate Operation = "create"
	OperationRead   Operation = "read"
	OperationUpdate Operation = "update"
	OperationDelete Operation = "delete"
	OperationList   Operation = "list"
	OperationClear  Operation = "clear"
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

// Plural returns the plural form of the passed singular string.
//
// This is the algorithm used to create idiomatic REST routes
func Plural(singular string) string {
	if strings.HasSuffix(singular, "y") {
		return strings.TrimSuffix(singular, "y") + "ies"
	}
	if strings.HasSuffix(singular, "child") {
		return strings.TrimSuffix(singular, "child") + "children"
	}
	return singular + "s"

}

// PropertyNameToCanonicalHeader converts kurbisio JSON property names
// to their canonical header representation. Example: "content_type"
// becomes "Content-Type".
func PropertyNameToCanonicalHeader(property string) string {
	parts := strings.Split(property, "_")
	for i := 0; i < len(parts); i++ {
		s := parts[i]
		if len(s) == 0 {
			continue
		}
		s = strings.ToLower(s)
		runes := []rune(s)
		r := runes[0]
		if 'a' <= r && r <= 'z' {
			r += 'A' - 'a'
			runes[0] = r
		}
		parts[i] = string(runes)
	}
	return strings.Join(parts, "-")
}

// CanonicalHeaderToPropertyName converts canonical header names
// to kurbisio JSON property names. Example: "Content-Type"
// becomes "content_type".
func CanonicalHeaderToPropertyName(header string) string {
	return strings.ReplaceAll(strings.ToLower(header), "-", "_")
}
