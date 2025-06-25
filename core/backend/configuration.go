// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend

import (
	"github.com/goccy/go-json"

	"github.com/relabs-tech/kurbisio/core/access"
)

// AuditLog represents the type of audit log operation
type AuditLog string

const (
	AuditLogCreate AuditLog = "create"
	AuditLogRead   AuditLog = "read"
	AuditLogUpdate AuditLog = "update"
	AuditLogDelete AuditLog = "delete"
	AuditLogClear  AuditLog = "clear"
)

// String returns the string representation of the AuditLog
func (a AuditLog) String() string {
	return string(a)
}

// IsValid checks if the AuditLog value is valid
func (a AuditLog) IsValid() bool {
	switch a {
	case AuditLogCreate, AuditLogRead, AuditLogUpdate, AuditLogDelete, AuditLogClear:
		return true
	default:
		return false
	}
}

// AllAuditLogs returns all valid audit log types
func AllAuditLogs() []AuditLog {
	return []AuditLog{
		AuditLogCreate,
		AuditLogRead,
		AuditLogUpdate,
		AuditLogDelete,
		AuditLogClear,
	}
}

// Configuration holds a complete backend configuration
type Configuration struct {
	Collections []CollectionConfiguration `json:"collections"`
	Singletons  []SingletonConfiguration  `json:"singletons"`
	Blobs       []BlobConfiguration       `json:"blobs"`
	Relations   []RelationConfiguration   `json:"relations"`
	Shortcuts   []ShortcutConfiguration   `json:"shortcuts"`
}

// CollectionConfiguration describes a collection resource
type CollectionConfiguration struct {
	Resource                      string          `json:"resource"`
	ExternalIndex                 string          `json:"external_index"`
	StaticProperties              []string        `json:"static_properties"`
	SearchableProperties          []string        `json:"searchable_properties"`
	Permits                       []access.Permit `json:"permits"`
	Description                   string          `json:"description"`
	SchemaID                      string          `json:"schema_id"`
	Default                       json.RawMessage `json:"default"`
	WithCompanionFile             bool            `json:"with_companion_file"`
	CompanionPresignedURLValidity int             `json:"companion_presigned_url_validity"`
	needsKSS                      bool            // true of this collection or any subcollection or subblob needs kss
	AuditLogs                     []AuditLog      `json:"audit_logs"` // list of audit log names to use for this collection
}

// SingletonConfiguration describes a singleton resource
type SingletonConfiguration struct {
	Resource             string          `json:"resource"`
	Permits              []access.Permit `json:"permits"`
	Description          string          `json:"description"`
	SchemaID             string          `json:"schema_id"`
	StaticProperties     []string        `json:"static_properties"`
	SearchableProperties []string        `json:"searchable_properties"`
	Default              json.RawMessage `json:"default"`
	AuditLogs            []AuditLog      `json:"audit_logs"` // list of audit log names to use for this collection
}

// BlobConfiguration describes a blob collection resource
type BlobConfiguration struct {
	Resource             string          `json:"resource"`
	ExternalIndex        string          `json:"external_index"`
	StaticProperties     []string        `json:"static_properties"`
	SearchableProperties []string        `json:"searchable_properties"`
	MaxAgeCache          int             `json:"max_age_cache"`
	Mutable              bool            `json:"mutable"`
	Permits              []access.Permit `json:"permits"`
	Description          string          `json:"description"`
	StoredExternally     bool            `json:"stored_externally"`
	needsKSS             bool            // true of this blob or any subcollection or subblob needs kss
	AuditLogs            []AuditLog      `json:"audit_logs"` // list of audit log names to use for this collection
}

// RelationConfiguration is a n:m relation from
// another collection, blob collection or relation
type RelationConfiguration struct {
	Resource     string          `json:"resource"`
	Left         string          `json:"left"`
	Right        string          `json:"right"`
	LeftPermits  []access.Permit `json:"left_permits"`
	RightPermits []access.Permit `json:"right_permits"`
	Description  string          `json:"description"`
}

// ShortcutConfiguration is shortcut to a resource
// for an authenticated request
type ShortcutConfiguration struct {
	Shortcut    string   `json:"shortcut"`
	Target      string   `json:"target"`
	Roles       []string `json:"roles"`
	Description string   `json:"description"`
}
