package backend

import (
	"github.com/relabs-tech/backends/core/access"
)

// backendConfiguration holds a complete backend configuration
type backendConfiguration struct {
	Collections []collectionConfiguration `json:"collections"`
	Singletons  []singletonConfiguration  `json:"singletons"`
	Blobs       []blobConfiguration       `json:"blobs"`
	Relations   []relationConfiguration   `json:"relations"`
	Shortcuts   []shortcutConfiguration   `json:"shortcuts"`
}

// collectionConfiguration describes a collection resource
type collectionConfiguration struct {
	Resource             string          `json:"resource"`
	ExternalIndex        string          `json:"external_index"`
	StaticProperties     []string        `json:"static_properties"`
	SearchableProperties []string        `json:"searchable_properties"`
	Permits              []access.Permit `json:"permits"`
	Description          string          `json:"description"`
	SchemaID             string          `json:"schema_id"`
}

// singletonConfiguration describes a singleton resource
type singletonConfiguration struct {
	Resource    string          `json:"resource"`
	Permits     []access.Permit `json:"permits"`
	Description string          `json:"description"`
	SchemaID    string          `json:"schema_id"`
}

// blobConfiguration describes a blob collection resource
type blobConfiguration struct {
	Resource             string          `json:"resource"`
	ExternalIndex        string          `json:"external_index"`
	StaticProperties     []string        `json:"static_properties"`
	SearchableProperties []string        `json:"searchable_properties"`
	MaxAgeCache          int             `json:"max_age_cache"`
	Mutable              bool            `json:"mutable"`
	Permits              []access.Permit `json:"permits"`
	Description          string          `json:"description"`
}

// relationConfiguration is a n:m relation from
// another collection, blob collection or relation
type relationConfiguration struct {
	Left         string          `json:"left"`
	Right        string          `json:"right"`
	LeftPermits  []access.Permit `json:"left_permits"`
	RightPermits []access.Permit `json:"right_permits"`
	Description  string          `json:"description"`
}

// shortcutConfiguration is shorcut to a resource
// for an authenticated request
type shortcutConfiguration struct {
	Shortcut    string   `json:"shortcut"`
	Target      string   `json:"target"`
	Roles       []string `json:"roles"`
	Description string   `json:"description"`
}
