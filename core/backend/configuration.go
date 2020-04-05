package backend

import "github.com/relabs-tech/backends/core/access"

// backendConfiguration holds a complete backend configuration
type backendConfiguration struct {
	Collections []collectionConfiguration `json:"collections"`
	Singletons  []singletonConfiguration  `json:"singletons"`
	Blobs       []blobConfiguration       `json:"blobs"`
	Relations   []relationConfiguration   `json:"relations"`
	Roles       []string                  `json:"roles"`
}

// collectionConfiguration describes a collection resource
type collectionConfiguration struct {
	Resource             string          `json:"resource"`
	Shortcuts            bool            `json:"shortcuts"`
	ExternalIndex        string          `json:"external_index"`
	StaticProperties     []string        `json:"static_properties"`
	SearchableProperties []string        `json:"searchable_properties"`
	Notifications        []string        `json:"notifications"`
	Permits              []access.Permit `json:"permits"`
}

// singletonConfiguration describes a singleton resource
type singletonConfiguration struct {
	Resource      string          `json:"resource"`
	Notifications []string        `json:"notifications"`
	Permits       []access.Permit `json:"permits"`
}

// blobConfiguration describes a blob collection resource
type blobConfiguration struct {
	Resource             string          `json:"resource"`
	ExternalIndex        string          `json:"external_index"`
	StaticProperties     []string        `json:"static_properties"`
	SearchableProperties []string        `json:"searchable_properties"`
	Notifications        []string        `json:"notifications"`
	MaxAgeCache          int             `json:"max_age_cache"`
	Mutable              bool            `json:"mutable"`
	Permits              []access.Permit `json:"permits"`
}

// relationConfiguration is a n:m relation from
// another collection, blob collection or relation
type relationConfiguration struct {
	Resource string          `json:"resource"`
	Origin   string          `json:"origin"`
	Permits  []access.Permit `json:"permits"`
}
