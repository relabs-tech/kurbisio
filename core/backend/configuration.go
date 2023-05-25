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

// Configuration holds a complete backend configuration
type Configuration struct {
	Collections []collectionConfiguration `json:"collections"`
	Singletons  []singletonConfiguration  `json:"singletons"`
	Blobs       []blobConfiguration       `json:"blobs"`
	Relations   []relationConfiguration   `json:"relations"`
	Shortcuts   []shortcutConfiguration   `json:"shortcuts"`
}

// collectionConfiguration describes a collection resource
type collectionConfiguration struct {
	Resource                      string          `json:"resource"`
	ExternalIndex                 string          `json:"external_index"`
	StaticProperties              []string        `json:"static_properties"`
	SearchableProperties          []string        `json:"searchable_properties"`
	Permits                       []access.Permit `json:"permits"`
	Description                   string          `json:"description"`
	SchemaID                      string          `json:"schema_id"`
	WithLog                       bool            `json:"with_log"`
	Default                       json.RawMessage `json:"default"`
	WithCompanionFile             bool            `json:"with_companion_file"`
	CompanionPresignedURLValidity int             `json:"companion_presigned_url_validity"`
	needsKSS                      bool            // true of this collection or any subcollection or subblob needs kss
}

// singletonConfiguration describes a singleton resource
type singletonConfiguration struct {
	Resource             string          `json:"resource"`
	Permits              []access.Permit `json:"permits"`
	Description          string          `json:"description"`
	SchemaID             string          `json:"schema_id"`
	StaticProperties     []string        `json:"static_properties"`
	SearchableProperties []string        `json:"searchable_properties"`
	WithLog              bool            `json:"with_log"`
	Default              json.RawMessage `json:"default"`
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
	StoredExternally     bool            `json:"stored_externally"`
	needsKSS             bool            // true of this blob or any subcollection or subblob needs kss
}

// relationConfiguration is a n:m relation from
// another collection, blob collection or relation
type relationConfiguration struct {
	Resource     string          `json:"resource"`
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
