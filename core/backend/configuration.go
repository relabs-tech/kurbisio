package backend

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
	Resource             string              `json:"resource"`
	LoggedInRoutes       bool                `json:"logged_in_routes"`
	ExternalIndex        string              `json:"external_index"`
	StaticProperties     []string            `json:"static_properties"`
	SearchableProperties []string            `json:"searchable_properties"`
	Notifications        []string            `json:"notifications"`
	Permissions          map[string][]string `json:"permissions"`
}

// singletonConfiguration describes a singleton resource
type singletonConfiguration struct {
	Resource      string              `json:"resource"`
	Notifications []string            `json:"notifications"`
	Permissions   map[string][]string `json:"permissions"`
}

// blobConfiguration describes a blob collection resource
type blobConfiguration struct {
	Resource             string              `json:"resource"`
	ExternalIndex        string              `json:"external_index"`
	StaticProperties     []string            `json:"static_properties"`
	SearchableProperties []string            `json:"searchable_properties"`
	Notifications        []string            `json:"notifications"`
	Permissions          map[string][]string `json:"permissions"`
	MaxAgeCache          int                 `json:"max_age_cache"`
}

// relationConfiguration is a n:m relation from
// another collection, blob collection or relation
type relationConfiguration struct {
	Resource string `json:"resource"`
	Origin   string `json:"origin"`
}
