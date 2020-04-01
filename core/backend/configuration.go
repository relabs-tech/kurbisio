package backend

// backendConfiguration holds a complete backend configuration
type backendConfiguration struct {
	Collections []collectionConfiguration `json:"collections"`
	Singletons  []singletonConfiguration  `json:"singletons"`
	Blobs       []collectionConfiguration `json:"blobs"`
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

// blobConfiguration describes a blob resource
type blobConfiguration struct {
	Resource      string              `json:"resource"`
	Notifications []string            `json:"notifications"`
	Permissions   map[string][]string `json:"permissions"`
}

// relationConfiguration is a n:m relation from any other resource,
// including from any other relation
type relationConfiguration struct {
	Resource string `json:"resource"`
	Origin   string `json:"origin"`
}
