package backend

import "github.com/gorilla/mux"

// KExtension is the interface that all extensions must implement
// It is used to manage the extensions in the kurbisio backend.
type KExtension interface {
	// GetName returns the name of the extension which is used for logging
	GetName() string

	// UpdateConfig update the kurbisio configuration, potentially adding collection, singletons...
	// The UpdateConfig is called by the builder
	UpdateConfig(config Configuration) (Configuration, error)

	// UpdateMux updates the mux router with the extension routes
	// The UpdateMux is called by the builder
	UpdateMux(router *mux.Router) error

}
