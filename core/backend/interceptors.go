package backend

import (
	"context"

	"github.com/google/uuid"
	"github.com/relabs-tech/kurbisio/core"
	"github.com/relabs-tech/kurbisio/core/logger"
)

// Request is a database request. Receive them
// with HandleResourceRequest()
type Request struct {
	// Resource for which this request is made
	Resource string
	// the primary ID for the resource, for singletons this is the parent ID, for
	// list requests this is a  null uuid.
	ResourceID uuid.UUID
	// Operation for this request
	Operation core.Operation
	// Selectors are the identifiers from the request URL, can be UUID or "all"
	Selectors map[string]string
	// Parameters are the query parameters from the request URL
	Parameters map[string]string
}

type requestHandler func(ctx context.Context, request Request, data []byte) ([]byte, error)

// HandleResourceRequest installs an in-band interceptors for a given resource and a set of operations.
// If no operations are specified, the handler will be installed for the Read operation only.
//
// Any returned non-nil error will abort the operation and result in a HTTP error status code. For write
// operations that would be 400 (bad request) and for read operations 500 (internal server error).
//
// If the handler returns a non-nil []byte, this will replace the original data.  In case of Read, the user will
// see the handler's version. In case of Create or Update, the handler's version will be written to the
// database and then be returned to the user. For the Delete operation, data will always be nil and the returned
// data is ignored.
//
// Update property requests cannot be intercepted.
func (b *Backend) HandleResourceRequest(resource string,

	handler func(ctx context.Context, request Request, data []byte) ([]byte, error),
	operations ...core.Operation) {
	if !b.hasCollectionOrSingleton(resource) {
		logger.FromContext(nil).Fatalf("handle resource request for %s: no such collection or singleton", resource)
	}

	if len(operations) == 0 {
		operations = []core.Operation{core.OperationRead}
	}
	for _, operation := range operations {
		key := requestKey(resource, operation)
		if _, ok := b.interceptors[key]; ok {
			logger.FromContext(nil).Fatalf("resource request handler for %s already installed", key)
		}
		logger.FromContext(nil).Debugf("install resource request handler for %s", key)
		b.interceptors[key] = handler
	}
}

func requestKey(resource string, operation core.Operation) string {
	key := resource + "(" + string(operation) + ")"
	return key
}

func (b *Backend) intercept(ctx context.Context, resource string, operation core.Operation, resourceID uuid.UUID,
	selectors map[string]string, parameters map[string]string, data []byte) ([]byte, error) {
	request := requestKey(resource, operation)
	if interceptor, ok := b.interceptors[request]; ok {
		return interceptor(ctx,
			Request{
				Resource:   resource,
				ResourceID: resourceID,
				Operation:  operation,
				Selectors:  selectors,
				Parameters: parameters,
			},
			data)
	}
	return nil, nil
}
