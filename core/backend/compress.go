package backend

import (
	"net/http"

	"github.com/gorilla/handlers"
)

func (b *Backend) handleCompression() {

	compressionMiddleware := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handlers.CompressHandler(h).ServeHTTP(w, r)
		})
	}
	b.router.Use(compressionMiddleware)
}
