package logger

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type contextLoggerValues struct {
	RequestID string `json:"requestID"`
	Identity  string `json:"identity"`
}

// Type for the context keys
type contextKeyRequestLoggerType struct{}

var contextKeyRequestLogger = &contextKeyRequestLoggerType{}

const (
	// Context key for the request ID
	requestIDLoggerKey string = "requestID"
	identityLoggerKey  string = "identity"
)

// InitLogger sets up the custom time formatter for all log statements.
func InitLogger(logLevel logrus.Level) {
	customFormatter := new(logrus.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	logrus.SetFormatter(customFormatter)
	logrus.SetLevel(logrus.DebugLevel)
	customFormatter.FullTimestamp = true
	logrus.SetLevel(logLevel)
}

// AddRequestID adds a logger with a new request ID if no logger exits yet for the context.
func AddRequestID(router *mux.Router) {

	reqID := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, _ := ContextWithLogger(r.Context())
			h.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	router.Use(reqID)
}

// Default returns a logger without a request ID.
func Default() *logrus.Entry {
	return logrus.NewEntry(logrus.StandardLogger())
}

// ContextWithLogger returns a new context with a logger if the given context has no logger yet. If
// the context already has a logger the given context will be returned.
func ContextWithLogger(ctx context.Context) (context.Context, *logrus.Entry) {
	if ctx == nil {
		ctx = context.Background()
	} else {
		rlog := loggerFromContext(ctx)
		if rlog != nil {
			return ctx, rlog
		}
	}
	id, _ := uuid.NewUUID()
	rlog := logrus.WithField(requestIDLoggerKey, id.String())
	return context.WithValue(ctx, contextKeyRequestLogger, rlog), rlog
}

// ContextWithLoggerFromData returns a context with a logger. If the context does not have a logger yet,
// the logger is constructed from the provided data. If the construction fails because of invalid
// data a new logger is created and added to the context. The given context is returned in case
// it already has a logger.
func ContextWithLoggerFromData(ctx context.Context, data []byte) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	rlog := loggerFromContext(ctx)
	if rlog != nil {
		return ctx
	}

	var ok bool
	ctx, ok = deserializeLoggerContext(ctx, data)
	if !ok {
		ctx, _ = ContextWithLogger(ctx)
	}
	return ctx
}

func loggerFromContext(ctx context.Context) *logrus.Entry {
	if ctx == nil {
		return nil
	}
	rlog, ok := ctx.Value(contextKeyRequestLogger).(*logrus.Entry)
	if !ok {
		return nil
	}
	return rlog
}

// FromContext returns the logger from the context. If the context does not have a logger
// a new logger is returned. If the provided context is nil, the default logger will be
// returned.
func FromContext(ctx context.Context) *logrus.Entry {
	if ctx == nil {
		return logrus.NewEntry(logrus.StandardLogger())
	}
	rlog := loggerFromContext(ctx)
	if rlog == nil {
		return logrus.NewEntry(logrus.StandardLogger())
	}
	return rlog
}

// ContextWithLoggerIdentity returns a new context with a logger and identity.
func ContextWithLoggerIdentity(ctx context.Context, identity string) (context.Context, *logrus.Entry) {
	var rlog *logrus.Entry
	ctx, rlog = ContextWithLogger(ctx)
	if rlog == nil {
		return ctx, rlog
	}
	rlog = rlog.WithField(identityLoggerKey, identity)
	ctx = context.WithValue(ctx, contextKeyRequestLogger, rlog)
	return ctx, rlog
}

// SerializeLoggerContext extracts the logger from the context and returns a json
// representation of the relevant parameters.
func SerializeLoggerContext(ctx context.Context) []byte {
	ctxValues := loggerValues(ctx)
	if ctxValues.RequestID == "" {
		return []byte("{}")
	}

	res, err := json.Marshal(ctxValues)
	if err != nil {
		return []byte("{}")
	}
	return res
}

// RequestIDFromContext returns the request id for the given context.
func RequestIDFromContext(ctx context.Context) string {
	v := loggerValues(ctx)
	return v.RequestID
}

func loggerValues(ctx context.Context) contextLoggerValues {
	var ctxValues contextLoggerValues

	if ctx == nil {
		return ctxValues
	}
	rlog, ok := ctx.Value(contextKeyRequestLogger).(*logrus.Entry)
	if !ok {
		return ctxValues
	}

	if rlog.Data[requestIDLoggerKey] != nil {
		if s, ok := rlog.Data[requestIDLoggerKey].(string); ok {
			ctxValues.RequestID = s
		}
	}
	if rlog.Data[identityLoggerKey] != nil {
		if s, ok := rlog.Data[identityLoggerKey].(string); ok {
			ctxValues.Identity = s
		}
	}
	return ctxValues
}

// deserializeLoggerContext creates a logger from the provided json data and returns a new context with this
// logger.
func deserializeLoggerContext(ctx context.Context, data []byte) (context.Context, bool) {
	var ctxValues contextLoggerValues
	err := json.Unmarshal(data, &ctxValues)
	if err != nil || len(ctxValues.RequestID) < 1 {
		return ctx, false
	}

	if ctx == nil {
		ctx = context.Background()
	}

	rlog := logrus.WithField(requestIDLoggerKey, ctxValues.RequestID)
	if len(ctxValues.Identity) > 0 {
		rlog = rlog.WithField(identityLoggerKey, ctxValues.Identity)
	}

	return context.WithValue(ctx, contextKeyRequestLogger, rlog), true
}
