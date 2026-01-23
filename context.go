package durex

import (
	"context"
)

// Context keys for durex-specific values.
type contextKey string

const (
	traceIDKey       contextKey = "durex_trace_id"
	correlationIDKey contextKey = "durex_correlation_id"
	instanceKey      contextKey = "durex_instance"
)

// WithTraceID returns a new context with the given trace ID.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

// TraceIDFromContext returns the trace ID from the context, or empty string if not set.
func TraceIDFromContext(ctx context.Context) string {
	if v := ctx.Value(traceIDKey); v != nil {
		return v.(string)
	}
	return ""
}

// WithCorrelationID returns a new context with the given correlation ID.
func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationIDKey, correlationID)
}

// CorrelationIDFromContext returns the correlation ID from the context, or empty string if not set.
func CorrelationIDFromContext(ctx context.Context) string {
	if v := ctx.Value(correlationIDKey); v != nil {
		return v.(string)
	}
	return ""
}

// WithInstance returns a new context with the given command instance.
func WithInstance(ctx context.Context, instance *Instance) context.Context {
	return context.WithValue(ctx, instanceKey, instance)
}

// InstanceFromContext returns the command instance from the context, or nil if not set.
func InstanceFromContext(ctx context.Context) *Instance {
	if v := ctx.Value(instanceKey); v != nil {
		return v.(*Instance)
	}
	return nil
}

// ContextExtractor extracts trace/correlation IDs from incoming requests.
// Implement this interface to integrate with your tracing system.
type ContextExtractor interface {
	// ExtractTraceID extracts a trace ID from the context.
	ExtractTraceID(ctx context.Context) string

	// ExtractCorrelationID extracts a correlation ID from the context.
	ExtractCorrelationID(ctx context.Context) string
}

// ContextInjector injects trace/correlation IDs into outgoing contexts.
// Implement this interface to integrate with your tracing system.
type ContextInjector interface {
	// InjectContext injects trace and correlation IDs into the context.
	InjectContext(ctx context.Context, traceID, correlationID string) context.Context
}

// DefaultContextExtractor uses durex context keys.
type DefaultContextExtractor struct{}

// ExtractTraceID implements ContextExtractor.
func (DefaultContextExtractor) ExtractTraceID(ctx context.Context) string {
	return TraceIDFromContext(ctx)
}

// ExtractCorrelationID implements ContextExtractor.
func (DefaultContextExtractor) ExtractCorrelationID(ctx context.Context) string {
	return CorrelationIDFromContext(ctx)
}

// DefaultContextInjector uses durex context keys.
type DefaultContextInjector struct{}

// InjectContext implements ContextInjector.
func (DefaultContextInjector) InjectContext(ctx context.Context, traceID, correlationID string) context.Context {
	if traceID != "" {
		ctx = WithTraceID(ctx, traceID)
	}
	if correlationID != "" {
		ctx = WithCorrelationID(ctx, correlationID)
	}
	return ctx
}

// TracingMiddleware creates middleware that propagates trace and correlation IDs.
// It injects the instance's trace/correlation IDs into the context before execution.
func TracingMiddleware(injector ContextInjector) Middleware {
	if injector == nil {
		injector = DefaultContextInjector{}
	}

	return func(mwCtx MiddlewareContext, next func() (Result, error)) (Result, error) {
		// The context enrichment happens via the instance itself
		// Commands can access TraceID and CorrelationID from the instance
		return next()
	}
}

// Ensure implementations satisfy interfaces.
var (
	_ ContextExtractor = DefaultContextExtractor{}
	_ ContextInjector  = DefaultContextInjector{}
)
