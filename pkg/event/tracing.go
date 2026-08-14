package event

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/httpgate/trace"
)

// traceparentExtension is the CloudEvents Distributed Tracing Extension
// attribute name carrying the W3C traceparent value.
const traceparentExtension = "traceparent"

// TraceFromCloudEvent extracts a valid trace context from the CloudEvent's
// traceparent extension, if present.
func TraceFromCloudEvent(ce cloudevents.Event) (trace.Context, bool) {
	v, ok := ce.Extensions()[traceparentExtension]
	if !ok {
		return trace.Context{}, false
	}
	s, ok := v.(string)
	if !ok {
		return trace.Context{}, false
	}
	tc, err := trace.Parse(s)
	if err != nil {
		return trace.Context{}, false
	}
	return tc, true
}

// InjectTrace stores tc as the CloudEvent's traceparent extension so the
// trace survives transports whose native carrier is stripped en route.
func InjectTrace(ce *cloudevents.Event, tc trace.Context) {
	ce.SetExtension(traceparentExtension, tc.Traceparent())
}

// InheritOrNewTrace resolves the trace context for the local work of an
// incoming message: the native carrier value wins, the CloudEvent extension
// is the fallback, and a brand-new trace is started when neither is valid.
// An inherited trace keeps its trace id and gets a fresh local span id —
// the trace id is never regenerated mid-chain.
func InheritOrNewTrace(traceparent string, ce *cloudevents.Event) trace.Context {
	if tc, err := trace.Parse(traceparent); err == nil {
		return trace.Context{TraceID: tc.TraceID, SpanID: trace.NewSpanID()}
	}
	if ce != nil {
		if tc, ok := TraceFromCloudEvent(*ce); ok {
			return trace.Context{TraceID: tc.TraceID, SpanID: trace.NewSpanID()}
		}
	}
	return trace.New()
}

// ContextWithTrace returns a context derived from parent carrying the trace
// context resolved by InheritOrNewTrace. Subscribers call it once per
// incoming message before building the event.Message.
func ContextWithTrace(parent context.Context, traceparent string, ce *cloudevents.Event) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	return trace.NewContext(parent, InheritOrNewTrace(traceparent, ce))
}

// InjectTraceFromContext copies the context's trace (when present) into the
// CloudEvent's traceparent extension and returns it so the transport can also
// set its native carrier. Publishers call it once per publish.
func InjectTraceFromContext(ctx context.Context, ce *cloudevents.Event) (trace.Context, bool) {
	tc, ok := trace.FromContext(ctx)
	if ok {
		InjectTrace(ce, tc)
	}
	return tc, ok
}
