package event

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/httpgate/trace"
)

// traceparentExtension is the CloudEvents Distributed Tracing Extension
// attribute name carrying the W3C traceparent value.
const traceparentExtension = "traceparent"

// traceFromCloudEvent extracts a valid trace context from the CloudEvent's
// traceparent extension, if present.
func traceFromCloudEvent(ce cloudevents.Event) (trace.Context, bool) {
	s, ok := extension(ce, traceparentExtension).(string)
	if !ok {
		return trace.Context{}, false
	}
	tc, err := trace.Parse(s)
	if err != nil {
		return trace.Context{}, false
	}
	return tc, true
}

// injectTrace stores tc as the CloudEvent's traceparent extension so the
// trace survives transports whose native carrier is stripped en route.
func injectTrace(ce *cloudevents.Event, tc trace.Context) {
	ce.SetExtension(traceparentExtension, tc.Traceparent())
}

// inheritOrNewTrace resolves the trace context for the local work of an
// incoming message: the native carrier value wins, the CloudEvent extension
// is the fallback, and a brand-new trace is started when neither is valid.
// An inherited trace keeps its trace id and gets a fresh local span id —
// the trace id is never regenerated mid-chain.
func inheritOrNewTrace(traceparent string, ce *cloudevents.Event) trace.Context {
	if tc, err := trace.Parse(traceparent); err == nil {
		return trace.Context{TraceID: tc.TraceID, SpanID: trace.NewSpanID()}
	}
	if ce != nil {
		if tc, ok := traceFromCloudEvent(*ce); ok {
			return trace.Context{TraceID: tc.TraceID, SpanID: trace.NewSpanID()}
		}
	}
	return trace.New()
}

// ContextWithTrace returns a context derived from parent carrying the trace
// context resolved by inheritOrNewTrace. Subscribers call it once per
// incoming message before building the event.Message.
func ContextWithTrace(parent context.Context, traceparent string, ce *cloudevents.Event) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	return trace.NewContext(parent, inheritOrNewTrace(traceparent, ce))
}

// InjectTraceFromContext copies the context's trace (when present) into the
// CloudEvent's traceparent extension and returns it so the transport can also
// set its native carrier. Publishers call it once per publish.
func InjectTraceFromContext(ctx context.Context, ce *cloudevents.Event) (trace.Context, bool) {
	tc, ok := trace.FromContext(ctx)
	if ok {
		injectTrace(ce, tc)
	}
	return tc, ok
}
