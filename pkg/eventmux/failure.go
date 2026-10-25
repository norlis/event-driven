package eventmux

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"
	"unicode/utf8"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/norlis/httpgate/logging"

	"github.com/norlis/event-driven/v2/pkg/event"
	"github.com/norlis/event-driven/v2/pkg/kit/logfields"
)

// panicError marks a recovered panic so classify can tell it apart. Never
// escapes the mux: Outcome() exposes cause.
type panicError struct {
	cause error
	stack []byte
}

func (e *panicError) Error() string { return e.cause.Error() }
func (e *panicError) Unwrap() error { return e.cause }

// preflightError marks an error returned by a preflight middleware. The mux
// cannot import middleware/validate (import cycle), so it tags the stage.
type preflightError struct{ err error }

func (e *preflightError) Error() string { return e.err.Error() }
func (e *preflightError) Unwrap() error { return e.err }

// panicCause turns a recovered value into the error every panic path reports.
func panicCause(r any) error { return fmt.Errorf("%w: %v", event.ErrPanic, r) }

// invoke runs the preflight chain and then the handler chain. A panic in
// either becomes a *panicError so a bug in user code never kills the broker
// SDK's goroutine.
func (mux *Mux) invoke(ctx context.Context, rt *route, payload any) (data jsontext.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &panicError{cause: panicCause(r), stack: debug.Stack()}
		}
	}()
	if _, perr := mux.preflight(ctx, payload); perr != nil {
		return nil, &preflightError{err: perr}
	}
	return rt.chain(ctx, payload)
}

// recoverOutsideHandler is deferred by handle. A panic before the decision
// (Filter.Match, route.decode) is dead-lettered as ReasonPanic. A panic after
// it (a Delivery callback, a dead-letter publisher) is only logged: the broker
// was already told once.
func (mux *Mux) recoverOutsideHandler(msg *event.Message, start time.Time) {
	r := recover()
	if r == nil {
		return
	}
	cause, stack := panicCause(r), debug.Stack()
	if outcome, _ := msg.Outcome(); outcome != event.OutcomePending {
		mux.cfg.Logger.LogAttrs(msg.Context(), slog.LevelError, "panic after message decision",
			errorAttr(cause, stack), slog.String(logfields.KeyMessagingMessageID, msg.ID()), correlationAttr(msg))
		return
	}
	defer func() {
		// fail itself panicking (a dead-letter publisher, for instance) must
		// not re-raise from a deferred recover.
		if again := recover(); again != nil {
			mux.cfg.Logger.LogAttrs(msg.Context(), slog.LevelError, "panic while settling a panicked message",
				errorAttr(panicCause(again), debug.Stack()),
				slog.String(logfields.KeyMessagingMessageID, msg.ID()))
		}
	}()
	mux.fail(msg, start, event.ReasonPanic, cause, stack)
}

// unwrapFailure strips the mux's internal markers: the stack is logged in its
// own field and the cause is what Outcome() reports.
func unwrapFailure(err error) (stack []byte, cause error) {
	if pe, ok := errors.AsType[*panicError](err); ok {
		return pe.stack, pe.cause
	}
	if fe, ok := errors.AsType[*preflightError](err); ok {
		return nil, fe.err
	}
	return nil, err
}

// classify maps a failure to a dead-letter reason; "" means retryable. Order
// matters: a preflight that returns NonRetryableError is still "validation".
func (mux *Mux) classify(msg *event.Message, err error) event.DeadLetterReason {
	if _, ok := errors.AsType[*panicError](err); ok {
		return event.ReasonPanic
	}
	if _, ok := errors.AsType[*preflightError](err); ok {
		return event.ReasonValidation
	}
	if isNonRetryable(err) {
		return event.ReasonNonRetryable
	}
	if !msg.CanNack() {
		return event.ReasonNoRedelivery
	}
	if mux.pastMaxRetries(msg) {
		return event.ReasonMaxRetries
	}
	return ""
}

// classifyPublish maps a result-publish failure to a dead-letter reason; ""
// means retryable. The handler already ran, so the only question is whether
// a redelivery could get the result out.
func (mux *Mux) classifyPublish(msg *event.Message, err error) event.DeadLetterReason {
	if isNonRetryable(err) || !msg.CanNack() || mux.pastMaxRetries(msg) {
		return event.ReasonPublish
	}
	return ""
}

// redeliveryBounded reports whether nacking msg leads to a redelivery the mux
// will eventually stop: the transport can redeliver, Config.MaxRetries is set
// and the broker reports a count still within it. Without that promise a
// nack could loop forever on a deterministic failure.
func (mux *Mux) redeliveryBounded(msg *event.Message) bool {
	_, known := msg.DeliveryCount()
	return msg.CanNack() && mux.cfg.MaxRetries > 0 && known && !mux.pastMaxRetries(msg)
}

// pastMaxRetries reports whether the broker's delivery count exceeds
// Config.MaxRetries; false when the limit is off or the count is unknown.
func (mux *Mux) pastMaxRetries(msg *event.Message) bool {
	count, known := msg.DeliveryCount()
	return mux.cfg.MaxRetries > 0 && known && count > mux.cfg.MaxRetries
}

// fail settles a failed message: retryable → Nack; definitive → dead-letter
// copy (when configured) then Discard. Exactly one log line per message.
func (mux *Mux) fail(msg *event.Message, start time.Time, reason event.DeadLetterReason, cause error, stack []byte) {
	attrs := make([]slog.Attr, 0, 8)
	attrs = append(attrs,
		errorAttr(cause, stack),
		slog.String(logfields.KeyMessagingMessageID, msg.ID()),
		correlationAttr(msg),
		deliveryAttr(msg),
	)
	if reason == "" {
		attrs = append(attrs, logfields.OutcomeNacked.Attr())
		mux.cfg.Logger.LogAttrs(msg.Context(), slog.LevelError, "message handling failed", attrs...)
		mux.settle(msg, logfields.OutcomeNacked, "", start, func() error { return msg.Nack(cause) })
		return
	}

	attrs = append(attrs, slog.String(logfields.KeyDeadLetterReason, string(reason)))
	logMsg := "message discarded"
	if mux.cfg.DeadLetter != nil {
		switch pubErr := mux.publishDeadLetter(msg, reason, cause); {
		case pubErr == nil:
			logMsg = "message dead-lettered"
		case isNonRetryable(pubErr) || !mux.redeliveryBounded(msg):
			// Retrying cannot fix it (an HTTP 4xx, a custom SNS Marshaler over
			// the attribute limit), or a nack would not be bounded (no Nack
			// primitive, no MaxRetries, no delivery count, or past the limit):
			// lose the copy, not the decision.
			attrs = append(attrs, slog.String(logfields.KeyDeadLetterPublishError, pubErr.Error()))
		default:
			attrs = append(attrs,
				slog.String(logfields.KeyDeadLetterPublishError, pubErr.Error()),
				logfields.OutcomeNacked.Attr(),
			)
			mux.cfg.Logger.LogAttrs(msg.Context(), slog.LevelError, "dead-letter publish failed", attrs...)
			mux.settle(msg, logfields.OutcomeNacked, "", start, func() error {
				return msg.Nack(fmt.Errorf("%w; %w: %w", cause, event.ErrPublicationFailed, pubErr))
			})
			return
		}
	}
	attrs = append(attrs, logfields.OutcomeDiscarded.Attr())
	mux.cfg.Logger.LogAttrs(msg.Context(), slog.LevelError, logMsg, attrs...)
	mux.settle(msg, logfields.OutcomeDiscarded, reason, start, func() error { return msg.Discard(cause) })
}

// publishDeadLetter sends the copy. The parent context (an HTTP request, for
// instance) may already be cancelled; the copy still goes out with its trace.
func (mux *Mux) publishDeadLetter(msg *event.Message, reason event.DeadLetterReason, cause error) error {
	//nolint:wrapcheck // the caller classifies the raw error and wraps it when it nacks
	return mux.cfg.DeadLetter.Publish(context.WithoutCancel(msg.Context()), deadLetterEvent(msg.Event, reason, cause))
}

func isNonRetryable(err error) bool {
	_, ok := errors.AsType[*event.NonRetryableError](err)
	return ok
}

// errorAttr renders cause like logging.Err. With a stack it builds the group
// by hand: logging.Err captures the stack of the logging call, which for a
// panic would hide the panic site and duplicate the field.
func errorAttr(cause error, stack []byte) slog.Attr {
	if stack == nil {
		return logging.Err(cause)
	}
	return slog.Attr{Key: "", Value: slog.GroupValue(
		slog.String(logging.KeyErrorType, rootType(cause)),
		slog.String(logging.KeyErrorMessage, cause.Error()),
		slog.String(logging.KeyErrorStackTrace, string(stack)),
	)}
}

// rootType mirrors logging.Err: the Go type of the innermost cause.
func rootType(err error) string {
	for {
		next := errors.Unwrap(err)
		if next == nil {
			return fmt.Sprintf("%T", err)
		}
		err = next
	}
}

// deliveryAttr is an empty Attr when the transport reports no delivery count.
func deliveryAttr(msg *event.Message) slog.Attr {
	count, known := msg.DeliveryCount()
	if !known {
		return slog.Attr{}
	}
	return slog.Uint64(logfields.KeyDeliveryCount, count)
}

// deadLetterEvent copies the original event (same id: it is a copy, not a new
// event) and stamps the reason and the truncated error. No handler metadata:
// the copy describes the input that failed, not anything the handler produced.
func deadLetterEvent(ce cloudevents.Event, reason event.DeadLetterReason, cause error) cloudevents.Event {
	dl := ce.Clone()
	dl.SetExtension(event.ExtDeadLetterReason, string(reason))
	dl.SetExtension(event.ExtDeadLetterError, truncate(sanitize(cause.Error()), event.MaxDeadLetterErrorLen))
	return dl
}

// sanitize makes s a valid broker attribute: valid UTF-8 and no control
// characters other than tab, newline and carriage return (SNS rejects them,
// and a rejected copy would otherwise be retried forever).
func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' {
			return -1
		}
		return r
	}, strings.ToValidUTF8(s, "\uFFFD"))
}

// truncate cuts s to at most maxBytes without splitting a rune.
func truncate(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
