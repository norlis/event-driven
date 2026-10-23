// Package logfields defines the messaging-specific field names of the
// platform logging standard that httpgate/logging does not cover, plus this
// library's domain fields. OTel semantic conventions first, ECS as fallback.
// A concept must have exactly one name across every transport and service.
package logfields

import "log/slog"

// Messaging fields (OTel messaging semantic conventions).
const (
	KeyMessagingSystem        = "messaging.system"
	KeyMessagingMessageID     = "messaging.message.id"               // CloudEvent ID, always
	KeyMessagingBrokerID      = "messaging.message.broker_id"        // broker-assigned ID on publish
	KeyMessagingDestination   = "messaging.destination.name"         // topic / subject / queue URL / subscription / HTTP pattern
	KeyMessagingOutcome       = "messaging.operation.outcome"        // OutcomeAcked | OutcomeNacked | OutcomeDiscarded
	KeyCorrelationID          = "messaging.message.correlation_id"   // CloudEvents extension "correlationid", when present
	KeyDeliveryCount          = "messaging.message.delivery_count"   // broker's 1-based delivery number, when known
	KeyMessageBodySize        = "messaging.message.body.size"        // bytes of the raw broker payload
	KeyDeadLetterReason       = "messaging.deadletter.reason"        // event.DeadLetterReason
	KeyDeadLetterPublishError = "messaging.deadletter.publish_error" // why the dead-letter copy could not be published

	KeyConsumerWorkers        = "messaging.consumer.workers"
	KeyConsumerMaxOutstanding = "messaging.consumer.max_outstanding"
	KeyConsumerGoroutines     = "messaging.consumer.goroutines"
	KeyConsumerGroup          = "messaging.consumer.group"

	KeyNATSStream        = "messaging.nats.stream"
	KeyNATSSequence      = "messaging.nats.sequence"
	KeyNATSFilterSubject = "messaging.nats.filter_subject"
)

// CloudEvents attributes.
const (
	KeyCloudEventsType        = "cloudevents.event_type"
	KeyCloudEventsSource      = "cloudevents.event_source"
	KeyCloudEventsExtension   = "cloudevents.extension"    // extension name a log line is about
	KeyCloudEventsContentMode = "cloudevents.content_mode" // binary | structured, as written on the wire
)

// General fields (ECS / OTel).
const (
	KeyLogLogger     = "log.logger"         // component name
	KeyCodeFunction  = "code.function.name" // fx callee / constructor / function
	KeyServerAddress = "server.address"
	KeyURLFull       = "url.full"
	KeyProcessSignal = "process.signal" // OS signal that triggered shutdown
)

// Library domain fields.
const (
	KeyMuxName          = "eventmux.name"
	KeyComponentName    = "lifecycle.component" // name a lifecycle.Group component was added with
	KeyMuxRoutesTotal   = "eventmux.routes.total"
	KeyPayloadType      = "eventmux.payload.type"
	KeyFilterExpression = "filter.expression"
	KeyRetryAttempt     = "retry.attempt"
	KeyRetryDelay       = "retry.delay"
	KeyPredicate        = "skiperr.predicate"
)

// messaging.system values.
const (
	SystemGCPPubSub = "gcp_pubsub"
	SystemAWSSQS    = "aws_sqs"
	SystemAWSSNS    = "aws_sns"
	SystemNATS      = "nats"
	SystemHTTP      = "http"
)

// Outcome is a messaging.operation.outcome value: the router's final decision
// for a message, as it appears in logs.
type Outcome string

// messaging.operation.outcome values.
const (
	OutcomeAcked     Outcome = "acked"
	OutcomeNacked    Outcome = "nacked"
	OutcomeDiscarded Outcome = "discarded"
)

// Attr is the log attribute for o.
func (o Outcome) Attr() slog.Attr { return slog.String(KeyMessagingOutcome, string(o)) }
