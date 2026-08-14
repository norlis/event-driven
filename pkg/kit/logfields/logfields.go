// Package logfields defines the messaging-specific field names of the
// platform logging standard that httpgate/logging does not cover, plus this
// library's domain fields. OTel semantic conventions first, ECS as fallback.
// A concept must have exactly one name across every transport and service.
package logfields

// Messaging fields (OTel messaging semantic conventions).
const (
	KeyMessagingSystem      = "messaging.system"
	KeyMessagingMessageID   = "messaging.message.id"        // CloudEvent ID, always
	KeyMessagingBrokerID    = "messaging.message.broker_id" // broker-assigned ID on publish
	KeyMessagingDestination = "messaging.destination.name"  // topic / subject / queue URL / subscription / HTTP pattern
	KeyMessagingOutcome     = "messaging.operation.outcome" // OutcomeAcked | OutcomeNacked | OutcomeDiscarded

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
	KeyCloudEventsType   = "cloudevents.event_type"
	KeyCloudEventsSource = "cloudevents.event_source"
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
	KeyMuxRoutesTotal   = "eventmux.routes.total"
	KeyMuxStopCause     = "eventmux.stop_cause"
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

// messaging.operation.outcome values.
const (
	OutcomeAcked     = "acked"
	OutcomeNacked    = "nacked"
	OutcomeDiscarded = "discarded"
)
