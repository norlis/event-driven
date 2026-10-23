package eventhttp

// ErrorCode is the machine-readable error identifier returned in Response.Code.
type ErrorCode string

// Error codes returned by the subscriber.
const (
	CodeInvalidRequest       ErrorCode = "INVALID-REQUEST"        // malformed CloudEvent, unreadable body or invalid client id
	CodeInvalidCorrelationID ErrorCode = "INVALID-CORRELATION-ID" // see event.ValidateCorrelationID
	CodePayloadTooLarge      ErrorCode = "PAYLOAD-TOO-LARGE"      // body above SubscriberConfig.MaxBodyBytes
	CodeUnsupportedMediaType ErrorCode = "UNSUPPORTED-MEDIA-TYPE" // plain request without application/json
	CodeRouteNotFound        ErrorCode = "ROUTE-NOT-FOUND"
	CodeInvalidPayload       ErrorCode = "INVALID-PAYLOAD"
	CodeValidationFailed     ErrorCode = "VALIDATION-FAILED"
	CodeHandlerFailed        ErrorCode = "HANDLER-FAILED" // the handler failed or discarded the event; details only in server logs
)

// GenericFailureDetail is the client-facing detail for CodeHandlerFailed: the
// real error may carry internal data and is only logged.
const GenericFailureDetail = "The event could not be processed"
