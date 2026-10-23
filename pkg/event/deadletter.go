package event

// DeadLetterReason classifies why the mux gave up on a message. It travels in
// the deadletterreason extension of the dead-lettered copy.
type DeadLetterReason string

// Reasons, in the order the mux evaluates them.
const (
	ReasonDecode       DeadLetterReason = "decode"       // payload does not decode into the route type
	ReasonValidation   DeadLetterReason = "validation"   // a preflight middleware rejected it
	ReasonPanic        DeadLetterReason = "panic"        // preflight or handler panicked
	ReasonNonRetryable DeadLetterReason = "nonretryable" // handler returned NonRetryableError
	ReasonNoRedelivery DeadLetterReason = "noredelivery" // retryable error on a transport that cannot redeliver (Delivery.Nack nil)
	ReasonMaxRetries   DeadLetterReason = "maxretries"   // retryable error past Config.MaxRetries
	ReasonPublish      DeadLetterReason = "publish"      // result publish failed and retrying cannot fix it
)

// Extensions the mux sets on a dead-lettered copy. Reserved: handler metadata
// with these keys is ignored, result events never carry them, and a copy that
// fails again gets them overwritten.
const (
	ExtDeadLetterReason = "deadletterreason"
	ExtDeadLetterError  = "deadlettererror"
	// MaxDeadLetterErrorLen bounds deadlettererror in bytes (cut at a rune
	// boundary): broker attributes are small and the full error is logged.
	MaxDeadLetterErrorLen = 256
)
