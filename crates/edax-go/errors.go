package edax

import "github.com/haskian/edax/crates/edax-go/internal/kernel"

// Error is a kernel failure with a stable category, message and optional cause.
type Error = kernel.Error

// Error sentinels are immutable categories matched through errors.Is.
// Inspect a concrete *Error for a failure message and cause.
const (
	ErrConcurrency       = kernel.ErrConcurrency
	ErrStore             = kernel.ErrStore
	ErrCodec             = kernel.ErrCodec
	ErrPayloadMismatch   = kernel.ErrPayloadMismatch
	ErrInProgress        = kernel.ErrInProgress
	ErrCachedFailure     = kernel.ErrCachedFailure
	ErrHandlerNotFound   = kernel.ErrHandlerNotFound
	ErrAlreadyRegistered = kernel.ErrAlreadyRegistered
	ErrBadHandle         = kernel.ErrBadHandle
	ErrRejected          = kernel.ErrRejected
)

// Reason is why a host's rejection refused (not_found), kept beside the rejected code in the cached
// record so a replay tells two refusals apart. Build it with NewReason or MustReason.
type Reason = kernel.Reason

// NewReason checks name with the kernel: 1 to 64 bytes of [a-z0-9._-], starting with [a-z0-9].
func NewReason(name string) (Reason, error) { return kernel.NewReason(name) }

// MustReason is NewReason for a package-level reason; it panics on a bad name, as regexp.MustCompile does.
func MustReason(name string) Reason { return kernel.MustReason(name) }

// Reject builds a host's terminal rejection that names why it refused. The guard caches it under the
// rejected code with its reason, and ReasonOf reads the reason off it and off its replay alike.
func Reject(reason Reason, message string) *Error { return kernel.Reject(reason, message) }

// ReasonOf is why a rejection refused: Reject's reason, or the one a replayed cached failure carries;
// "" for any other failure.
func ReasonOf(err error) string { return kernel.ReasonOf(err) }

// WrapError adds a kernel category and message while retaining the original cause.
func WrapError(code ErrorCode, message string, cause error) *Error {
	return kernel.WrapError(code, message, cause)
}

// ErrorFromWire decodes the kernel's stable error representation.
func ErrorFromWire(wire string) *Error { return kernel.ErrorFromWire(wire) }

// IsTerminal reports whether the kernel considers this failure repeatable on retry.
func IsTerminal(err error) bool { return kernel.IsTerminal(err) }

// IsFatal reports whether the failure affects every message alike.
func IsFatal(err error) bool         { return kernel.IsFatal(err) }
func closedHandle(name string) error { return kernel.ClosedHandle(name) }
func errorCode(err error) *string    { return kernel.CodeOf(err) }
func errorMessage(err error) string  { return kernel.MessageOf(err) }
func errorReason(err error) *string {
	if reason := kernel.ReasonOf(err); reason != "" {
		return &reason
	}
	return nil
}
