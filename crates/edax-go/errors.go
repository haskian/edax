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
	ErrShredded          = kernel.ErrShredded
	ErrKeyMissing        = kernel.ErrKeyMissing
	ErrKeyRevoking       = kernel.ErrKeyRevoking
	ErrKeyUnauthorized   = kernel.ErrKeyUnauthorized
	ErrKeyUnavailable    = kernel.ErrKeyUnavailable
)

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
