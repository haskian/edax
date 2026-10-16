package kernel

import (
	"errors"
	"strconv"
	"strings"

	"github.com/haskian/edax/crates/edax-go/internal/native"
)

// Error is a kernel failure. Code and Message preserve the kernel's wire form. OriginalCode is the code
// of the failure a cached_failure replays, empty otherwise. A rejection's reason is unexported, so only
// Reject or a replay sets it and a struct literal cannot fake one.
type Error struct {
	Code         ErrorCode
	Message      string
	OriginalCode ErrorCode
	reason       string
	cause        error
}

// Reason is why a host's rejection refused (not_found): a name the host branches on, kept beside the
// rejected code in the cached record so a replay tells two refusals apart. The zero Reason names none.
type Reason struct{ name string }

// NewReason checks name with the kernel: 1 to 64 bytes of [a-z0-9._-], starting with [a-z0-9], else a
// codec error.
func NewReason(name string) (Reason, error) {
	if err := CheckRejectionReason(name); err != nil {
		return Reason{}, err
	}
	return Reason{name: name}, nil
}

// MustReason is NewReason for a reason fixed at package initialization; it panics on a bad one, as
// regexp.MustCompile does, so the mistake fails the program's start rather than a settle.
func MustReason(name string) Reason {
	reason, err := NewReason(name)
	if err != nil {
		panic(err)
	}
	return reason
}

// String returns the name, or "" for the zero Reason.
func (r Reason) String() string { return r.name }

// Reject builds a host's terminal rejection that names why it refused.
func Reject(reason Reason, message string) *Error {
	return &Error{Code: CodeRejected, Message: message, reason: reason.name}
}

// Replayed is the cached failure a losing claimant raises from wire, carrying the original code and the
// reason a rejection named.
func Replayed(wire string, originalCode *string, reason *string) *Error {
	err := ErrorFromWire(wire)
	if originalCode != nil {
		err.OriginalCode = ErrorCode(*originalCode)
	}
	if reason != nil {
		err.reason = *reason
	}
	return err
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }
func (e *Error) Unwrap() error { return e.cause }

// WrapError adds a kernel category and message while retaining the original cause.
func WrapError(code ErrorCode, message string, cause error) *Error {
	return &Error{Code: code, Message: message, cause: cause}
}

// Is compares taxonomy codes. Message text does not affect error identity.
func (e *Error) Is(target error) bool {
	if e == nil {
		return false
	}
	switch other := target.(type) {
	case sentinel:
		return e.Code == ErrorCode(other)
	case *Error:
		return other != nil && e.Code == other.Code
	default:
		return false
	}
}

// A sentinel is a category, not a mutable failure instance. It remains an error
// when returned or wrapped, but only concrete failures expose *Error details.
type sentinel ErrorCode

func (s sentinel) Error() string { return string(s) + ": " }
func (s sentinel) Is(target error) bool {
	switch other := target.(type) {
	case sentinel:
		return s == other
	case *Error:
		return other != nil && ErrorCode(s) == other.Code
	default:
		return false
	}
}

const (
	ErrConcurrency       = sentinel(CodeConcurrency)
	ErrStore             = sentinel(CodeStore)
	ErrCodec             = sentinel(CodeCodec)
	ErrPayloadMismatch   = sentinel(CodePayloadMismatch)
	ErrInProgress        = sentinel(CodeInProgress)
	ErrCachedFailure     = sentinel(CodeCachedFailure)
	ErrHandlerNotFound   = sentinel(CodeHandlerNotFound)
	ErrAlreadyRegistered = sentinel(CodeAlreadyRegistered)
	ErrBadHandle         = sentinel(CodeBadHandle)
	ErrRejected          = sentinel(CodeRejected)
	ErrShredded          = sentinel(CodeShredded)
	ErrKeyMissing        = sentinel(CodeKeyMissing)
	ErrKeyRevoking       = sentinel(CodeKeyRevoking)
	ErrKeyUnauthorized   = sentinel(CodeKeyUnauthorized)
	ErrKeyUnavailable    = sentinel(CodeKeyUnavailable)
)

// ClosedHandle reports a released SDK resource.
func ClosedHandle(name string) error {
	return &Error{Code: CodeBadHandle, Message: name + " is closed"}
}

// mapNativeError removes alef's numeric transport prefix. The kernel alone
// interprets its own wire error and supplies the fallback code.
func mapNativeError(err error) error {
	if err == nil {
		return nil
	}
	wire := err.Error()
	if strings.HasPrefix(wire, "[") {
		if end := strings.Index(wire, "] "); end > 1 {
			if _, parseErr := strconv.ParseInt(wire[1:end], 10, 32); parseErr == nil {
				wire = wire[end+2:]
			}
		}
	}
	// These are generator conversion failures before a kernel function can run.
	// They repeat for the same input and belong to the binding's codec boundary.
	for _, prefix := range []string{"failed to create ", "failed to marshal:", "failed to unmarshal:", "failed to serialise ", "failed to convert to JSON", "Invalid JSON in parameter '", "Invalid UTF-8 in parameter '", "Null pointer passed for parameter '", "FFI field value contains an interior NUL byte", "FFI JSON value contains an interior NUL byte", "failed to serialize FFI field value"} {
		if strings.HasPrefix(wire, prefix) {
			return &Error{Code: CodeCodec, Message: wire, cause: err}
		}
	}
	result := ErrorFromWire(wire)
	result.cause = err
	return result
}

// ErrorFromWire asks the kernel to decode its stable error representation.
func ErrorFromWire(wire string) *Error {
	if err := checkNativeString("wire", wire); err != nil {
		return err.(*Error)
	}
	split := native.GetSplitError(wire)
	return &Error{Code: ErrorCode(split.Code), Message: split.Message}
}

// CodeOf selects the kernel category, preserving fatal precedence in joined errors.
func CodeOf(err error) *string {
	code, _ := failureOf(err)
	return code
}

// ReasonOf is why a rejection refused: Reject's reason, or the one a replayed cached failure carries
// from the record; "" for any other failure. It reads the same branch of a joined error CodeOf does.
func ReasonOf(err error) string {
	_, reason := failureOf(err)
	return reason
}

// failureOf selects one failure's code and reason together, preserving fatal precedence in joined
// errors, so the two never come from different branches.
func failureOf(err error) (*string, string) {
	if category, ok := err.(sentinel); ok {
		code := string(category)
		return &code, ""
	}
	// A joined cleanup or partition error must not hide a fatal failure behind
	// the first transient one. Ask the kernel about each branch's code.
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var selected *string
		var selectedReason string
		selectedRank := -1
		for _, child := range joined.Unwrap() {
			code, reason := failureOf(child)
			kind, failure := native.RetryFailure(code)
			rank := 0
			if failure == nil {
				switch kind {
				case "fatal":
					return code, reason
				case "terminal":
					rank = 1
				}
			}
			if rank > selectedRank || rank == selectedRank && selected == nil {
				selected, selectedReason, selectedRank = code, reason, rank
			}
		}
		return selected, selectedReason
	}
	if _, typed := err.(*Error); !typed {
		if wrapped, ok := err.(interface{ Unwrap() error }); ok {
			return failureOf(wrapped.Unwrap())
		}
	}
	var typed *Error
	if errors.As(err, &typed) && typed != nil {
		if checkNativeString("code", string(typed.Code)) != nil {
			return nil, ""
		}
		code := string(typed.Code)
		return &code, typed.reason
	}
	return nil, ""
}

// MessageOf is what a failure records beside its code, like Rust's
// failure_message: a top-level *Error gives its Message, a bare sentinel gives
// none, and anything else gives its full text. Only the top level counts, so a
// wrapper's own context stays in the message.
func MessageOf(err error) string {
	switch typed := err.(type) {
	case nil, sentinel:
		return ""
	case *Error:
		return typed.Message
	default:
		return err.Error()
	}
}

// IsTerminal asks the kernel whether this failure repeats on retry.
func IsTerminal(err error) bool {
	if err == nil {
		return false
	}
	terminal, failure := native.ErrorVerdict(CodeOf(err))
	return failure == nil && terminal
}

// IsFatal asks the kernel whether the failure affects every message alike.
func IsFatal(err error) bool {
	if err == nil {
		return false
	}
	kind, failure := native.RetryFailure(CodeOf(err))
	return failure == nil && kind == "fatal"
}
