package kernel

import (
	"errors"
	"strconv"
	"strings"

	"github.com/haskian/edax/crates/edax-go/internal/native"
)

// Error is a kernel failure. Code and Message preserve the kernel's wire form.
type Error struct {
	Code    ErrorCode
	Message string
	cause   error
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
	if category, ok := err.(sentinel); ok {
		code := string(category)
		return &code
	}
	// A joined cleanup or partition error must not hide a fatal failure behind
	// the first transient one. Ask the kernel about each branch's code.
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var selected *string
		selectedRank := -1
		for _, child := range joined.Unwrap() {
			code := CodeOf(child)
			kind, failure := native.RetryFailure(code)
			rank := 0
			if failure == nil {
				switch kind {
				case "fatal":
					return code
				case "terminal":
					rank = 1
				}
			}
			if rank > selectedRank || rank == selectedRank && selected == nil {
				selected, selectedRank = code, rank
			}
		}
		return selected
	}
	if _, typed := err.(*Error); !typed {
		if wrapped, ok := err.(interface{ Unwrap() error }); ok {
			return CodeOf(wrapped.Unwrap())
		}
	}
	var typed *Error
	if errors.As(err, &typed) && typed != nil {
		if checkNativeString("code", string(typed.Code)) != nil {
			return nil
		}
		code := string(typed.Code)
		return &code
	}
	return nil
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
