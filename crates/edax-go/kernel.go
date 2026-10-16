package edax

import "github.com/haskian/edax/crates/edax-go/internal/kernel"

// BackoffPolicy defines the kernel's retry delay bounds in milliseconds.
type BackoffPolicy = kernel.BackoffPolicy

// ClaimResult reports ownership or the existing idempotency record for a key.
type ClaimResult = kernel.ClaimResult

// CommandMessage carries a command payload and its identity, lineage and tenant.
type CommandMessage = kernel.CommandMessage

// EventDraft is an event staged for an atomic append to its aggregate stream.
type EventDraft = kernel.EventDraft

// EventMessage carries a recorded event payload and its stream position and lineage.
type EventMessage = kernel.EventMessage

// IdempotencyRecord stores the payload hash and pending or completed command outcome.
type IdempotencyRecord = kernel.IdempotencyRecord

// Lineage identifies a message's correlation and immediate cause.
type Lineage = kernel.Lineage

// Payload carries a named, versioned domain value in its chosen wire encoding.
type Payload = kernel.Payload

// ProjectionRow stores a versioned JSON projection document.
type ProjectionRow = kernel.ProjectionRow

// DefaultBackoff returns the kernel's default delay bounds.
func DefaultBackoff() *BackoffPolicy { return kernel.DefaultBackoff() }

// Guid returns a new message identifier.
func Guid() string { return kernel.Guid() }

// CurrentTimestamp returns the kernel clock's Unix timestamp in nanoseconds.
func CurrentTimestamp() int64 { return kernel.CurrentTimestamp() }

// IsStale reports whether an incoming sequence precedes the stored mark.
func IsStale(stored, incoming *uint64) bool {
	return kernel.IsStale(stored != nil, sequenceValue(stored), incoming != nil, sequenceValue(incoming))
}

// Advance returns the larger available sequence mark.
func Advance(stored, incoming *uint64) *uint64 {
	return kernel.Advance(stored != nil, sequenceValue(stored), incoming != nil, sequenceValue(incoming))
}

// sequenceValue is the value half of a sequence's presence pair. The ABI crosses presence
// separately because its absent sentinel, the maximum uint64, is a valid sequence.
func sequenceValue(sequence *uint64) uint64 {
	if sequence == nil {
		return 0
	}
	return *sequence
}

// PayloadHashJSON hashes canonical JSON through the kernel's shared rules.
func PayloadHashJSON(payload []byte) (uint64, error) { return kernel.PayloadHashJSON(payload) }

// EncodeEvent serializes an event envelope as json or msgpack.
func EncodeEvent(format string, message EventMessage) ([]byte, error) {
	return kernel.EncodeEvent(format, message)
}

// DecodeEvent reads an event envelope encoded as json or msgpack.
func DecodeEvent(format string, wire []byte) (*EventMessage, error) {
	return kernel.DecodeEvent(format, wire)
}

// EncodeCommand serializes a command envelope as json or msgpack.
func EncodeCommand(format string, message CommandMessage) ([]byte, error) {
	return kernel.EncodeCommand(format, message)
}

// DecodeCommand reads a command envelope encoded as json or msgpack.
func DecodeCommand(format string, wire []byte) (*CommandMessage, error) {
	return kernel.DecodeCommand(format, wire)
}

// EncryptionInfo is the typed public payload encryption record.
type EncryptionInfo = kernel.EncryptionInfo
