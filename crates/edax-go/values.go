package edax

import (
	"encoding/json"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
	// Decimal is canonical decimal text. NewDecimal validates and normalizes through the kernel.
)

type Decimal struct{ text string }

// NewDecimal validates and canonicalizes decimal text in the kernel.
func NewDecimal(text string) (Decimal, error) {
	canonical, err := kernel.CanonicalDecimal(text)
	return Decimal{text: canonical}, err
}

// String returns canonical decimal text; the zero value is "0".
func (d Decimal) String() string {
	if d.text == "" {
		return "0"
	}
	return d.text
}

// MarshalJSON encodes the canonical value as a JSON string.
func (d Decimal) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

// UnmarshalJSON validates a JSON string before replacing the receiver.
func (d *Decimal) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return &Error{Code: CodeCodec, Message: err.Error()}
	}
	value, err := NewDecimal(text)
	if err == nil {
		*d = value
	}
	return err
}

// Instant is a kernel-normalized UTC timestamp with microsecond precision.
type Instant struct{ text string }

// NewInstant validates timestamp text and normalizes it to UTC with microsecond precision.
func NewInstant(text string) (Instant, error) {
	canonical, err := kernel.CanonicalTimestamp(text)
	return Instant{text: canonical}, err
}

// String returns canonical UTC text, or an empty string for an uninitialized Instant.
func (i Instant) String() string { return i.text }

// MarshalJSON writes a canonical JSON string and rejects an uninitialized Instant.
func (i Instant) MarshalJSON() ([]byte, error) {
	if i.text == "" {
		return nil, &Error{Code: CodeCodec, Message: "an Instant must be constructed with NewInstant"}
	}
	return json.Marshal(i.text)
}

// UnmarshalJSON validates a JSON string before replacing the receiver.
func (i *Instant) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return &Error{Code: CodeCodec, Message: err.Error()}
	}
	value, err := NewInstant(text)
	if err == nil {
		*i = value
	}
	return err
}

// MarshalText preserves the canonical form when a Decimal is a map key.
func (d Decimal) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

// UnmarshalText validates canonical text before replacing the receiver.
func (d *Decimal) UnmarshalText(text []byte) error {
	value, err := NewDecimal(string(text))
	if err == nil {
		*d = value
	}
	return err
}

// MarshalText preserves the canonical form when an Instant is a map key.
func (i Instant) MarshalText() ([]byte, error) {
	if i.text == "" {
		return nil, &Error{Code: CodeCodec, Message: "an Instant must be constructed with NewInstant"}
	}
	return []byte(i.text), nil
}

// UnmarshalText validates canonical text before replacing the receiver.
func (i *Instant) UnmarshalText(text []byte) error {
	value, err := NewInstant(string(text))
	if err == nil {
		*i = value
	}
	return err
}
