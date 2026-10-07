package edax

import (
	"bytes"
	"io"
	"reflect"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// MessagePackCodec maps the same named payload registry to MessagePack bytes.
// It uses json field tags when msgpack tags are absent. Upcast transforms receive
// MessagePack bytes and must return MessagePack bytes.
type MessagePackCodec struct{ *JSONCodec }

// NewMessagePackCodec validates a payload registry and uses named MessagePack fields.
// Configure upcasters before concurrent use, and return fresh values from registry constructors.
func NewMessagePackCodec(registry map[string]func() any) (*MessagePackCodec, error) {
	codec, err := NewJSONCodec(registry)
	if err != nil {
		return nil, err
	}
	codec.encode = func(value any) ([]byte, error) {
		var buffer bytes.Buffer
		encoder := msgpack.NewEncoder(&buffer)
		encoder.SetCustomStructTag("json")
		if err := encoder.Encode(value); err != nil {
			return nil, err
		}
		return buffer.Bytes(), nil
	}
	codec.decode = func(data []byte, value any) error {
		shape := msgpack.NewDecoder(bytes.NewReader(data))
		if err := checkMessagePackShape(shape, reflect.TypeOf(value).Elem()); err != nil {
			return err
		}
		decoder := msgpack.NewDecoder(bytes.NewReader(data))
		decoder.SetCustomStructTag("json")
		if err := decoder.Decode(value); err != nil {
			return err
		}
		if _, err := decoder.PeekCode(); err != io.EOF {
			if err != nil {
				return err
			}
			return &Error{Code: CodeCodec, Message: "messagepack payload contains trailing data"}
		}
		return nil
	}
	return &MessagePackCodec{codec}, nil
}

// EncodeMsgpack writes the canonical text representation used across SDKs.
func (d Decimal) EncodeMsgpack(encoder *msgpack.Encoder) error {
	return encoder.EncodeString(d.String())
}

// DecodeMsgpack validates a stored value through the kernel before replacing the receiver.
func (d *Decimal) DecodeMsgpack(decoder *msgpack.Decoder) error {
	text, err := decoder.DecodeString()
	if err != nil {
		return err
	}
	return d.UnmarshalText([]byte(text))
}

// EncodeMsgpack writes the canonical text representation used across SDKs.
func (i Instant) EncodeMsgpack(encoder *msgpack.Encoder) error {
	text, err := i.MarshalText()
	if err != nil {
		return err
	}
	return encoder.EncodeString(string(text))
}

// DecodeMsgpack validates a stored value through the kernel before replacing the receiver.
func (i *Instant) DecodeMsgpack(decoder *msgpack.Decoder) error {
	code, err := decoder.PeekCode()
	if err != nil {
		return err
	}
	if msgpcode.IsExt(code) {
		value, err := decoder.DecodeTime()
		if err != nil {
			return err
		}
		return i.UnmarshalText([]byte(value.UTC().Format(time.RFC3339Nano)))
	}
	text, err := decoder.DecodeString()
	if err != nil {
		return err
	}
	return i.UnmarshalText([]byte(text))
}
