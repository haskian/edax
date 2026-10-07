package edax

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// JSONCodec maps stable event names to application types. Configure it before concurrent use.
type JSONCodec struct {
	types     map[string]func() any
	names     map[reflect.Type]string
	upcasters map[string]*Upcaster
	encode    func(any) ([]byte, error)
	decode    func([]byte, any) error
}

// NewJSONCodec takes constructors so decoding returns the application's concrete pointer types.
// Constructors must return fresh values and support concurrent calls after configuration.
func NewJSONCodec(registry map[string]func() any) (*JSONCodec, error) {
	codec := &JSONCodec{encode: json.Marshal, decode: json.Unmarshal, types: make(map[string]func() any), names: make(map[reflect.Type]string), upcasters: make(map[string]*Upcaster)}
	for name, makeValue := range registry {
		if name == "" || makeValue == nil {
			return nil, &Error{Code: CodeCodec, Message: "payload registration needs a name and constructor"}
		}
		value := makeValue()
		typ := reflect.TypeOf(value)
		if typ == nil || typ.Kind() != reflect.Pointer || reflect.ValueOf(value).IsNil() {
			return nil, &Error{Code: CodeCodec, Message: "payload constructor must return a non-nil pointer"}
		}
		if prior, ok := codec.names[typ]; ok {
			return nil, &Error{Code: CodeAlreadyRegistered, Message: fmt.Sprintf("payload type is registered as %s", prior)}
		}
		codec.types[name] = makeValue
		codec.names[typ] = name
		codec.names[typ.Elem()] = name
	}
	return codec, nil
}

// Marshal encodes a registered value at its current schema version. Nil payloads are refused.
func (c *JSONCodec) Marshal(value any) (Payload, error) {
	name, ok := c.names[reflect.TypeOf(value)]
	if value != nil {
		current := reflect.ValueOf(value)
		if current.Kind() == reflect.Pointer && current.IsNil() {
			return Payload{}, &Error{Code: CodeCodec, Message: "cannot encode a nil payload"}
		}
	}
	if !ok {
		return Payload{}, &Error{Code: CodeCodec, Message: fmt.Sprintf("unregistered payload type %T", value)}
	}
	data, err := c.encode(value)
	if err != nil {
		return Payload{}, &Error{Code: CodeCodec, Message: err.Error()}
	}
	version := uint32(1)
	if upcaster := c.upcasters[name]; upcaster != nil {
		version, err = upcaster.Target()
		if err != nil {
			return Payload{}, err
		}
	}
	return Payload{Name: name, Version: version, Data: data}, nil
}

// Unmarshal applies required upcasts and decodes into a fresh registered pointer value.
func (c *JSONCodec) Unmarshal(payload Payload) (any, error) {
	constructor, ok := c.types[payload.Name]
	if !ok {
		return nil, &Error{Code: CodeCodec, Message: "unregistered payload name " + payload.Name}
	}
	data := payload.Data
	if upcaster := c.upcasters[payload.Name]; upcaster != nil {
		var err error
		data, err = upcaster.Apply(payload.Version, data)
		if err != nil {
			return nil, err
		}
	} else if _, err := kernel.UpcastPlan(payload.Version, 1, nil); err != nil {
		return nil, err
	}
	value := constructor()
	if reflect.TypeOf(value) == nil || reflect.ValueOf(value).Kind() != reflect.Pointer || reflect.ValueOf(value).IsNil() {
		return nil, &Error{Code: CodeCodec, Message: "payload constructor returned a nil or non-pointer value"}
	}
	if c.names[reflect.TypeOf(value)] != payload.Name {
		return nil, &Error{Code: CodeCodec, Message: "payload constructor changed its registered type"}
	}
	if err := c.decode(data, value); err != nil {
		return nil, &Error{Code: CodeCodec, Message: err.Error()}
	}
	return value, nil
}

// WithUpcaster assigns schema transforms for one registered payload name.
func (c *JSONCodec) WithUpcaster(name string, upcaster *Upcaster) error {
	if _, ok := c.types[name]; !ok {
		return &Error{Code: CodeCodec, Message: "unregistered payload name " + name}
	}
	if upcaster != nil {
		if _, err := upcaster.Target(); err != nil {
			return err
		}
		for version, step := range upcaster.Steps {
			if step == nil {
				return &Error{Code: CodeBadHandle, Message: fmt.Sprintf("upcast transform %d is nil", version)}
			}
		}
	}
	c.upcasters[name] = upcaster
	return nil
}

// Upcaster applies host transforms in the order validated by the kernel.
// A transform keyed by n changes schema n to n+1.
type Upcaster struct {
	Steps map[uint32]func([]byte) ([]byte, error)
}

func (u *Upcaster) versions() []uint32 {
	versions := make([]uint32, 0, len(u.Steps))
	for version := range u.Steps {
		versions = append(versions, version)
	}
	return versions
}

// Target asks the kernel for the current schema version and validates the transform sequence.
func (u *Upcaster) Target() (uint32, error) { return kernel.UpcastTarget(u.versions()) }

// Apply runs the kernel-selected transforms from the supplied version to Target.
// Transform functions receive raw bytes and may return an error to stop the sequence.
func (u *Upcaster) Apply(from uint32, data []byte) ([]byte, error) {
	target, err := u.Target()
	if err != nil {
		return nil, err
	}
	plan, err := kernel.UpcastPlan(from, target, u.versions())
	if err != nil {
		return nil, err
	}
	for _, version := range plan {
		step := u.Steps[version]
		if step == nil {
			return nil, &Error{Code: CodeCodec, Message: fmt.Sprintf("upcast transform %d is nil", version)}
		}
		data, err = step(data)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}
