package edax

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
)

// encodeJSON is the SDK's one encoder for host values bound for the kernel or the wire. It writes
// what encoding/json writes, except a float. encoding/json spells a whole float as integer digits,
// and the kernel reads those as an integer: it refuses 2^53 and compares 5.8e16 as an integer.
// Here a float keeps a fraction or an exponent, and NaN and the infinities become null, as the
// Python SDK writes them (host_values.json).
func encodeJSON(value any) ([]byte, error) {
	tree, err := floatTree(reflect.ValueOf(value), 0)
	if err != nil {
		return nil, err
	}
	return json.Marshal(tree)
}

// encoding/json starts its cycle check at this depth. The walk has no check of its own, so it stops.
const maxJSONDepth = 1000

var (
	jsonMarshalerType = reflect.TypeFor[json.Marshaler]()
	textMarshalerType = reflect.TypeFor[encoding.TextMarshaler]()
	isZeroerType      = reflect.TypeFor[isZeroer]()
)

type isZeroer interface{ IsZero() bool }

// floatTree mirrors v for encoding/json: each float becomes a json.Number in the Python SDK's
// spelling, and each struct a jsonObject. A value that marshals itself stays as it is.
func floatTree(v reflect.Value, depth int) (any, error) {
	if !v.IsValid() {
		return nil, nil
	}
	if depth > maxJSONDepth {
		return nil, fmt.Errorf("json: value nests deeper than %d levels or is cyclic", maxJSONDepth)
	}
	if self, ok := marshalsItself(v); ok {
		return self, nil
	}
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return jsonFloat(v.Float(), v.Type().Bits())
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return nil, nil
		}
		return floatTree(v.Elem(), depth+1)
	case reflect.Map:
		if v.IsNil() {
			return nil, nil
		}
		// The mirror keeps the key type, so encoding/json still writes and sorts the keys.
		tree := reflect.MakeMapWithSize(reflect.MapOf(v.Type().Key(), reflect.TypeFor[any]()), v.Len())
		for entry := v.MapRange(); entry.Next(); {
			value, err := floatTree(entry.Value(), depth+1)
			if err != nil {
				return nil, err
			}
			// reflect.ValueOf(nil) would delete the key, so set an interface that holds nil.
			tree.SetMapIndex(entry.Key(), reflect.ValueOf(&value).Elem())
		}
		return tree.Interface(), nil
	case reflect.Slice:
		// A byte slice is base64 text, and a nil slice is null.
		if v.IsNil() || v.Type().Elem().Kind() == reflect.Uint8 {
			return v.Interface(), nil
		}
		fallthrough
	case reflect.Array:
		tree := make([]any, v.Len())
		for i := range tree {
			value, err := floatTree(v.Index(i), depth+1)
			if err != nil {
				return nil, err
			}
			tree[i] = value
		}
		return tree, nil
	case reflect.Struct:
		return structTree(v, depth)
	}
	return v.Interface(), nil
}

// jsonFloat spells a float as encoding/json does, plus ".0" when it would read as an integer.
func jsonFloat(value float64, bits int) (any, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, nil
	}
	var number any = value
	if bits == 32 {
		number = float32(value)
	}
	wire, err := json.Marshal(number)
	if err != nil {
		return nil, err
	}
	if !bytes.ContainsAny(wire, ".e") {
		wire = append(wire, ".0"...)
	}
	return json.Number(wire), nil
}

// jsonObject holds a struct's members in field order, as encoding/json writes them.
type jsonObject []jsonMember

type jsonMember struct {
	name  string
	value any
}

func (o jsonObject) MarshalJSON() ([]byte, error) {
	wire := []byte{'{'}
	for i, member := range o {
		if i > 0 {
			wire = append(wire, ',')
		}
		name, _ := json.Marshal(member.name) // A string always encodes.
		value, err := json.Marshal(member.value)
		if err != nil {
			return nil, err
		}
		wire = append(append(append(wire, name...), ':'), value...)
	}
	return append(wire, '}'), nil
}

func structTree(v reflect.Value, depth int) (jsonObject, error) {
	object := jsonObject{}
	for _, field := range jsonFields(v.Type()) {
		value, ok := fieldByIndex(v, field.index)
		if !ok || field.omitEmpty && isEmptyJSON(value) || field.omitZero && isZeroJSON(value) {
			continue
		}
		var member any
		var err error
		if field.quoted {
			member, err = quotedJSON(value)
		} else {
			member, err = floatTree(value, depth+1)
		}
		if err != nil {
			return nil, err
		}
		object = append(object, jsonMember{field.name, member})
	}
	return object, nil
}

// marshalsItself returns what encoding/json hands to v's MarshalJSON or MarshalText method. As
// in encoding/json, a pointer method counts only when v is addressable.
func marshalsItself(v reflect.Value) (any, bool) {
	typ := v.Type()
	pointer := reflect.PointerTo(typ)
	if typ.Kind() != reflect.Pointer && v.CanAddr() && (pointer.Implements(jsonMarshalerType) || pointer.Implements(textMarshalerType)) {
		return v.Addr().Interface(), true
	}
	if typ.Implements(jsonMarshalerType) || typ.Implements(textMarshalerType) {
		return v.Interface(), true
	}
	return nil, false
}

// quotedJSON writes a ",string" field: the scalar's JSON text as a string, in Go's spelling. A
// value that marshals itself ignores the option, as in encoding/json.
func quotedJSON(v reflect.Value) (any, error) {
	if self, ok := marshalsItself(v); ok {
		return self, nil
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, nil
		}
		v = v.Elem()
	}
	wire, err := json.Marshal(v.Interface())
	return string(wire), err
}

// fieldByIndex follows a promoted field's index. It reports false under a nil embedded pointer,
// where encoding/json skips the field.
func fieldByIndex(v reflect.Value, index []int) (reflect.Value, bool) {
	for i, position := range index {
		if i > 0 && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(position)
	}
	return v, true
}

// isEmptyJSON is encoding/json's omitempty test.
func isEmptyJSON(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Interface, reflect.Pointer:
		return v.IsZero()
	}
	return false
}

// isZeroJSON is encoding/json's omitzero test: the declared type's IsZero method, else the Go
// zero value. A nil interface or pointer is zero without a call.
func isZeroJSON(v reflect.Value) bool {
	typ := v.Type()
	nilable := typ.Kind() == reflect.Interface || typ.Kind() == reflect.Pointer
	switch {
	case nilable && v.IsNil():
		return true
	case typ.Kind() == reflect.Interface && typ.Implements(isZeroerType):
		return v.Elem().Kind() == reflect.Pointer && v.Elem().IsNil() || v.Interface().(isZeroer).IsZero()
	case typ.Implements(isZeroerType):
		return v.Interface().(isZeroer).IsZero()
	case reflect.PointerTo(typ).Implements(isZeroerType):
		if !v.CanAddr() {
			boxed := reflect.New(typ).Elem()
			boxed.Set(v)
			v = boxed
		}
		return v.Addr().Interface().(isZeroer).IsZero()
	}
	return v.IsZero()
}
